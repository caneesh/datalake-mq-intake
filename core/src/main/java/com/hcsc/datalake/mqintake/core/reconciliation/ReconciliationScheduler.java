package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Runs reconciliation periodically — the check half of ABC.
 *
 * <p>The audit records state what should be on HDFS. Reconciliation confirms
 * it actually is, by comparing the audited record count for a partition
 * against what the files contain. Without something to run it, the audit is
 * evidence nobody reads: {@code PartitionReconciliationService} existed and
 * was tested, but had no production caller at all.
 *
 * <p>Design constraints this class exists to satisfy:
 * <ul>
 *   <li><strong>Never interferes with ingestion.</strong> Reconciliation reads
 *       HDFS; it holds no JMS session and touches no receive loop. Every
 *       failure is caught per binding, so a reconciliation problem cannot stop
 *       messages being consumed. That ordering is deliberate — a checking
 *       mechanism that can halt the thing it checks is worse than no check.</li>
 *   <li><strong>Per binding, independently.</strong> One binding's slow or
 *       failing reconciliation must not delay another's.</li>
 *   <li><strong>No overlapping runs for the same binding.</strong> A run that
 *       takes longer than the interval would otherwise start again beneath
 *       itself, doubling HDFS load exactly when it is already slow.</li>
 * </ul>
 *
 * <p>WHEN passes happen is all this class decides. What a pass does — which
 * windows to examine, what to carry forward, how to report what it found —
 * belongs to {@link BindingReconciliationRunner}, one per binding.
 */
public class ReconciliationScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationScheduler.class);

    private final PartitionReconciler reconciliationService;
    private final IntakeProperties properties;
    private final Function<String, BindingMetrics> metricsLookup;
    private final Clock clock;

    /**
     * One runner per binding, holding everything that is per-binding: the
     * in-progress flag, the windows, the pending backlog and the last
     * completed pass. Created on demand so a pass driven directly needs no
     * schedule.
     */
    private final Map<String, BindingReconciliationRunner> runners = new ConcurrentHashMap<>();

    private volatile ScheduledExecutorService scheduler;

    /**
     * Where a binding's work actually runs.
     *
     * <p>The class has always documented that one binding's slow or failing
     * reconciliation must not delay another's, and did not honour it: a single
     * scheduled thread called every binding in turn, so the first to block
     * held up the rest of the pass and the next pass behind it. Sized to the
     * binding count so each has a thread of its own, and capped so the pool is
     * bounded whatever the configuration.
     */
    private volatile ExecutorService bindingWorkers;

    /** Partitions carried over from earlier passes; see PendingPartitions. */
    private final PendingPartitions pendingPartitions;

    static final int MAX_BINDING_WORKERS = 8;

    /**
     * Passes that must produce nothing before a binding is reported stalled.
     *
     * <p>One interval late is ordinary — a pass slower than the interval,
     * which the in-progress flag already reports when the next task runs.
     * Three means two whole passes came back with nothing, which no benign
     * cause explains.
     */
    static final int STALE_PASSES_BEFORE_REPORTING = 3;

    /**
     * Stall reporting, which is a scheduling concern: the runner knows it is
     * late, this decides that being late is worth saying out loud, and only
     * says it once.
     *
     * <p>It exists because a stalled reconciliation was completely silent.
     * The "still running — skipping this run" warning fires inside
     * {@link #runBindingQuietly}, which means it only fires for a task that
     * got a worker thread. Once every worker is blocked on HDFS — the
     * correlated case, since all bindings read the same cluster — later passes
     * queue behind them and log nothing at all, and the only reconciliation
     * metric was a discrepancy counter that stays flat because no comparison
     * is being made. Reconciliation could stop for the life of the process
     * with the sole symptom being the absence of a periodic INFO line.
     */
    /** Whether a binding's stall has been reported, so it is reported once. */
    private final Map<String, AtomicBoolean> reportedStalled = new ConcurrentHashMap<>();

    public ReconciliationScheduler(PartitionReconciler reconciliationService,
                                   IntakeProperties properties,
                                   Function<String, BindingMetrics> metricsLookup,
                                   Clock clock) {
        this(reconciliationService, properties, metricsLookup, clock, null);
    }

    public ReconciliationScheduler(PartitionReconciler reconciliationService,
                                   IntakeProperties properties,
                                   Function<String, BindingMetrics> metricsLookup,
                                   Clock clock,
                                   PendingPartitions pendingPartitions) {
        this.reconciliationService =
                Objects.requireNonNull(reconciliationService, "reconciliationService required");
        this.properties = Objects.requireNonNull(properties, "properties required");
        this.metricsLookup = Objects.requireNonNull(metricsLookup, "metricsLookup required");
        this.clock = Objects.requireNonNull(clock, "clock required");
        this.pendingPartitions = pendingPartitions;
    }

    /** Starts the schedule, unless reconciliation is disabled. */
    public void start() {
        IntakeProperties.ReconciliationProperties config = properties.getReconciliation();

        if (!config.isEnabled()) {
            log.info("Reconciliation is disabled — landed data will not be checked against the "
                    + "audit trail. Set intake.reconciliation.enabled=true to enable it.");
            return;
        }
        if (config.getIntervalMs() <= 0) {
            log.warn("Reconciliation enabled but interval is {}ms — not scheduling",
                    config.getIntervalMs());
            return;
        }

        int workerCount = Math.min(
                Math.max(1, properties.getBindings().size()), MAX_BINDING_WORKERS);
        java.util.concurrent.atomic.AtomicInteger workerNumber =
                new java.util.concurrent.atomic.AtomicInteger();
        bindingWorkers = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r, "reconcile-worker-" + workerNumber.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "reconcile");
            // Daemon: reconciliation must never hold up JVM shutdown. A run
            // interrupted mid-way costs nothing — the next run re-examines the
            // same windows.
            t.setDaemon(true);
            return t;
        });

        // First run one interval out, not immediately: at startup the most
        // recent windows are still inside their grace period anyway, and
        // competing with startup for HDFS helps nobody.
        scheduler.scheduleWithFixedDelay(this::runAllBindingsQuietly,
                config.getIntervalMs(), config.getIntervalMs(), TimeUnit.MILLISECONDS);

        // Seeded now rather than at zero: the first run is one interval out,
        // and a binding must not read as stalled before it has had a chance to
        // run once.
        long startedAt = clock.millis();
        for (BindingConfig binding : properties.getBindings()) {
            BindingReconciliationRunner runner = runnerFor(binding);
            runner.seed(startedAt);
            reportedStalled.put(binding.getId(), new AtomicBoolean(false));
            BindingMetrics metrics = metricsLookup.apply(binding.getId());
            if (metrics != null) {
                metrics.setReconciliationAgeSupplier(runner::ageOfLastCompletedPassMs);
            }
        }

        log.info("Reconciliation scheduled every {}ms, grace {}ms, {} windows per run",
                config.getIntervalMs(), config.getGracePeriodMs(), config.getLookbackWindows());
    }

    @Override
    public void close() {
        ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdownNow();
        }
        ExecutorService workers = bindingWorkers;
        if (workers != null) {
            workers.shutdownNow();
        }
        if (s != null || workers != null) {
            log.info("Reconciliation scheduler stopped");
        }
    }

    /**
     * One pass over every binding. Nothing escapes into the scheduler: a task
     * that throws out of scheduleWithFixedDelay is cancelled silently, which
     * would stop reconciliation for the life of the process without saying so.
     */
    void runAllBindingsQuietly() {
        try {
            ExecutorService workers = bindingWorkers;
            for (BindingConfig binding : properties.getBindings()) {
                // Before submitting, not after: this thread always runs
                // promptly — submission never blocks — so it is the one place
                // guaranteed to notice that nothing is coming back.
                reportStallTransition(binding.getId());
                if (workers == null) {
                    // No pool: start() was never called, which is the shape
                    // tests use when driving a single pass directly.
                    runBindingQuietly(binding);
                    continue;
                }
                try {
                    // Submitted, not called. The scheduler thread must return
                    // promptly whatever any one binding is doing — otherwise a
                    // binding blocked on an HDFS read holds up every binding
                    // behind it and the next pass too. Overlap is still
                    // prevented per binding by the in-progress flag, so a slow
                    // binding cannot pile up beneath itself either.
                    workers.execute(() -> runBindingQuietly(binding));
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    log.debug("Reconciliation worker pool is shutting down — skipping "
                            + "binding '{}' this pass", binding.getId());
                }
            }
        } catch (Throwable t) {
            log.error("Reconciliation pass failed: {}", t.getMessage(), t);
        }
    }

    /** Runs one binding's pass now, on the calling thread. */
    void runBindingQuietly(BindingConfig binding) {
        runnerFor(binding).run();
    }

    private BindingReconciliationRunner runnerFor(BindingConfig binding) {
        return runners.computeIfAbsent(binding.getId(), id ->
                new BindingReconciliationRunner(binding, reconciliationService, properties,
                        metricsLookup, pendingPartitions, clock));
    }

    /**
     * Milliseconds since this binding's last completed pass, or -1 when
     * nothing is scheduling it.
     *
     * <p>Computed from the timestamp rather than reported by the scheduler
     * thread, so it stays true even if that thread is the thing that died —
     * which is the other way this can go quiet.
     */
    public long ageOfLastCompletedPassMs(String bindingId) {
        BindingReconciliationRunner runner = runners.get(bindingId);
        return runner == null ? -1 : runner.ageOfLastCompletedPassMs();
    }

    /**
     * True when this binding has produced no completed pass for
     * {@link #STALE_PASSES_BEFORE_REPORTING} intervals.
     *
     * <p>False whenever the schedule is not running: reconciliation being
     * switched off is not the same condition as reconciliation being stuck,
     * and reporting them alike would make the signal useless.
     */
    public boolean isStalled(String bindingId) {
        if (!isScheduled()) {
            return false;
        }
        BindingReconciliationRunner runner = runners.get(bindingId);
        return runner != null && runner.isBehind(staleAfterMs());
    }

    private long staleAfterMs() {
        return (long) STALE_PASSES_BEFORE_REPORTING
                * properties.getReconciliation().getIntervalMs();
    }

    /**
     * Whether this binding's stall has been reported and not yet cleared.
     *
     * <p>The report itself is a log line, which nothing can assert on without
     * a log-capturing appender this project does not otherwise use. This makes
     * the transition observable instead: without it, deleting the check from
     * the pass changed no test result.
     */
    boolean hasReportedStall(String bindingId) {
        AtomicBoolean reported = reportedStalled.get(bindingId);
        return reported != null && reported.get();
    }

    /** Reports a binding falling behind, and later catching up, once each. */
    private void reportStallTransition(String bindingId) {
        AtomicBoolean reported =
                reportedStalled.computeIfAbsent(bindingId, id -> new AtomicBoolean(false));
        if (isStalled(bindingId)) {
            if (reported.compareAndSet(false, true)) {
                log.error("Binding '{}': no reconciliation pass has completed in {}ms — landed "
                                + "data is no longer being checked against the audit trail. "
                                + "Ingestion is unaffected. The usual cause is every worker "
                                + "blocked on HDFS, which queues later passes behind them "
                                + "silently.",
                        bindingId, ageOfLastCompletedPassMs(bindingId));
            }
        } else if (reported.compareAndSet(true, false)) {
            log.info("Binding '{}': reconciliation is completing passes again", bindingId);
        }
    }

    /** True when the schedule is active. */
    public boolean isScheduled() {
        ScheduledExecutorService s = scheduler;
        return s != null && !s.isShutdown();
    }
}
