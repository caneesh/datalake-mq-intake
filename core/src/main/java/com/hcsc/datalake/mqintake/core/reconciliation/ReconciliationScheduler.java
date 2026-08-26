package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p>Each run examines the last {@code lookbackWindows} closed partitions
 * rather than only the most recent one, so a skipped run — overlap, restart,
 * a transient HDFS problem — does not leave a window permanently unchecked.
 */
public class ReconciliationScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationScheduler.class);

    private final PartitionReconciler reconciliationService;
    private final IntakeProperties properties;
    private final Function<String, BindingMetrics> metricsLookup;
    private final Clock clock;

    /** One flag per binding: held while that binding's run is in progress. */
    private final Map<String, AtomicBoolean> running = new ConcurrentHashMap<>();

    private volatile ScheduledExecutorService scheduler;

    public ReconciliationScheduler(PartitionReconciler reconciliationService,
                                   IntakeProperties properties,
                                   Function<String, BindingMetrics> metricsLookup,
                                   Clock clock) {
        this.reconciliationService =
                Objects.requireNonNull(reconciliationService, "reconciliationService required");
        this.properties = Objects.requireNonNull(properties, "properties required");
        this.metricsLookup = Objects.requireNonNull(metricsLookup, "metricsLookup required");
        this.clock = Objects.requireNonNull(clock, "clock required");
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

        log.info("Reconciliation scheduled every {}ms, grace {}ms, {} windows per run",
                config.getIntervalMs(), config.getGracePeriodMs(), config.getLookbackWindows());
    }

    @Override
    public void close() {
        ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdownNow();
            log.info("Reconciliation scheduler stopped");
        }
    }

    /**
     * One pass over every binding. Nothing escapes into the scheduler: a task
     * that throws out of scheduleWithFixedDelay is cancelled silently, which
     * would stop reconciliation for the life of the process without saying so.
     */
    private void runAllBindingsQuietly() {
        try {
            for (BindingConfig binding : properties.getBindings()) {
                runBindingQuietly(binding);
            }
        } catch (Throwable t) {
            log.error("Reconciliation pass failed: {}", t.getMessage(), t);
        }
    }

    /**
     * Reconciles one binding's recent windows.
     *
     * <p>Failures are contained here so one binding cannot affect another, and
     * so nothing propagates towards ingestion.
     */
    void runBindingQuietly(BindingConfig binding) {
        AtomicBoolean inProgress =
                running.computeIfAbsent(binding.getId(), id -> new AtomicBoolean(false));

        if (!inProgress.compareAndSet(false, true)) {
            log.warn("Reconciliation for binding '{}' is still running — skipping this run. "
                    + "If this repeats, the interval is shorter than a pass takes.",
                    binding.getId());
            return;
        }

        try {
            reconcileRecentWindows(binding);
        } catch (Throwable t) {
            // Deliberately swallowed. Reconciliation is a check on ingestion,
            // not a participant in it: a failure here must never stop messages
            // being consumed and landed.
            log.error("Reconciliation failed for binding '{}' — ingestion is unaffected: {}",
                    binding.getId(), t.getMessage(), t);
            BindingMetrics metrics = metricsLookup.apply(binding.getId());
            if (metrics != null) {
                metrics.recordReconciliationDiscrepancy();
            }
        } finally {
            inProgress.set(false);
        }
    }

    private void reconcileRecentWindows(BindingConfig binding) {
        IntakeProperties.ReconciliationProperties config = properties.getReconciliation();
        BindingMetrics metrics = metricsLookup.apply(binding.getId());

        for (Instant window : recentWindows(config.getLookbackWindows())) {
            PartitionReconciliationService.ReconciliationReport report =
                    reconciliationService.reconcilePartition(
                            binding.getId(),
                            binding.getHdfsBasePath(),
                            window,
                            // Identity is only trustworthy where the sidecar
                            // index is written; without it reconciliation
                            // correctly refuses rather than guessing.
                            binding.isRecordIndexEnabled(),
                            config.isQuarantineDuplicates(),
                            metrics);

            report(binding.getId(), report);
        }
    }

    /**
     * The closed partition windows to examine, most recent first.
     *
     * <p>Starts one window back: the current window is still being written to.
     */
    List<Instant> recentWindows(int count) {
        List<Instant> windows = new ArrayList<>();
        Instant now = clock.instant();
        for (int i = 1; i <= Math.max(1, count); i++) {
            windows.add(now.minus(Duration.ofMinutes(15L * i)));
        }
        return windows;
    }

    private void report(String bindingId,
                        PartitionReconciliationService.ReconciliationReport report) {
        switch (report.getStatus()) {
            case SKIPPED_GRACE_PERIOD:
                log.debug("Binding '{}': {} still inside grace period", bindingId,
                        report.getPartitionPath());
                return;
            case NOT_READY:
                log.debug("Binding '{}': reconciliation not ready for {} — {}", bindingId,
                        report.getPartitionPath(), report.getMessage());
                return;
            case ERROR:
                log.error("Binding '{}': reconciliation error for {} — {}", bindingId,
                        report.getPartitionPath(), report.getMessage());
                return;
            default:
                break;
        }

        if (report.getDiscrepancies().isEmpty()
                && report.getActualRecordSum() == report.getAuditedRecordSum()) {
            log.info("Binding '{}': {} balances — {} files, {} records, audit agrees",
                    bindingId, report.getPartitionPath(), report.getFileCount(),
                    report.getActualRecordSum());
            return;
        }

        // The ABC control failing is the whole point of running it: say so
        // loudly, with both sides of the balance, and leave the data alone.
        log.error("Binding '{}': RECONCILIATION DISCREPANCY in {} — landed {} records across {} "
                        + "files, audit accounts for {}. Discrepancies: {}",
                bindingId, report.getPartitionPath(), report.getActualRecordSum(),
                report.getFileCount(), report.getAuditedRecordSum(), report.getDiscrepancies());
    }

    /** True when the schedule is active. */
    public boolean isScheduled() {
        ScheduledExecutorService s = scheduler;
        return s != null && !s.isShutdown();
    }
}
