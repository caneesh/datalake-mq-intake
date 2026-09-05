package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * One binding's reconciliation pass, from choosing windows to reporting what
 * was found.
 *
 * <p>Extracted from {@code ReconciliationScheduler} unchanged. The division is
 * between WHAT a pass does and WHEN passes happen: this class knows nothing
 * about schedules, thread pools or shutdown, and the scheduler knows nothing
 * about windows, the pending backlog or how a discrepancy is worded.
 *
 * <p>One instance per binding, which is what turns the scheduler's three
 * maps-keyed-by-binding-id into plain fields here — including the in-progress
 * flag, whose whole purpose is that a binding never runs beneath itself.
 *
 * <p>Each pass examines the last {@code lookbackWindows} closed partitions
 * rather than only the most recent one, so a skipped pass — overlap, restart,
 * a transient HDFS problem — does not leave a window permanently unchecked.
 *
 * <p>Runs on a worker thread, and its state is read from the scheduler thread
 * (staleness) and from a metrics scrape (the age gauge), so both fields are
 * atomic. Two passes for the same binding never run at once; the flag is what
 * guarantees it.
 */
class BindingReconciliationRunner {

    private static final Logger log = LoggerFactory.getLogger(BindingReconciliationRunner.class);

    /**
     * The timestamp before a schedule has seeded one.
     *
     * <p>A pass that completes leaves the timestamp alone while unseeded, so a
     * runner driven directly — with nothing scheduling it — never starts
     * reporting an age. That preserves exactly what the scheduler's nullable
     * map entry did before the extraction.
     */
    private static final long UNSEEDED = -1L;

    private final BindingConfig binding;
    private final PartitionReconciler reconciliationService;
    private final IntakeProperties properties;
    private final Function<String, BindingMetrics> metricsLookup;
    private final PendingPartitions pendingPartitions;   // may be absent
    private final Clock clock;

    /** Held for the duration of this binding's pass; see run(). */
    private final AtomicBoolean inProgress = new AtomicBoolean(false);

    /** When the last pass finished, in epoch millis, or UNSEEDED. */
    private final AtomicLong lastPassCompletedMs = new AtomicLong(UNSEEDED);

    BindingReconciliationRunner(BindingConfig binding,
                                PartitionReconciler reconciliationService,
                                IntakeProperties properties,
                                Function<String, BindingMetrics> metricsLookup,
                                PendingPartitions pendingPartitions,
                                Clock clock) {
        this.binding = binding;
        this.reconciliationService = reconciliationService;
        this.properties = properties;
        this.metricsLookup = metricsLookup;
        this.pendingPartitions = pendingPartitions;
        this.clock = clock;
    }

    String bindingId() {
        return binding.getId();
    }

    /**
     * Starts the clock on this runner, so that from here on a pass that never
     * completes is visible as a growing age.
     */
    void seed(long nowMs) {
        lastPassCompletedMs.set(nowMs);
    }

    /** Milliseconds since the last completed pass, or -1 while unseeded. */
    long ageOfLastCompletedPassMs() {
        long last = lastPassCompletedMs.get();
        if (last == UNSEEDED) {
            return -1;
        }
        return Math.max(0, clock.millis() - last);
    }

    /** True when no pass has completed within the given budget. */
    boolean isBehind(long staleAfterMs) {
        long age = ageOfLastCompletedPassMs();
        return age >= 0 && age > staleAfterMs;
    }

    /**
     * Reconciles one binding's recent windows.
     *
     * <p>Failures are contained here so one binding cannot affect another, and
     * so nothing propagates towards ingestion.
     */
    void run() {
        if (!inProgress.compareAndSet(false, true)) {
            log.warn("Reconciliation for binding '{}' is still running — skipping this run. "
                    + "If this repeats, the interval is shorter than a pass takes.",
                    binding.getId());
            return;
        }

        try {
            reconcileRecentWindows();
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
            // A contained failure counts as a completed pass: it proves the
            // machinery is alive, and it already logs and increments a
            // counter. What this timestamp detects is nothing happening at
            // all.
            if (lastPassCompletedMs.get() != UNSEEDED) {
                lastPassCompletedMs.set(clock.millis());
            }
            inProgress.set(false);
        }
    }

    private void reconcileRecentWindows() {
        IntakeProperties.ReconciliationProperties config = properties.getReconciliation();
        BindingMetrics metrics = metricsLookup.apply(binding.getId());

        // The recent windows PLUS anything an earlier pass could not resolve.
        // A LinkedHashSet so a partition carried over that is also still
        // recent is reconciled once, not twice.
        java.util.Set<Instant> windows =
                new java.util.LinkedHashSet<>(recentWindows(clock, config.getLookbackWindows()));
        if (pendingPartitions != null) {
            windows.addAll(pendingPartitions.pending(binding.getId()));
        }

        for (Instant window : windows) {
            PartitionReconciliationService.ReconciliationReport report =
                    reconciliationService.reconcilePartition(
                            binding.getId(),
                            binding.getHdfs().getBasePath(),
                            window,
                            // Identity is only trustworthy where the sidecar
                            // index is written; without it reconciliation
                            // correctly refuses rather than guessing.
                            binding.getHdfs().isRecordIndexEnabled(),
                            config.isQuarantineDuplicates(),
                            metrics);

            trackPending(window, report);
            report(report);
        }
    }

    /**
     * Carries a partition forward when it could not be resolved, and drops it
     * once it was.
     *
     * <p>NOT_READY is excluded deliberately. It means the binding has no
     * approved identity — a standing property of the binding, not something a
     * later pass on this partition could change — so every window would enter
     * the backlog and none would ever leave. SKIPPED_GRACE_PERIOD and ERROR
     * are both genuinely worth another look.
     */
    private void trackPending(Instant window,
                              PartitionReconciliationService.ReconciliationReport report) {
        if (pendingPartitions == null) {
            return;
        }
        boolean worthAnotherPass = report.isRetryLater()
                && report.getStatus()
                    != PartitionReconciliationService.ReconciliationStatus.NOT_READY;
        if (worthAnotherPass) {
            pendingPartitions.retain(binding.getId(), window);
        } else {
            pendingPartitions.resolved(binding.getId(), window);
        }
    }

    /**
     * The closed partition windows to examine, most recent first.
     *
     * <p>Starts one window back: the current window is still being written to.
     */
    static List<Instant> recentWindows(Clock clock, int count) {
        List<Instant> windows = new ArrayList<>();
        Instant now = clock.instant();
        for (int i = 1; i <= Math.max(1, count); i++) {
            windows.add(now.minus(
                    com.hcsc.datalake.mqintake.core.hdfs.PartitionPath.WINDOW.multipliedBy(i)));
        }
        return windows;
    }

    private void report(PartitionReconciliationService.ReconciliationReport report) {
        String bindingId = binding.getId();
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
}
