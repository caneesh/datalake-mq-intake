package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Turns what a listener observes into health and metric transitions.
 *
 * <p>Extracted from the receive loop unchanged. The split is between deciding
 * and reporting: the degradation policy still decides whether a failure means
 * degraded mode, and this only records what was decided. Moving the policy
 * calls in here as well would have made "reporter" a misnomer for a class that
 * also drives behaviour.
 *
 * <p>Confined to one listener thread. The stall counters are plain fields for
 * that reason, exactly as they were in the loop.
 */
class LoopStateReporter {

    private static final Logger log = LoggerFactory.getLogger(LoopStateReporter.class);

    /**
     * Consecutive rolled-back batches before this listener is reported stalled.
     *
     * <p>One rollback is ordinary: the messages go back on the queue and the
     * next attempt usually succeeds. A run of them is not, and the failures
     * that produce a run — an unreachable tracker queue, an unwritable landing
     * path, an audit store refusing records — all classify as infrastructure,
     * which by design never enters degraded mode. That was the only route from
     * the loop to the health manager for a batch failure, so a binding could
     * roll back indefinitely with the endpoint reporting UP.
     *
     * <p>Five rather than one to avoid flapping on a transient blip, and
     * rather than fifty because the point is to be visible before anyone has
     * to notice by hand.
     */
    static final int CONSECUTIVE_BATCH_FAILURES_BEFORE_STALLED = 5;

    private final String bindingId;
    private final BindingHealthManager healthManager;   // may be absent
    private final BindingMetrics metrics;

    /** Loop-thread-confined; see CONSECUTIVE_BATCH_FAILURES_BEFORE_STALLED. */
    private int consecutiveBatchFailures = 0;

    /** Whether this listener's stall has already been reported, so it is reported once. */
    private boolean reportedStalled = false;

    /** Distinguishes this listener from its siblings in health reporting. */
    private volatile String listenerId = "unstarted";

    LoopStateReporter(String bindingId, BindingHealthManager healthManager,
                      BindingMetrics metrics) {
        this.bindingId = bindingId;
        this.healthManager = healthManager;
        this.metrics = metrics;
    }

    /** Names the thread this listener runs on, for per-listener health. */
    void listenerStarted(String listenerId) {
        this.listenerId = listenerId;
    }

    void unhealthy() {
        metrics.setHealthy(false);
    }

    void healthy() {
        metrics.setHealthy(true);
    }

    void suspects(long count) {
        metrics.setSuspectCount(count);
    }

    /**
     * Records the binding entering degraded mode.
     *
     * <p>The metric is written only when a health manager is present, which is
     * how the loop behaved and is preserved deliberately rather than tidied:
     * changing it would alter what a deployment without a health manager
     * publishes, which is a behavioural change disguised as a cleanup.
     */
    void enteredDegradedMode(FailureClass failureClass) {
        if (healthManager != null) {
            healthManager.recordDegraded(bindingId,
                    "Entered degraded mode due to " + failureClass + " failure");
            metrics.recordDegradedModeEntry();
        }
    }

    /** Records the binding leaving degraded mode; same health-manager guard. */
    void exitedDegradedMode() {
        if (healthManager != null) {
            healthManager.recordHealthy(bindingId);
            metrics.recordDegradedModeExit();
        }
    }

    /**
     * A batch failed. Health is left alone until a RUN of failures shows this
     * listener is not getting through at all.
     */
    void batchFailed(Throwable e) {
        consecutiveBatchFailures++;
        if (reportedStalled
                || consecutiveBatchFailures < CONSECUTIVE_BATCH_FAILURES_BEFORE_STALLED) {
            return;
        }
        reportedStalled = true;
        log.error("Binding '{}': {} consecutive batches rolled back on listener {} — reporting "
                        + "this listener stalled. Nothing is lost, every message is back on the "
                        + "queue, but this listener is committing nothing: {}",
                bindingId, consecutiveBatchFailures, listenerId, e.getMessage());
        if (healthManager != null) {
            healthManager.recordListenerStalled(bindingId, listenerId,
                    consecutiveBatchFailures + " consecutive batches rolled back; last failure: "
                            + e.getMessage());
        }
    }

    /** A batch committed: this listener is getting through again. */
    void batchProgressed() {
        consecutiveBatchFailures = 0;
        if (!reportedStalled) {
            return;
        }
        reportedStalled = false;
        log.info("Binding '{}': listener {} is committing again", bindingId, listenerId);
        if (healthManager != null) {
            healthManager.recordListenerProgressing(bindingId, listenerId);
        }
    }
}
