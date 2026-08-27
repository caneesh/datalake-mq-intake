package com.hcsc.datalake.mqintake.core.failure;

import java.util.Collection;

/**
 * Decides how large a batch a binding may currently use, and tracks the
 * messages under suspicion while a poison message is being isolated.
 *
 * <p>Narrower than {@link DegradedModeManager}'s full surface on purpose. The
 * receive loop needs exactly these operations; the counters and level
 * accessors exist for diagnostics and tests, and putting them here would make
 * every implementation carry state it may not have.
 *
 * <p>Implementations are shared by all listener threads of a binding — MQ
 * redistributes a rolled-back batch across them, so isolation only converges
 * if every thread sees the same batch size and the same suspect set. They must
 * therefore be thread-safe.
 */
public interface DegradationPolicy {

    /**
     * The batch size to use right now: the configured size normally, a reduced
     * one while isolating a poison message.
     */
    int getCurrentBatchSize();

    boolean isInDegradedMode();

    /**
     * A batch committed. May restore the normal batch size.
     *
     * @return true when THIS call restored it — the degraded→normal edge.
     *         Reported from inside the transition rather than left to the
     *         caller's before/after snapshots, so under contention exactly one
     *         caller observes the edge and the exit health/metrics updates
     *         fire once per transition, not once per racing thread.
     */
    boolean recordSuccess();

    /**
     * A batch failed. When the failure is data-classified, the batch's message
     * IDs are marked suspect as part of the same state change — see
     * {@link DegradedModeManager#recordFailure(Throwable, Collection)} for why
     * these cannot be two calls.
     *
     * @param batchMessageIds IDs of the failed batch, or null if unknown
     * @return the classification, plus whether this call was the
     *         normal→degraded edge (entering, not deepening) — exactly-once
     *         under contention, for the same reason as {@link #recordSuccess()}
     */
    FailureResult recordFailure(Throwable throwable, Collection<String> batchMessageIds);

    /** What {@link #recordFailure} decided: the classification and the edge. */
    final class FailureResult {

        private final FailureClass failureClass;
        private final boolean enteredDegradedMode;

        public FailureResult(FailureClass failureClass, boolean enteredDegradedMode) {
            this.failureClass = failureClass;
            this.enteredDegradedMode = enteredDegradedMode;
        }

        public FailureClass getFailureClass() {
            return failureClass;
        }

        /** True only for the call that entered degraded mode; deepening is false. */
        public boolean enteredDegradedMode() {
            return enteredDegradedMode;
        }
    }

    /**
     * These messages are resolved — committed, or routed to the backout queue.
     * Until every suspect is cleared, the normal batch size is not restored.
     */
    void clearSuspects(Collection<String> messageIds);

    /**
     * Unresolved suspects. Feeds the {@code mq_intake_suspect_count} gauge:
     * a policy that tracks none may keep the default zero.
     */
    default int getSuspectCount() {
        return 0;
    }
}
