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

    /** A batch committed. May restore the normal batch size. */
    void recordSuccess();

    /**
     * A batch failed. When the failure is data-classified, the batch's message
     * IDs are marked suspect as part of the same state change — see
     * {@link DegradedModeManager#recordFailure(Throwable, Collection)} for why
     * these cannot be two calls.
     *
     * @param batchMessageIds IDs of the failed batch, or null if unknown
     * @return how the failure was classified
     */
    FailureClass recordFailure(Throwable throwable, Collection<String> batchMessageIds);

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
