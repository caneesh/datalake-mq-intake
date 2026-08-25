package com.hcsc.datalake.mqintake.core.loop.recovery;

import java.time.Duration;

/**
 * How long to wait before the next reconnect attempt.
 *
 * <p>Separated from the loop so the waiting strategy can be reasoned about and
 * tested without a queue manager, and so the loop reads as "wait the policy's
 * interval" rather than carrying exponent and jitter arithmetic inline.
 */
@FunctionalInterface
public interface BackoffPolicy {

    /**
     * @param attempt 1 for the first retry, increasing thereafter
     * @return how long to wait before that attempt
     */
    Duration backoffFor(int attempt);

    static BackoffPolicy exponentialWithJitter() {
        return new ExponentialBackoffWithJitter();
    }

    /** Fixed interval, mainly useful in tests. */
    static BackoffPolicy fixed(Duration interval) {
        return attempt -> interval;
    }
}
