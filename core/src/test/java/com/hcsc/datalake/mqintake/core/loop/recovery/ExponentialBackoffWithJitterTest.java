package com.hcsc.datalake.mqintake.core.loop.recovery;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backoff arithmetic, previously inline in the receive loop and reachable only
 * by running one.
 *
 * <p>Jitter is supplied rather than drawn from a random source, so the values
 * are exact instead of approximate.
 */
class ExponentialBackoffWithJitterTest {

    /** Zero jitter draw (0.5 maps to the midpoint, i.e. no offset). */
    private static final double NO_JITTER = 0.5;

    private BackoffPolicy withFixedDraw(double draw) {
        return new ExponentialBackoffWithJitter(
                Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, 0.2, () -> draw);
    }

    @Test
    void backoffDoublesWithEachAttempt() {
        BackoffPolicy policy = withFixedDraw(NO_JITTER);

        assertThat(policy.backoffFor(1)).isEqualTo(Duration.ofMillis(1000));
        assertThat(policy.backoffFor(2)).isEqualTo(Duration.ofMillis(2000));
        assertThat(policy.backoffFor(3)).isEqualTo(Duration.ofMillis(4000));
        assertThat(policy.backoffFor(4)).isEqualTo(Duration.ofMillis(8000));
    }

    @Test
    void backoffIsCappedAtTheMaximum() {
        BackoffPolicy policy = withFixedDraw(NO_JITTER);

        // Without a cap, attempt 20 would be ~500,000 seconds
        assertThat(policy.backoffFor(20)).isEqualTo(Duration.ofSeconds(60));
        assertThat(policy.backoffFor(50)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void jitterMovesTheIntervalInBothDirections() {
        // draw 1.0 -> +20%, draw 0.0 -> -20%
        assertThat(withFixedDraw(1.0).backoffFor(3)).isEqualTo(Duration.ofMillis(4800));
        assertThat(withFixedDraw(0.0).backoffFor(3)).isEqualTo(Duration.ofMillis(3200));
    }

    @Test
    void neverWaitsLessThanTheInitialInterval() {
        // A negative draw on attempt 1 would otherwise give ~800ms, and on a
        // very small initial interval could approach zero — hammering a queue
        // manager that is still coming up.
        assertThat(withFixedDraw(0.0).backoffFor(1)).isEqualTo(Duration.ofMillis(1000));
    }

    @Test
    void attemptZeroOrNegativeIsTreatedAsTheFirstAttempt() {
        BackoffPolicy policy = withFixedDraw(NO_JITTER);

        assertThat(policy.backoffFor(0)).isEqualTo(Duration.ofMillis(1000));
        assertThat(policy.backoffFor(-5)).isEqualTo(Duration.ofMillis(1000));
    }

    @Test
    void realJitterProducesSpreadSoThreadsDoNotRetryInLockstep() {
        // Every listener thread of a binding loses its session at the same
        // instant when a queue manager goes down. Without spread they would
        // return as a synchronised burst.
        BackoffPolicy policy = new ExponentialBackoffWithJitter();
        Set<Duration> observed = new HashSet<>();

        for (int i = 0; i < 50; i++) {
            observed.add(policy.backoffFor(5));
        }

        assertThat(observed).hasSizeGreaterThan(1);
        assertThat(observed).allSatisfy(d -> {
            assertThat(d.toMillis()).isBetween(12_800L, 19_200L);   // 16s +/- 20%
        });
    }

    @Test
    void fixedPolicyIgnoresTheAttemptNumber() {
        BackoffPolicy policy = BackoffPolicy.fixed(Duration.ofMillis(250));

        assertThat(policy.backoffFor(1)).isEqualTo(Duration.ofMillis(250));
        assertThat(policy.backoffFor(9)).isEqualTo(Duration.ofMillis(250));
    }
}
