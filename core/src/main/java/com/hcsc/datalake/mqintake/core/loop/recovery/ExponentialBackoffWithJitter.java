package com.hcsc.datalake.mqintake.core.loop.recovery;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff, capped, with proportional jitter.
 *
 * <p>Jitter matters here rather than being a flourish: every listener thread of
 * a binding loses its session at the same instant when a queue manager goes
 * down, so without it they would all retry in lockstep and hit the recovering
 * queue manager as a synchronised burst.
 */
public class ExponentialBackoffWithJitter implements BackoffPolicy {

    private static final Duration DEFAULT_INITIAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX = Duration.ofSeconds(60);
    private static final double DEFAULT_MULTIPLIER = 2.0;
    private static final double DEFAULT_JITTER_FACTOR = 0.2;

    private final long initialMs;
    private final long maxMs;
    private final double multiplier;
    private final double jitterFactor;

    /** Supplies a value in [0,1). Injectable so jitter is testable. */
    private final DoubleSupplier random;

    public ExponentialBackoffWithJitter() {
        this(DEFAULT_INITIAL, DEFAULT_MAX, DEFAULT_MULTIPLIER, DEFAULT_JITTER_FACTOR,
                () -> ThreadLocalRandom.current().nextDouble());
    }

    public ExponentialBackoffWithJitter(Duration initial, Duration max, double multiplier,
                                        double jitterFactor, DoubleSupplier random) {
        this.initialMs = initial.toMillis();
        this.maxMs = max.toMillis();
        this.multiplier = multiplier;
        this.jitterFactor = jitterFactor;
        this.random = random;
    }

    @Override
    public Duration backoffFor(int attempt) {
        double exponential = initialMs * Math.pow(multiplier, Math.max(0, attempt - 1));
        long capped = Math.min((long) exponential, maxMs);

        // +/- jitterFactor of the capped interval
        double jitter = (random.getAsDouble() * 2 - 1) * jitterFactor * capped;

        // Never shorter than the initial interval: a negative jitter draw on
        // the first attempt would otherwise produce a near-zero wait and
        // hammer a queue manager that is still coming up. And never longer
        // than the cap: jitter was previously applied after capping, so a
        // high draw waited up to 20% beyond the documented ceiling.
        long jittered = (long) (capped + jitter);
        return Duration.ofMillis(Math.min(maxMs, Math.max(initialMs, jittered)));
    }
}
