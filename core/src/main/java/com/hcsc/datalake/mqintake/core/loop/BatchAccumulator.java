package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;

import javax.jms.Message;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The messages held between one commit and the next, and the decision about
 * when to stop holding them.
 *
 * <p>Extracted from the receive loop unchanged. It pairs the message list with
 * the {@link FlushTrigger} that watches it, because the two were only ever
 * correct together: every place the loop cleared the list also had to reset the
 * trigger, and a batch that lost one but not the other reads as a batch that
 * grows or shrinks without explanation.
 *
 * <p>Confined to one listener thread, like everything else on the loop's hot
 * path. Nothing here is synchronised, and it must not be shared.
 *
 * <p>Deliberately NOT included: the effective batch size. Degraded mode can
 * shrink it between polls, so the loop asks its degradation policy each time
 * rather than caching a number here that would go stale.
 */
class BatchAccumulator {

    private final List<Message> messages;
    private final FlushTrigger flushTrigger;
    private final BindingMetrics metrics;

    BatchAccumulator(int expectedSize, long maxBatchBytes, long maxBatchIntervalMs,
                     java.time.Clock clock, BindingMetrics metrics) {
        this.messages = new ArrayList<>(expectedSize);
        this.flushTrigger = new FlushTrigger(expectedSize, maxBatchBytes, maxBatchIntervalMs, clock);
        this.metrics = metrics;
    }

    /**
     * Adds a message and updates the in-flight gauge.
     *
     * <p>One atomic store per message, negligible beside the receive that
     * produced it.
     */
    void add(Message message) {
        messages.add(message);
        flushTrigger.trackMessage(message);
        metrics.setCurrentBatchSize(messages.size());
    }

    /** True when any trigger says this batch should close. */
    boolean shouldFlush() {
        return flushTrigger.shouldFlush();
    }

    boolean isEmpty() {
        return messages.isEmpty();
    }

    int size() {
        return messages.size();
    }

    /** The messages, for the caller to process. Not a copy: the hot path. */
    List<Message> messages() {
        return messages;
    }

    /**
     * The window this batch's messages arrived in.
     *
     * <p>Not what the file is filed under — the writer files by flush time,
     * which is a deliberate contract decision. Exposed for diagnostics.
     */
    Instant anchor() {
        return flushTrigger.getBatchAnchor();
    }

    /**
     * Empties the batch and rearms the trigger.
     *
     * <p>Both together, always. Clearing the messages without resetting the
     * trigger leaves the next batch inheriting the previous one's byte total
     * and interval, so it flushes early and shrinks; resetting without
     * clearing double-counts. Every caller in the loop did both, and pairing
     * them here is the reason this class exists.
     */
    void reset() {
        messages.clear();
        flushTrigger.reset();
        metrics.setCurrentBatchSize(0);
    }
}
