package com.hcsc.datalake.mqintake.core.loop;

import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Clock;

/**
 * Determines when a batch should be flushed.
 *
 * <p>Three triggers, whichever fires first:
 * <ul>
 *   <li>Message count: batch_size reached</li>
 *   <li>Accumulated bytes: batch_bytes reached</li>
 *   <li>Elapsed time: batch_interval_ms since batch opened</li>
 * </ul>
 *
 * <p>The two feeds hit opposite constraints:
 * <ul>
 *   <li>RMS (lower volume): usually flushes on TIME trigger</li>
 *   <li>Claims (higher volume): usually flushes on SIZE or BYTES trigger</li>
 * </ul>
 *
 * <p>Do not assume one trigger dominates — all three must be implemented correctly.
 */
public class FlushTrigger {

    /**
     * Identifies which trigger caused a flush.
     */
    public enum Trigger {
        NONE,
        SIZE,
        BYTES,
        TIME
    }

    private final int maxBatchSize;
    private final long maxBatchBytes;
    private final long maxBatchIntervalMs;
    private final Clock clock;

    private long batchStartTimeMs;
    private long accumulatedBytes;
    private int messageCount;

    /**
     * Creates a flush trigger with system clock.
     */
    public FlushTrigger(int maxBatchSize, long maxBatchBytes, long maxBatchIntervalMs) {
        this(maxBatchSize, maxBatchBytes, maxBatchIntervalMs, Clock.systemUTC());
    }

    /**
     * Creates a flush trigger with injectable clock (for testing).
     */
    public FlushTrigger(int maxBatchSize, long maxBatchBytes, long maxBatchIntervalMs, Clock clock) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchBytes = maxBatchBytes;
        this.maxBatchIntervalMs = maxBatchIntervalMs;
        this.clock = clock;
        reset();
    }

    /**
     * Resets the trigger state for a new batch.
     */
    public void reset() {
        this.batchStartTimeMs = clock.millis();
        this.accumulatedBytes = 0;
        this.messageCount = 0;
    }

    /**
     * Tracks a message for size-based flushing.
     * Call this for each message added to the batch.
     */
    public void trackMessage(Message message) {
        messageCount++;
        accumulatedBytes += estimateMessageSize(message);
    }

    /**
     * Tracks a message with explicit size (for testing or pre-computed sizes).
     */
    public void trackMessage(long sizeBytes) {
        messageCount++;
        accumulatedBytes += sizeBytes;
    }

    /**
     * Checks if the batch should be flushed.
     *
     * @return true if any flush trigger condition is met
     */
    public boolean shouldFlush() {
        return getActiveTrigger() != Trigger.NONE;
    }

    /**
     * Returns which trigger would cause a flush, or NONE.
     * Triggers are checked in order: SIZE, BYTES, TIME.
     */
    public Trigger getActiveTrigger() {
        if (messageCount == 0) {
            return Trigger.NONE;
        }

        // Size trigger: batch_size reached
        if (messageCount >= maxBatchSize) {
            return Trigger.SIZE;
        }

        // Bytes trigger: batch_bytes reached
        if (accumulatedBytes >= maxBatchBytes) {
            return Trigger.BYTES;
        }

        // Time trigger: batch_interval_ms elapsed
        if (getElapsedMs() >= maxBatchIntervalMs) {
            return Trigger.TIME;
        }

        return Trigger.NONE;
    }

    /**
     * Checks if the time trigger has expired.
     * Used for flushing partial batches on timeout.
     */
    public boolean isTimeoutExpired() {
        return getElapsedMs() >= maxBatchIntervalMs;
    }

    /**
     * Returns elapsed time since batch started.
     */
    public long getElapsedMs() {
        return clock.millis() - batchStartTimeMs;
    }

    public long getAccumulatedBytes() {
        return accumulatedBytes;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public long getMaxBatchBytes() {
        return maxBatchBytes;
    }

    public long getMaxBatchIntervalMs() {
        return maxBatchIntervalMs;
    }

    /**
     * Estimates message size in bytes.
     */
    private long estimateMessageSize(Message message) {
        try {
            if (message instanceof javax.jms.TextMessage) {
                String text = ((javax.jms.TextMessage) message).getText();
                if (text != null) {
                    // UTF-16 internal representation, but typically serialized as UTF-8
                    return text.length();
                }
                return 0;
            } else if (message instanceof javax.jms.BytesMessage) {
                return ((javax.jms.BytesMessage) message).getBodyLength();
            } else {
                // Default estimate for other message types
                return 1024;
            }
        } catch (JMSException e) {
            return 1024; // Default on error
        }
    }
}
