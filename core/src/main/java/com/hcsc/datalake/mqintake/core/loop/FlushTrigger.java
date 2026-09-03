package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;

import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Clock;
import java.time.Instant;

/**
 * Determines when a batch should be flushed.
 *
 * <p>Four triggers, whichever fires first:
 * <ul>
 *   <li>Message count: batch_size reached</li>
 *   <li>Accumulated bytes: batch_bytes reached</li>
 *   <li>Partition boundary: the batch would otherwise span two partition
 *       windows (always active — see below)</li>
 *   <li>Elapsed time: batch_interval_ms since the batch opened. Set
 *       batch_interval_ms to 0 to disable, leaving the partition boundary as
 *       the only time-based trigger.</li>
 * </ul>
 *
 * <p>The two feeds hit opposite constraints:
 * <ul>
 *   <li>RMS (lower volume): usually flushes on TIME or PARTITION trigger</li>
 *   <li>Claims (higher volume): usually flushes on SIZE or BYTES trigger</li>
 * </ul>
 *
 * <p>Do not assume one trigger dominates — all must be implemented correctly.
 *
 * <p><strong>Why the partition trigger is unconditional.</strong> The legacy
 * writer rolls a file only when its partition path changes, so a quiet window
 * yields one file. A fixed interval instead yields one file per interval, and
 * at trickle volume approaches one file per message — the shape of the failed
 * JSONL migration that exhausted the MQ listeners. Bounding a batch to a single
 * partition window restores the legacy cadence as a floor, independently of how
 * batch_interval_ms is tuned, and keeps a window's data in one file rather than
 * spread across whichever partitions were current at each flush.
 *
 * <p><strong>The interval measures from the first message, not from reset.</strong>
 * Otherwise an idle gap longer than the interval counts against the next
 * message, which flushes it alone the moment it arrives — again one file per
 * message. The interval bounds how long a message waits, so it starts when
 * there is a message to wait.
 */
public class FlushTrigger {

    /**
     * Identifies which trigger caused a flush.
     */
    public enum Trigger {
        NONE,
        SIZE,
        BYTES,
        TIME,
        PARTITION
    }

    private final int maxBatchSize;
    private final long maxBatchBytes;
    private final long maxBatchIntervalMs;
    private final Clock clock;

    private Instant batchAnchor;
    private long batchStartTimeMs;
    private long batchWindowId;
    private boolean batchOpen;
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
        anchorTo(clock.instant());
        this.batchOpen = false;
        this.accumulatedBytes = 0;
        this.messageCount = 0;
    }

    /**
     * Fixes the batch's anchor from a SINGLE clock reading.
     *
     * <p>The interval start, the partition window and the instant handed to
     * the writer must all describe the same moment. Reading the clock once per
     * derived value let a read straddle a window boundary, so a batch could be
     * timed against one window and written into another.
     */
    private void anchorTo(Instant now) {
        this.batchAnchor = now;
        this.batchStartTimeMs = now.toEpochMilli();
        this.batchWindowId = PartitionPath.windowId(now);
    }

    /**
     * Tracks a message for size-based flushing.
     * Call this for each message added to the batch.
     */
    public void trackMessage(Message message) {
        trackMessage(estimateMessageSize(message));
    }

    /**
     * Tracks a message with explicit size (for testing or pre-computed sizes).
     */
    public void trackMessage(long sizeBytes) {
        openBatch();
        messageCount++;
        accumulatedBytes += sizeBytes;
    }

    /**
     * Marks the batch as opened by its first message, anchoring both the
     * interval and the partition window to that moment.
     */
    private void openBatch() {
        if (!batchOpen) {
            batchOpen = true;
            anchorTo(clock.instant());
        }
    }

    /**
     * The instant this batch opened — the partition window its messages
     * arrived in.
     *
     * <p><strong>Not what the file is filed under.</strong> The writer files
     * by flush time, so a partition-triggered batch lands in the following
     * window; that is a deliberate contract decision, explained on
     * {@code SequenceFileBatchWriter.write} and in READINESS_REVIEW.md §F.6.
     * This accessor exposes the batch's own window for diagnostics, and so
     * that reversing §F.6 stays a small change if the downstream consumers
     * ask for it. Do not wire it into placement without that decision.
     *
     * <p>Before a batch has opened this is the reset instant.
     */
    public Instant getBatchAnchor() {
        return batchAnchor;
    }

    /**
     * Returns true if the batch has messages and the clock has since moved into
     * a different partition window.
     */
    public boolean isPartitionBoundaryCrossed() {
        return batchOpen && PartitionPath.windowId(clock.instant()) != batchWindowId;
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
     * Triggers are checked in order: SIZE, BYTES, PARTITION, TIME.
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

        // Partition trigger: the batch must not span two partition windows
        if (isPartitionBoundaryCrossed()) {
            return Trigger.PARTITION;
        }

        // Time trigger: batch_interval_ms elapsed (0 or less disables it)
        if (maxBatchIntervalMs > 0 && getElapsedMs() >= maxBatchIntervalMs) {
            return Trigger.TIME;
        }

        return Trigger.NONE;
    }

    /**
     * Checks if the time trigger has expired.
     * Used for flushing partial batches on timeout.
     * Always false when the interval is disabled (0 or less).
     */
    public boolean isTimeoutExpired() {
        return maxBatchIntervalMs > 0 && getElapsedMs() >= maxBatchIntervalMs;
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
                    return utf8Length(text);
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

    /**
     * Exact UTF-8 encoded length, which is what actually gets written and so
     * what batch_bytes must bound. String.length() counts UTF-16 code units and
     * undercounts by up to 3x on non-ASCII payloads, letting batches grow past
     * the configured ceiling. Computed arithmetically rather than via
     * getBytes(), which would copy every payload on the hot path — claims runs
     * batches of 8000.
     */
    private static long utf8Length(String s) {
        long bytes = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                bytes += 1;
            } else if (c < 0x800) {
                bytes += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < s.length()
                    && Character.isLowSurrogate(s.charAt(i + 1))) {
                bytes += 4;
                i++;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }
}
