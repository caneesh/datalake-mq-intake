package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for FlushTrigger.
 *
 * Verifies that each trigger fires independently:
 * - SIZE: when batch_size messages are accumulated
 * - BYTES: when batch_bytes are accumulated
 * - TIME: when batch_interval_ms elapses
 *
 * The two feeds hit opposite constraints:
 * - RMS (lower volume): usually flushes on TIME trigger
 * - Claims (higher volume): usually flushes on SIZE or BYTES trigger
 */
class FlushTriggerTest {

    // --- Size Trigger Tests ---

    @Test
    void sizeTriggerFiresAtExactlyBatchSize() {
        FlushTrigger trigger = new FlushTrigger(5, Long.MAX_VALUE, Long.MAX_VALUE);

        // Add 4 messages - should not flush
        for (int i = 0; i < 4; i++) {
            trigger.trackMessage(100);
            assertThat(trigger.shouldFlush()).isFalse();
        }

        // Add 5th message - should flush
        trigger.trackMessage(100);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.SIZE);
    }

    @Test
    void sizeTriggerDoesNotFireBelowBatchSize() {
        FlushTrigger trigger = new FlushTrigger(100, Long.MAX_VALUE, Long.MAX_VALUE);

        for (int i = 0; i < 99; i++) {
            trigger.trackMessage(100);
        }

        assertThat(trigger.shouldFlush()).isFalse();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.NONE);
        assertThat(trigger.getMessageCount()).isEqualTo(99);
    }

    @Test
    void batchNeverExceedsBatchSize() {
        // Simulating the receive loop: check shouldFlush after each message
        // and verify we always flush exactly at batch_size
        FlushTrigger trigger = new FlushTrigger(5, Long.MAX_VALUE, Long.MAX_VALUE);

        int messagesBeforeFlush = 0;
        for (int i = 0; i < 10; i++) {
            trigger.trackMessage(100);
            messagesBeforeFlush++;
            if (trigger.shouldFlush()) {
                // Should flush at exactly 5
                assertThat(messagesBeforeFlush).isEqualTo(5);
                trigger.reset();
                messagesBeforeFlush = 0;
            }
        }
    }

    // --- Bytes Trigger Tests ---

    @Test
    void bytesTriggerFiresAtExactlyBatchBytes() {
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, 1000, Long.MAX_VALUE);

        // Add messages totaling 900 bytes - should not flush
        trigger.trackMessage(400);
        assertThat(trigger.shouldFlush()).isFalse();
        trigger.trackMessage(500);
        assertThat(trigger.shouldFlush()).isFalse();

        // Add message pushing over 1000 bytes - should flush
        trigger.trackMessage(200);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.BYTES);
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(1100);
    }

    @Test
    void bytesTriggerDoesNotFireBelowBatchBytes() {
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, 1000, Long.MAX_VALUE);

        trigger.trackMessage(999);
        assertThat(trigger.shouldFlush()).isFalse();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.NONE);
    }

    @Test
    void bytesTriggerAccumulatesAcrossMessages() {
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, 500, Long.MAX_VALUE);

        trigger.trackMessage(100);
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(100);

        trigger.trackMessage(200);
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(300);

        trigger.trackMessage(150);
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(450);
        assertThat(trigger.shouldFlush()).isFalse();

        trigger.trackMessage(100);
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(550);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.BYTES);
    }

    // --- Time Trigger Tests ---

    @Test
    void timeTriggerFiresAfterBatchInterval() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 1000, clock);

        // Add a message
        trigger.trackMessage(100);
        assertThat(trigger.shouldFlush()).isFalse();

        // Advance time but not enough
        clock.advance(999);
        assertThat(trigger.shouldFlush()).isFalse();

        // Advance to exactly the interval
        clock.advance(1);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.TIME);
    }

    @Test
    void timeTriggerFiresOnPartialBatch() {
        // RMS scenario: low volume, partial batch flushes on time
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(4000, 128 * 1024 * 1024, 30000, clock);

        // Only 100 messages received (well below batch_size of 4000)
        for (int i = 0; i < 100; i++) {
            trigger.trackMessage(1000); // ~1KB each = 100KB total
        }
        assertThat(trigger.shouldFlush()).isFalse();
        assertThat(trigger.getMessageCount()).isEqualTo(100);

        // 30 seconds elapse - should flush despite partial batch
        clock.advance(30000);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.TIME);
    }

    @Test
    void timeTriggerDoesNotFireOnEmptyBatch() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 1000, clock);

        // No messages, time elapses
        clock.advance(2000);

        // Should not flush empty batch
        assertThat(trigger.shouldFlush()).isFalse();
        assertThat(trigger.isTimeoutExpired()).isTrue(); // But timeout IS expired
    }

    @Test
    void intervalAnchorsToFirstMessageNotToReset() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 1000, clock);

        // With no messages the interval is measured from reset
        assertThat(trigger.isTimeoutExpired()).isFalse();
        clock.advance(1500);
        assertThat(trigger.isTimeoutExpired()).isTrue();

        // The first message opens the batch and re-anchors the interval, so an
        // idle gap does not count against it. Previously the message would
        // arrive already timed out and flush alone — one file per message at
        // trickle volume, the shape that exhausted the MQ listeners.
        trigger.trackMessage(100);
        assertThat(trigger.isTimeoutExpired()).isFalse();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.NONE);

        // It then gets its own full interval to accumulate
        clock.advance(1000);
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.TIME);
    }

    // --- Partition Boundary Trigger ---

    /** Quarter-hour partition window, in ms. */
    private static final long WINDOW = 15L * 60L * 1000L;

    @Test
    void partitionBoundaryFlushesEvenWhenIntervalDisabled() {
        TestClock clock = new TestClock();
        // Interval disabled: the partition boundary is the only time trigger
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 0, clock);

        clock.set(WINDOW * 100 + 60_000); // one minute into a window
        trigger.reset();
        trigger.trackMessage(100);

        // Still inside the same window
        clock.advance(60_000);
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.NONE);

        // Crossing into the next window forces the flush
        clock.set(WINDOW * 101);
        assertThat(trigger.isPartitionBoundaryCrossed()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.PARTITION);
    }

    @Test
    void partitionBoundaryDoesNotFireWithoutMessages() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 0, clock);

        clock.set(WINDOW * 100);
        trigger.reset();

        // An empty batch spanning a boundary must not produce an empty file
        clock.set(WINDOW * 102);
        assertThat(trigger.isPartitionBoundaryCrossed()).isFalse();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.NONE);
    }

    @Test
    void sizeTriggerTakesPriorityOverPartition() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(2, Long.MAX_VALUE, 0, clock);

        clock.set(WINDOW * 100);
        trigger.reset();
        trigger.trackMessage(100);
        trigger.trackMessage(100);

        clock.set(WINDOW * 101); // boundary crossed as well
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.SIZE);
    }

    @Test
    void resetClearsPartitionAnchor() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 0, clock);

        clock.set(WINDOW * 100);
        trigger.reset();
        trigger.trackMessage(100);
        clock.set(WINDOW * 101);
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.PARTITION);

        // After the flush, the next batch anchors to the new window
        trigger.reset();
        trigger.trackMessage(100);
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.NONE);
    }

    @Test
    void anchorIsTheBatchOpenInstantNotTheFlushInstant() {
        // The anchor records the window the messages ARRIVED in. The file is
        // filed under the window it is WRITTEN in (§F.6) -- these differ by
        // design, and this pins the accessor's meaning, not the placement.
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 0, clock);

        clock.set(WINDOW * 100);
        trigger.reset();

        // The batch opens a minute into window 100...
        clock.advance(60_000);
        trigger.trackMessage(100);
        Instant opened = clock.instant();

        // ...and is flushed just after window 100 closes, which is how the
        // partition trigger always fires. The anchor must still describe
        // window 100 even though the file will be filed under 101.
        clock.set(WINDOW * 101 + 500);
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.PARTITION);
        assertThat(trigger.getBatchAnchor()).isEqualTo(opened);
        assertThat(PartitionPath.windowId(trigger.getBatchAnchor()))
                .as("the anchor is the batch's own window, whatever the file is filed under")
                .isEqualTo(100L)
                .isNotEqualTo(PartitionPath.windowId(clock.instant()));
    }

    @Test
    void anchorMovesToTheNewWindowOnlyWhenTheNextBatchOpens() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 0, clock);

        clock.set(WINDOW * 100);
        trigger.reset();
        trigger.trackMessage(100);

        clock.set(WINDOW * 101);
        trigger.reset();

        // reset() alone anchors to now; the first message of the next batch
        // re-anchors to its own arrival, so a batch that opens late in a
        // window is still stamped with that window.
        clock.advance(30_000);
        trigger.trackMessage(100);
        assertThat(PartitionPath.windowId(trigger.getBatchAnchor())).isEqualTo(101L);
    }

    @Test
    void aClockThatMovesBetweenReadsStillAnchorsToOneWindow() {
        // reset()/openBatch() used to read the clock twice — once for the
        // interval start, once for the window id. A boundary falling between
        // the two reads anchored the batch to window N+1 while its first
        // message arrived in N, so the partition trigger would not fire until
        // N+2 and the batch spanned two windows. Both values now come from a
        // single reading.
        // A full window per read, so ANY two reads taken while anchoring one
        // batch land in different windows.
        SteppingClock clock = new SteppingClock(WINDOW * 100, WINDOW);
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 0, clock);

        trigger.reset();
        trigger.trackMessage(100);

        long anchorWindow = PartitionPath.windowId(trigger.getBatchAnchor());
        clock.freezeAt(anchorWindow * WINDOW + 1);   // still inside the anchor's own window

        assertThat(trigger.getActiveTrigger())
                .as("a batch must not flush while the clock is still in the window it opened in")
                .isEqualTo(FlushTrigger.Trigger.NONE);
    }

    /**
     * A clock that advances on every read, so a window boundary can fall
     * between two reads taken while anchoring one batch.
     */
    private static class SteppingClock extends Clock {
        private final AtomicLong now;
        private final long stepMillis;
        private volatile boolean frozen;

        SteppingClock(long startMillis, long stepMillis) {
            this.now = new AtomicLong(startMillis);
            this.stepMillis = stepMillis;
        }

        void freezeAt(long millis) {
            now.set(millis);
            frozen = true;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public long millis() {
            return frozen ? now.get() : now.getAndAdd(stepMillis);
        }
    }

    @Test
    void batchSpanningQuietWindowsProducesOneFilePerWindow() {
        TestClock clock = new TestClock();
        // No size/bytes pressure and no interval: only the boundary flushes
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, Long.MAX_VALUE, 0, clock);

        clock.set(WINDOW * 100);
        trigger.reset();

        int flushes = 0;
        // One message every 5 minutes across three windows = 9 messages.
        // With a 30s interval this was 9 files; aligned to the boundary it is
        // one file per window.
        for (int i = 0; i < 9; i++) {
            trigger.trackMessage(100);
            clock.advance(5L * 60L * 1000L);
            if (trigger.shouldFlush()) {
                flushes++;
                trigger.reset();
            }
        }

        assertThat(flushes)
                .as("one flush per partition window crossed, not one per message")
                .isEqualTo(3);
    }

    // --- Trigger Priority Tests ---

    @Test
    void sizeTriggerTakesPriorityOverBytesAndTime() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(5, 100, 100, clock);

        // Set up all three triggers to fire
        for (int i = 0; i < 5; i++) {
            trigger.trackMessage(50); // 250 bytes > 100
        }
        clock.advance(200); // > 100ms

        // Size should be reported (checked first)
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.SIZE);
    }

    @Test
    void bytesTriggerTakesPriorityOverTime() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(Integer.MAX_VALUE, 100, 100, clock);

        // Set up bytes and time triggers to fire
        trigger.trackMessage(150); // > 100 bytes
        clock.advance(200); // > 100ms

        // Bytes should be reported (checked before time)
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.BYTES);
    }

    // --- Reset Tests ---

    @Test
    void resetClearsAllState() {
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(10, 1000, 1000, clock);

        // Accumulate state
        for (int i = 0; i < 5; i++) {
            trigger.trackMessage(100);
        }
        clock.advance(500);

        assertThat(trigger.getMessageCount()).isEqualTo(5);
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(500);
        assertThat(trigger.getElapsedMs()).isEqualTo(500);

        // Reset
        trigger.reset();

        assertThat(trigger.getMessageCount()).isEqualTo(0);
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(0);
        assertThat(trigger.getElapsedMs()).isEqualTo(0);
        assertThat(trigger.shouldFlush()).isFalse();
    }

    // --- Scenario Tests ---

    @Test
    void rmsScenario_lowVolume_flushesOnTime() {
        // RMS: batch_size=4000, batch_bytes=128MB, interval=30s
        // At low volume, time trigger fires first
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(4000, 128 * 1024 * 1024, 30000, clock);

        // Receive 500 messages over 30 seconds (low volume)
        for (int i = 0; i < 500; i++) {
            trigger.trackMessage(2000); // 2KB each = 1MB total
        }
        assertThat(trigger.shouldFlush()).isFalse();

        // 30 seconds pass
        clock.advance(30000);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.TIME);
    }

    @Test
    void claimsScenario_highVolume_flushesOnSize() {
        // Claims: batch_size=8000, batch_bytes=128MB, interval=30s
        // At high volume, size trigger fires first
        TestClock clock = new TestClock();
        FlushTrigger trigger = new FlushTrigger(8000, 128 * 1024 * 1024, 30000, clock);

        // Receive 8000 messages quickly (high volume)
        for (int i = 0; i < 7999; i++) {
            trigger.trackMessage(1000);
            assertThat(trigger.shouldFlush()).isFalse();
        }

        // 8000th message
        trigger.trackMessage(1000);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.SIZE);

        // Only 5 seconds elapsed
        assertThat(trigger.getElapsedMs()).isLessThan(30000);
    }

    @Test
    void claimsScenario_largeMessages_flushesOnBytes() {
        // Claims with large messages: bytes trigger fires before size
        TestClock clock = new TestClock();
        long maxBytes = 128 * 1024 * 1024L; // 128 MB
        FlushTrigger trigger = new FlushTrigger(8000, maxBytes, 30000, clock);

        // Receive large messages (100KB each)
        // 128MB (134,217,728) / 100KB (102,400) = 1311 messages to hit bytes trigger
        long msgSize = 100 * 1024;
        int messagesNeeded = (int) (maxBytes / msgSize); // 1310

        for (int i = 0; i < messagesNeeded; i++) {
            trigger.trackMessage(msgSize);
            assertThat(trigger.shouldFlush()).isFalse();
        }

        // One more tips it over
        trigger.trackMessage(msgSize);
        assertThat(trigger.shouldFlush()).isTrue();
        assertThat(trigger.getActiveTrigger()).isEqualTo(FlushTrigger.Trigger.BYTES);
        assertThat(trigger.getMessageCount()).isLessThan(8000); // Well below batch_size
    }

    // --- Test Clock ---

    /**
     * Controllable clock for testing time-based behavior.
     */
    private static class TestClock extends Clock {
        private final AtomicLong currentTime = new AtomicLong(0);

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(currentTime.get());
        }

        @Override
        public long millis() {
            return currentTime.get();
        }

        public void advance(long millis) {
            currentTime.addAndGet(millis);
        }

        public void set(long millis) {
            currentTime.set(millis);
        }
    }

    @Test
    void byteTriggerCountsUtf8BytesNotUtf16CodeUnits() throws Exception {
        // batch_bytes bounds what actually gets written, which is UTF-8.
        // String.length() counts UTF-16 code units and undercounts by up to
        // 3x on non-ASCII, letting batches grow well past the ceiling.
        FlushTrigger trigger = new FlushTrigger(1000, 1000, 0);

        javax.jms.TextMessage message = org.mockito.Mockito.mock(javax.jms.TextMessage.class);
        org.mockito.Mockito.when(message.getText()).thenReturn("\u20ac\u20ac\u20ac\u20ac");

        trigger.trackMessage(message);

        // 4 characters, 3 UTF-8 bytes each
        assertThat(trigger.getAccumulatedBytes()).isEqualTo(12);
    }

    @Test
    void utf8ByteCountHandlesSurrogatePairs() throws Exception {
        FlushTrigger trigger = new FlushTrigger(1000, 1000, 0);

        javax.jms.TextMessage message = org.mockito.Mockito.mock(javax.jms.TextMessage.class);
        // One emoji: 2 UTF-16 code units, 4 UTF-8 bytes
        org.mockito.Mockito.when(message.getText()).thenReturn("\ud83d\ude00");

        trigger.trackMessage(message);

        assertThat(trigger.getAccumulatedBytes()).isEqualTo(4);
    }

}
