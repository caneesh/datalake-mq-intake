package com.hcsc.datalake.mqintake.core.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for BindingMetrics.
 *
 * <p>From DESIGN.md §14: Metrics dimensioned by binding_id.
 */
class BindingMetricsTest {

    private BindingMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new BindingMetrics("test-binding");
    }

    @Test
    void constructorSetsBindingId() {
        assertThat(metrics.getBindingId()).isEqualTo("test-binding");
    }

    @Test
    void countersStartAtZero() {
        assertThat(metrics.getCommitCount()).isEqualTo(0);
        assertThat(metrics.getRollbackCount()).isEqualTo(0);
        assertThat(metrics.getMessagesConsumed()).isEqualTo(0);
        assertThat(metrics.getMessagesWritten()).isEqualTo(0);
        assertThat(metrics.getBytesWritten()).isEqualTo(0);
        assertThat(metrics.getPoisonMessagesRouted()).isEqualTo(0);
        assertThat(metrics.getDegradedModeEntries()).isEqualTo(0);
        assertThat(metrics.getDegradedModeExits()).isEqualTo(0);
    }

    @Test
    void recordCommitIncrementsCount() {
        metrics.recordCommit();
        metrics.recordCommit();
        metrics.recordCommit();

        assertThat(metrics.getCommitCount()).isEqualTo(3);
    }

    @Test
    void recordRollbackIncrementsCount() {
        metrics.recordRollback();
        metrics.recordRollback();

        assertThat(metrics.getRollbackCount()).isEqualTo(2);
    }

    @Test
    void recordMessagesConsumedAddsToTotal() {
        metrics.recordMessagesConsumed(100);
        metrics.recordMessagesConsumed(50);

        assertThat(metrics.getMessagesConsumed()).isEqualTo(150);
    }

    @Test
    void recordMessagesWrittenTracksBothCountAndBytes() {
        metrics.recordMessagesWritten(100, 50000);
        metrics.recordMessagesWritten(50, 25000);

        assertThat(metrics.getMessagesWritten()).isEqualTo(150);
        assertThat(metrics.getBytesWritten()).isEqualTo(75000);
    }

    @Test
    void recordPoisonMessageRoutedIncrementsCount() {
        metrics.recordPoisonMessageRouted();
        metrics.recordPoisonMessageRouted();

        assertThat(metrics.getPoisonMessagesRouted()).isEqualTo(2);
    }

    @Test
    void degradedModeEntryOnlyCountsOnce() {
        metrics.recordDegradedModeEntry();
        metrics.recordDegradedModeEntry(); // Already in degraded, should not increment
        metrics.recordDegradedModeEntry();

        assertThat(metrics.getDegradedModeEntries()).isEqualTo(1);
        assertThat(metrics.isInDegradedMode()).isTrue();
    }

    @Test
    void degradedModeExitOnlyCountsOnce() {
        metrics.recordDegradedModeEntry();

        metrics.recordDegradedModeExit();
        metrics.recordDegradedModeExit(); // Already exited, should not increment

        assertThat(metrics.getDegradedModeExits()).isEqualTo(1);
        assertThat(metrics.isInDegradedMode()).isFalse();
    }

    @Test
    void degradedModeToggleCountsBothEntriesAndExits() {
        // Enter -> Exit -> Enter -> Exit
        metrics.recordDegradedModeEntry();
        metrics.recordDegradedModeExit();
        metrics.recordDegradedModeEntry();
        metrics.recordDegradedModeExit();

        assertThat(metrics.getDegradedModeEntries()).isEqualTo(2);
        assertThat(metrics.getDegradedModeExits()).isEqualTo(2);
    }

    @Test
    void gaugesCanBeSet() {
        metrics.setSourceQueueDepth(100);
        metrics.setTrackerQueueDepth(50);
        metrics.setBackoutQueueDepth(5);
        metrics.setCurrentBatchSize(1000);

        assertThat(metrics.getSourceQueueDepth()).isEqualTo(100);
        assertThat(metrics.getTrackerQueueDepth()).isEqualTo(50);
        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(5);
        assertThat(metrics.getCurrentBatchSize()).isEqualTo(1000);
    }

    @Test
    void healthyDefaultsToTrue() {
        assertThat(metrics.isHealthy()).isTrue();
    }

    @Test
    void healthyCanBeSet() {
        metrics.setHealthy(false);
        assertThat(metrics.isHealthy()).isFalse();

        metrics.setHealthy(true);
        assertThat(metrics.isHealthy()).isTrue();
    }

    @Test
    void flushLatencyTracking() {
        metrics.recordFlushLatency(Duration.ofMillis(100));
        metrics.recordFlushLatency(Duration.ofMillis(200));
        metrics.recordFlushLatency(Duration.ofMillis(300));

        assertThat(metrics.getFlushCount()).isEqualTo(3);
        assertThat(metrics.getLastFlushLatency()).isEqualTo(Duration.ofMillis(300));
        assertThat(metrics.getAverageFlushLatency()).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void averageFlushLatencyIsZeroWithNoFlushes() {
        assertThat(metrics.getAverageFlushLatency()).isEqualTo(Duration.ZERO);
    }

    @Test
    void snapshotCapturesAllMetrics() {
        metrics.recordCommit();
        metrics.recordCommit();
        metrics.recordRollback();
        metrics.recordMessagesWritten(100, 50000);
        metrics.recordDegradedModeEntry();
        metrics.setSourceQueueDepth(500);
        metrics.recordFlushLatency(Duration.ofMillis(150));

        var snapshot = metrics.snapshot();

        assertThat(snapshot.getBindingId()).isEqualTo("test-binding");
        assertThat(snapshot.getCommitCount()).isEqualTo(2);
        assertThat(snapshot.getRollbackCount()).isEqualTo(1);
        assertThat(snapshot.getMessagesWritten()).isEqualTo(100);
        assertThat(snapshot.getBytesWritten()).isEqualTo(50000);
        assertThat(snapshot.isInDegradedMode()).isTrue();
        assertThat(snapshot.getSourceQueueDepth()).isEqualTo(500);
        assertThat(snapshot.getLastFlushLatency()).isEqualTo(Duration.ofMillis(150));
    }
}
