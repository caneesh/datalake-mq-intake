package com.hcsc.datalake.mqintake.core.metrics;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collector for a single binding.
 *
 * <p>From DESIGN.md §14: Metrics dimensioned by binding_id:
 * <ul>
 *   <li>Source queue depth and consumption rate</li>
 *   <li>Tracker queue depth (TRACKED only)</li>
 *   <li>Backout queue depth (non-zero = page)</li>
 *   <li>Degraded-mode entry/exit events</li>
 *   <li>Batch commit rate, rollback rate, flush latency</li>
 *   <li>Per-binding health status</li>
 * </ul>
 *
 * <p>Aggregate dashboards hide a single stalled binding.
 * Alert on per-binding thresholds.
 */
public class BindingMetrics {

    private final String bindingId;

    // Counters
    private final LongAdder commitCount = new LongAdder();
    private final LongAdder rollbackCount = new LongAdder();
    private final LongAdder messagesConsumed = new LongAdder();
    private final LongAdder messagesWritten = new LongAdder();
    private final LongAdder bytesWritten = new LongAdder();
    private final LongAdder poisonMessagesRouted = new LongAdder();
    private final LongAdder degradedModeEntries = new LongAdder();
    private final LongAdder degradedModeExits = new LongAdder();
    private final LongAdder reconnectSuccessCount = new LongAdder();
    private final LongAdder reconnectFailureCount = new LongAdder();
    private final LongAdder auditFailureCount = new LongAdder();
    private final LongAdder trackerFailureCount = new LongAdder();
    /** Tracker messages actually put on the tracker queue. */
    private final LongAdder trackerSentCount = new LongAdder();
    /** Source messages the builder deliberately produced no tracker for. */
    private final LongAdder trackerSuppressedCount = new LongAdder();
    /** Batches rolled back because consumed != written + backout (ABC). */
    private final LongAdder balanceCheckFailureCount = new LongAdder();
    private final LongAdder reconciliationDiscrepancyCount = new LongAdder();

    // Gauges
    private final AtomicLong sourceQueueDepth = new AtomicLong(0);
    private final AtomicLong trackerQueueDepth = new AtomicLong(0);
    private final AtomicLong backoutQueueDepth = new AtomicLong(0);
    private final AtomicLong currentBatchSize = new AtomicLong(0);

    // Timing
    private final AtomicLong lastFlushLatencyNanos = new AtomicLong(0);
    private final LongAdder totalFlushLatencyNanos = new LongAdder();
    private final LongAdder flushCount = new LongAdder();

    // State. AtomicBoolean rather than volatile: entry/exit used a
    // check-then-act on a plain volatile, so two threads entering degraded
    // mode together — the normal case, since MQ redistributes a rolled-back
    // batch across listener threads — could both pass the check and
    // double-increment the counters.
    private final java.util.concurrent.atomic.AtomicBoolean inDegradedMode =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    private volatile boolean healthy = true;

    /** Unresolved suspect message IDs; non-zero for long = a limping binding. */
    private final AtomicLong suspectCount = new AtomicLong(0);

    /**
     * Supplies the binding serializer's identity-miss count, wired by the
     * runtime factory when the serializer reports one. A supplier rather than
     * a counter of our own because the serializer already owns the count;
     * duplicating it would invite drift.
     */
    private volatile java.util.function.LongSupplier identityMissSupplier = () -> 0L;

    public BindingMetrics(String bindingId) {
        this.bindingId = Objects.requireNonNull(bindingId, "bindingId required");
    }

    /**
     * A metrics sink for callers wired without metrics (tests, disabled
     * observability). It records into ordinary counters that nothing ever
     * reads — never registered with a meter registry — so callers can invoke
     * it unconditionally instead of null-checking before every recording.
     */
    public static BindingMetrics noop() {
        return new BindingMetrics("noop");
    }

    // --- Counter increments ---

    public void recordCommit() {
        commitCount.increment();
    }

    public void recordRollback() {
        rollbackCount.increment();
    }

    public void recordMessagesConsumed(int count) {
        messagesConsumed.add(count);
    }

    public void recordMessagesWritten(int count, long bytes) {
        messagesWritten.add(count);
        bytesWritten.add(bytes);
    }

    public void recordPoisonMessageRouted() {
        poisonMessagesRouted.increment();
    }

    public void recordDegradedModeEntry() {
        if (inDegradedMode.compareAndSet(false, true)) {
            degradedModeEntries.increment();
        }
    }

    public void recordDegradedModeExit() {
        if (inDegradedMode.compareAndSet(true, false)) {
            degradedModeExits.increment();
        }
    }

    public void recordFlushLatency(Duration latency) {
        long nanos = latency.toNanos();
        lastFlushLatencyNanos.set(nanos);
        totalFlushLatencyNanos.add(nanos);
        flushCount.increment();
    }

    public void recordReconnect() {
        reconnectSuccessCount.increment();
    }

    public void recordReconnectFailure() {
        reconnectFailureCount.increment();
    }

    /** A tracker message that could not be built or sent, per message. */
    public void recordTrackerFailure() {
        trackerFailureCount.increment();
    }

    /**
     * A tracker message put on the tracker queue.
     *
     * <p>The only POSITIVE tracker signal. Failures and suppressions are both
     * counted, but neither fires when tracking simply stops — an upstream that
     * drops {@code MessageHeaderDetails} produces suppressions, while an
     * unreachable tracker queue produces failures, and a binding
     * misconfigured to LAND_ONLY produces neither. Alerting on this counter
     * flatlining against {@code messages_consumed_total} catches all three.
     */
    public void recordTrackerSent() {
        trackerSentCount.increment();
    }

    /**
     * A source message the builder chose not to track.
     *
     * <p>Not an error: the RMS builder suppresses any message without
     * {@code MessageHeaderDetails}, which is what keeps claims-shaped messages
     * off the tracker queue. It becomes a problem only in bulk — if upstream
     * stops setting that property, every message lands and none is
     * acknowledged, which without this counter was visible only at DEBUG.
     */
    public void recordTrackerSuppressed() {
        trackerSuppressedCount.increment();
    }

    public void setIdentityMissSupplier(java.util.function.LongSupplier supplier) {
        this.identityMissSupplier = java.util.Objects.requireNonNull(supplier);
    }

    /** Payloads whose identity could not be extracted (0 when not wired). */
    public long getIdentityMisses() {
        return identityMissSupplier.getAsLong();
    }

    public void recordBalanceCheckFailure() {
        balanceCheckFailureCount.increment();
    }

    public long getBalanceCheckFailures() {
        return balanceCheckFailureCount.sum();
    }

    public void recordAuditFailure() {
        auditFailureCount.increment();
    }

    public void recordReconciliationDiscrepancy() {
        reconciliationDiscrepancyCount.increment();
    }

    // --- Gauge setters ---

    public void setSourceQueueDepth(long depth) {
        sourceQueueDepth.set(depth);
    }

    public void setTrackerQueueDepth(long depth) {
        trackerQueueDepth.set(depth);
    }

    public void setBackoutQueueDepth(long depth) {
        backoutQueueDepth.set(depth);
    }

    public void setCurrentBatchSize(long size) {
        currentBatchSize.set(size);
    }

    /**
     * Unresolved suspects. Previously collected but wired to nothing: an
     * orphaned suspect pinned the binding at reduced batch size invisibly
     * until restart, with no gauge an operator could alert on.
     */
    public void setSuspectCount(long count) {
        suspectCount.set(count);
    }

    public long getSuspectCount() {
        return suspectCount.get();
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    // --- Getters ---

    public String getBindingId() {
        return bindingId;
    }

    public long getCommitCount() {
        return commitCount.sum();
    }

    public long getRollbackCount() {
        return rollbackCount.sum();
    }

    public long getMessagesConsumed() {
        return messagesConsumed.sum();
    }

    public long getMessagesWritten() {
        return messagesWritten.sum();
    }

    public long getBytesWritten() {
        return bytesWritten.sum();
    }

    public long getPoisonMessagesRouted() {
        return poisonMessagesRouted.sum();
    }

    public long getDegradedModeEntries() {
        return degradedModeEntries.sum();
    }

    public long getDegradedModeExits() {
        return degradedModeExits.sum();
    }

    public long getReconnectSuccessCount() {
        return reconnectSuccessCount.sum();
    }

    public long getReconnectFailureCount() {
        return reconnectFailureCount.sum();
    }

    public long getTrackerSentCount() {
        return trackerSentCount.sum();
    }

    public long getTrackerSuppressedCount() {
        return trackerSuppressedCount.sum();
    }

    public long getTrackerFailureCount() {
        return trackerFailureCount.sum();
    }

    public long getAuditFailureCount() {
        return auditFailureCount.sum();
    }

    public long getReconciliationDiscrepancyCount() {
        return reconciliationDiscrepancyCount.sum();
    }

    public long getSourceQueueDepth() {
        return sourceQueueDepth.get();
    }

    public long getTrackerQueueDepth() {
        return trackerQueueDepth.get();
    }

    public long getBackoutQueueDepth() {
        return backoutQueueDepth.get();
    }

    public long getCurrentBatchSize() {
        return currentBatchSize.get();
    }

    public boolean isInDegradedMode() {
        return inDegradedMode.get();
    }

    public boolean isHealthy() {
        return healthy;
    }

    public Duration getLastFlushLatency() {
        return Duration.ofNanos(lastFlushLatencyNanos.get());
    }

    public Duration getAverageFlushLatency() {
        long count = flushCount.sum();
        if (count == 0) {
            return Duration.ZERO;
        }
        return Duration.ofNanos(totalFlushLatencyNanos.sum() / count);
    }

    public long getFlushCount() {
        return flushCount.sum();
    }

    /**
     * Returns a snapshot of all metrics.
     */
    public MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
                bindingId,
                getCommitCount(),
                getRollbackCount(),
                getMessagesConsumed(),
                getMessagesWritten(),
                getBytesWritten(),
                getPoisonMessagesRouted(),
                getDegradedModeEntries(),
                getDegradedModeExits(),
                getSourceQueueDepth(),
                getTrackerQueueDepth(),
                getBackoutQueueDepth(),
                getCurrentBatchSize(),
                isInDegradedMode(),
                isHealthy(),
                getLastFlushLatency(),
                getAverageFlushLatency(),
                getFlushCount()
        );
    }

    /**
     * Immutable snapshot of binding metrics.
     */
    public static class MetricsSnapshot {
        private final String bindingId;
        private final long commitCount;
        private final long rollbackCount;
        private final long messagesConsumed;
        private final long messagesWritten;
        private final long bytesWritten;
        private final long poisonMessagesRouted;
        private final long degradedModeEntries;
        private final long degradedModeExits;
        private final long sourceQueueDepth;
        private final long trackerQueueDepth;
        private final long backoutQueueDepth;
        private final long currentBatchSize;
        private final boolean inDegradedMode;
        private final boolean healthy;
        private final Duration lastFlushLatency;
        private final Duration averageFlushLatency;
        private final long flushCount;

        public MetricsSnapshot(String bindingId, long commitCount, long rollbackCount,
                                long messagesConsumed, long messagesWritten, long bytesWritten,
                                long poisonMessagesRouted, long degradedModeEntries,
                                long degradedModeExits, long sourceQueueDepth,
                                long trackerQueueDepth, long backoutQueueDepth,
                                long currentBatchSize, boolean inDegradedMode, boolean healthy,
                                Duration lastFlushLatency, Duration averageFlushLatency,
                                long flushCount) {
            this.bindingId = bindingId;
            this.commitCount = commitCount;
            this.rollbackCount = rollbackCount;
            this.messagesConsumed = messagesConsumed;
            this.messagesWritten = messagesWritten;
            this.bytesWritten = bytesWritten;
            this.poisonMessagesRouted = poisonMessagesRouted;
            this.degradedModeEntries = degradedModeEntries;
            this.degradedModeExits = degradedModeExits;
            this.sourceQueueDepth = sourceQueueDepth;
            this.trackerQueueDepth = trackerQueueDepth;
            this.backoutQueueDepth = backoutQueueDepth;
            this.currentBatchSize = currentBatchSize;
            this.inDegradedMode = inDegradedMode;
            this.healthy = healthy;
            this.lastFlushLatency = lastFlushLatency;
            this.averageFlushLatency = averageFlushLatency;
            this.flushCount = flushCount;
        }

        public String getBindingId() { return bindingId; }
        public long getCommitCount() { return commitCount; }
        public long getRollbackCount() { return rollbackCount; }
        public long getMessagesConsumed() { return messagesConsumed; }
        public long getMessagesWritten() { return messagesWritten; }
        public long getBytesWritten() { return bytesWritten; }
        public long getPoisonMessagesRouted() { return poisonMessagesRouted; }
        public long getDegradedModeEntries() { return degradedModeEntries; }
        public long getDegradedModeExits() { return degradedModeExits; }
        public long getSourceQueueDepth() { return sourceQueueDepth; }
        public long getTrackerQueueDepth() { return trackerQueueDepth; }
        public long getBackoutQueueDepth() { return backoutQueueDepth; }
        public long getCurrentBatchSize() { return currentBatchSize; }
        public boolean isInDegradedMode() { return inDegradedMode; }
        public boolean isHealthy() { return healthy; }
        public Duration getLastFlushLatency() { return lastFlushLatency; }
        public Duration getAverageFlushLatency() { return averageFlushLatency; }
        public long getFlushCount() { return flushCount; }
    }
}
