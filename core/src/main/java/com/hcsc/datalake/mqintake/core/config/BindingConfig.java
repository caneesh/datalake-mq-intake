package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;

/**
 * Configuration for a single binding: a self-contained pipeline from
 * one source queue to one HDFS landing path.
 */
public class BindingConfig {

    private String id;
    private String mqConnection;
    private String sourceQueue;
    private BindingMode mode;
    private String trackerQueue;
    private TrackerBodyMode trackerBodyMode = TrackerBodyMode.FULL_COPY;
    private TrackerFields trackerFields;
    private String hdfsBasePath;
    private int batchSize;
    private long batchBytes;
    private long batchIntervalMs;
    private int listenerThreads = 1;

    // Poison message handling (§6.1)
    private String backoutQueue;
    private int backoutThreshold = 5;

    /**
     * Whether a tracker build/send failure should fail the whole batch.
     *
     * <p>Default false, matching the legacy MDB: it catches and logs tracker
     * exceptions in both {@code HDFSIngest.forwardToMessageTracker} and
     * {@code EJBHelper}, so a tracker failure never rolls back the message.
     * The landed data is kept and that one tracker notification is lost.
     *
     * <p>Set true for the stricter reading of §2.2 — tracker and get in one
     * unit of work, so losing the tracker means replaying the message. That is
     * safer for the tracker consumer but turns a tracker-side outage into
     * repeated batch rollbacks on the landing path.
     *
     * <p>Note this only governs per-message failures. A broken session or
     * connection still surfaces at commit and rolls the batch back either way.
     */
    private boolean failBatchOnTrackerError = false;

    // Degraded mode (§6.1)
    private DegradationStrategy degradationStrategy = DegradationStrategy.BATCH_OF_ONE;
    private int successesRequiredToRestore = 10;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMqConnection() {
        return mqConnection;
    }

    public void setMqConnection(String mqConnection) {
        this.mqConnection = mqConnection;
    }

    public String getSourceQueue() {
        return sourceQueue;
    }

    public void setSourceQueue(String sourceQueue) {
        this.sourceQueue = sourceQueue;
    }

    public BindingMode getMode() {
        return mode;
    }

    public void setMode(BindingMode mode) {
        this.mode = mode;
    }

    public String getTrackerQueue() {
        return trackerQueue;
    }

    public void setTrackerQueue(String trackerQueue) {
        this.trackerQueue = trackerQueue;
    }

    public TrackerBodyMode getTrackerBodyMode() {
        return trackerBodyMode;
    }

    public void setTrackerBodyMode(TrackerBodyMode trackerBodyMode) {
        this.trackerBodyMode = trackerBodyMode;
    }

    public TrackerFields getTrackerFields() {
        return trackerFields;
    }

    public void setTrackerFields(TrackerFields trackerFields) {
        this.trackerFields = trackerFields;
    }

    public String getHdfsBasePath() {
        return hdfsBasePath;
    }

    public void setHdfsBasePath(String hdfsBasePath) {
        this.hdfsBasePath = hdfsBasePath;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getBatchBytes() {
        return batchBytes;
    }

    public void setBatchBytes(long batchBytes) {
        this.batchBytes = batchBytes;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public void setBatchIntervalMs(long batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
    }

    public int getListenerThreads() {
        return listenerThreads;
    }

    public void setListenerThreads(int listenerThreads) {
        this.listenerThreads = listenerThreads;
    }

    public String getBackoutQueue() {
        return backoutQueue;
    }

    public void setBackoutQueue(String backoutQueue) {
        this.backoutQueue = backoutQueue;
    }

    public int getBackoutThreshold() {
        return backoutThreshold;
    }

    public void setBackoutThreshold(int backoutThreshold) {
        this.backoutThreshold = backoutThreshold;
    }

    public boolean isFailBatchOnTrackerError() {
        return failBatchOnTrackerError;
    }

    public void setFailBatchOnTrackerError(boolean failBatchOnTrackerError) {
        this.failBatchOnTrackerError = failBatchOnTrackerError;
    }

    public DegradationStrategy getDegradationStrategy() {
        return degradationStrategy;
    }

    public void setDegradationStrategy(DegradationStrategy degradationStrategy) {
        this.degradationStrategy = degradationStrategy;
    }

    public int getSuccessesRequiredToRestore() {
        return successesRequiredToRestore;
    }

    public void setSuccessesRequiredToRestore(int successesRequiredToRestore) {
        this.successesRequiredToRestore = successesRequiredToRestore;
    }

    /**
     * Returns the maximum allowed batch size for this binding's mode.
     * TRACKED: MAXUMSGS / 2 (unit of work is 2N)
     * LAND_ONLY: MAXUMSGS (unit of work is N)
     */
    public int getMaxBatchSizeFor(int maxumsgs) {
        return mode == BindingMode.TRACKED ? maxumsgs / 2 : maxumsgs;
    }

    /**
     * Returns the memory footprint of this binding: batchBytes * listenerThreads.
     */
    public long getMemoryFootprint() {
        return batchBytes * listenerThreads;
    }
}
