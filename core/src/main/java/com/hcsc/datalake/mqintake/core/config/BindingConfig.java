package com.hcsc.datalake.mqintake.core.config;

/**
 * Configuration for a single binding: a self-contained pipeline from
 * one source queue to one HDFS landing path.
 */
public class BindingConfig {

    private String id;
    private String sourceQueue;
    private BindingMode mode;
    private String trackerQueue;
    private TrackerBodyMode trackerBodyMode = TrackerBodyMode.FULL_COPY;
    private TrackerFields trackerFields;
    private String hdfsBasePath;
    private int batchSize;
    private long batchBytes;
    private long batchIntervalMs;
    private int listenerThreads;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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
