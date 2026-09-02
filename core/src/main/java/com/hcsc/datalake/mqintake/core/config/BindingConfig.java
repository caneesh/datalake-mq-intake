package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;

/**
 * Configuration for a single binding: a self-contained pipeline from
 * one source queue to one HDFS landing path.
 *
 * <p>Grouped by concern — {@code batch}, {@code hdfs}, {@code tracker},
 * {@code backout}, {@code audit}, {@code degradation} — so the YAML reads as
 * the pipeline's stages and each group can be handed to the collaborator it
 * configures. Property paths follow the groups
 * (e.g. {@code intake.bindings[0].batch.size}).
 *
 * <p><strong>Migration note (2026-08):</strong> these groups replaced a flat
 * key set ({@code batch-size}, {@code backout-queue}, …). Legacy flat keys no
 * longer bind; {@link LegacyBindingKeyDetector} fails startup naming any it
 * finds, so a stale override surfaces as a refusal to start rather than as a
 * silently ignored setting.
 */
public class BindingConfig {

    private String id;
    private String mqConnection;
    private String sourceQueue;
    private BindingMode mode;
    private int listenerThreads = 1;

    private Batch batch = new Batch();
    private Hdfs hdfs = new Hdfs();
    private Tracker tracker = new Tracker();
    private Backout backout = new Backout();
    private Audit audit = new Audit();
    private Degradation degradation = new Degradation();

    /**
     * Accepts this binding's placeholder serializer in production mode.
     *
     * <p>A placeholder serializer is one whose output format is not final.
     * Production mode refuses it by default, because data landed in a format
     * that later changes has to be reprocessed, and a serializer with no
     * identity field cannot be reconciled or de-duplicated at all.
     *
     * <p>Setting this accepts those consequences for one binding, deliberately
     * and visibly, rather than disarming production mode wholesale — which
     * would also switch off the dev-default connection gate, the tracker
     * contract check, and the refusal to write to a local filesystem. Losing
     * the last of those on a host that carries another cluster's configuration
     * is a far worse trade than the one being made here.
     *
     * <p>The acceptance is logged at startup and reported by preflight; it is
     * never silent.
     */
    private boolean acceptPlaceholderSerializer = false;

    /** Batch accumulation: when a unit of work is considered full. */
    public static class Batch {

        private int size;
        private long bytes;
        private long intervalMs;

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public long getBytes() {
            return bytes;
        }

        public void setBytes(long bytes) {
            this.bytes = bytes;
        }

        public long getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
        }
    }

    /** The HDFS landing: where files go and how durably they are written. */
    public static class Hdfs {

        private String basePath;

        /**
         * Whether to write a sidecar record index beside each landed file.
         *
         * <p>Off by default. The index is what lets reconciliation identify the
         * records in a file, but it is only meaningful when the binding's
         * serializer supplies a per-message identity. Enabling it for a binding
         * without one produces an index of nulls, which reconciliation would
         * read as missing records.
         */
        private boolean recordIndexEnabled = false;

        /**
         * Whether the batch is fsynced to DataNode disks (hsync) before close,
         * rather than only flushed to the replica pipeline (hflush).
         *
         * <p>Default true. hflush makes bytes visible to readers; it does not
         * force them out of the DataNodes' OS page cache, and neither does a
         * default close(). Without hsync there is a window after the MQ commit
         * in which correlated power loss across the replica set loses
         * acknowledged data. The legacy MDB called hsync() per record and had
         * no such window — one hsync per batch closes it at a fraction of that
         * cost.
         *
         * <p>Set false only for a feed where that narrow window is an
         * acceptable trade for one less DataNode disk round trip per batch.
         */
        private boolean hsyncOnFlush = true;

        public String getBasePath() {
            return basePath;
        }

        public void setBasePath(String basePath) {
            this.basePath = basePath;
        }

        public boolean isRecordIndexEnabled() {
            return recordIndexEnabled;
        }

        public void setRecordIndexEnabled(boolean recordIndexEnabled) {
            this.recordIndexEnabled = recordIndexEnabled;
        }

        public boolean isHsyncOnFlush() {
            return hsyncOnFlush;
        }

        public void setHsyncOnFlush(boolean hsyncOnFlush) {
            this.hsyncOnFlush = hsyncOnFlush;
        }
    }

    /** Tracker notifications (TRACKED mode only). */
    public static class Tracker {

        private String queue;
        private TrackerBodyMode bodyMode = TrackerBodyMode.FULL_COPY;
        private TrackerFields fields;

        /**
         * Whether a per-message tracker CONTENT failure should fail the batch.
         *
         * <p><strong>This is no longer the only control over whether a tracker
         * failure rolls back.</strong> The loop splits the two failure kinds:
         * a {@code JMSException} from the build or the put — tracker queue
         * full, message too big for it, producer broken — always fails the
         * batch regardless of this flag, because it will refuse the next
         * message too and the alternative is landing every message with its
         * acknowledgement silently dropped. This flag governs only the
         * remaining case: a {@code RuntimeException} out of the builder, which
         * means one message's own content broke the header rewrite.
         *
         * <p>Default false for that case, matching the legacy MDB: it catches
         * and logs tracker exceptions in both
         * {@code HDFSIngest.forwardToMessageTracker} and {@code EJBHelper}, so
         * a tracker failure never rolls back the message. The landed data is
         * kept and that one tracker notification is lost.
         *
         * <p>Setting it true is the stricter reading of §2.2 — tracker and get
         * in one unit of work — but think it through first: a content failure
         * classifies as UNKNOWN, which never triggers degraded mode, so there
         * is no bisection to isolate the offending message. The batch rolls
         * back at full size until delivery count carries the WHOLE batch past
         * BOTHRESH and onto the backout queue. One malformed header then costs
         * a thousand healthy messages a manual replay.
         *
         * <p>Either way the outcome is counted: {@code tracker_sent_total},
         * {@code tracker_suppressed_total} and {@code tracker_failures_total}.
         */
        private boolean failBatchOnError = false;

        public String getQueue() {
            return queue;
        }

        public void setQueue(String queue) {
            this.queue = queue;
        }

        public TrackerBodyMode getBodyMode() {
            return bodyMode;
        }

        public void setBodyMode(TrackerBodyMode bodyMode) {
            this.bodyMode = bodyMode;
        }

        public TrackerFields getFields() {
            return fields;
        }

        public void setFields(TrackerFields fields) {
            this.fields = fields;
        }

        public boolean isFailBatchOnError() {
            return failBatchOnError;
        }

        public void setFailBatchOnError(boolean failBatchOnError) {
            this.failBatchOnError = failBatchOnError;
        }
    }

    /** Poison message handling (§6.1). */
    public static class Backout {

        private String queue;
        private int threshold = 5;

        /**
         * How often to sample the backout queue's depth, in milliseconds.
         *
         * <p>Feeds the gauge DESIGN §14 nominates as the pager condition.
         * Sampling browses the queue, which is cheap while it is empty — the
         * state we expect — and capped when it is not. 0 or less disables the
         * monitor, and with it the backout-depth alert for this binding.
         */
        private long depthPollIntervalMs = 30_000;

        /**
         * Whether backout routing is suppressed while failures are
         * infrastructure-classified.
         *
         * <p>Off by default: routing on delivery count alone is the legacy
         * MDB's behaviour. Turn it on where diverting healthy messages is
         * worse than isolating a poison message one cycle later.
         *
         * <p>Delivery count cannot distinguish a malformed message from a
         * good one that sat in several batches which rolled back for an
         * unrelated reason. With a large batch and a backout queue sized for
         * poison rather than for whole batches, a landing-path outage lasting
         * a few retry cycles can divert thousands of healthy messages — safe,
         * but requiring manual replay, and capable of filling the queue.
         *
         * <p>With this on, a genuine poison message still reaches the backout
         * queue: its own failure classifies as message data (or UNKNOWN, which
         * also permits routing), which opens the gate on the next redelivery.
         */
        private boolean routeOnlyOnDataFailures = false;

        public boolean isRouteOnlyOnDataFailures() {
            return routeOnlyOnDataFailures;
        }

        public void setRouteOnlyOnDataFailures(boolean routeOnlyOnDataFailures) {
            this.routeOnlyOnDataFailures = routeOnlyOnDataFailures;
        }

        public String getQueue() {
            return queue;
        }

        public void setQueue(String queue) {
            this.queue = queue;
        }

        public int getThreshold() {
            return threshold;
        }

        public void setThreshold(int threshold) {
            this.threshold = threshold;
        }

        public long getDepthPollIntervalMs() {
            return depthPollIntervalMs;
        }

        public void setDepthPollIntervalMs(long depthPollIntervalMs) {
            this.depthPollIntervalMs = depthPollIntervalMs;
        }
    }

    /** The ABC audit control. */
    public static class Audit {

        /**
         * Whether the transaction-time ABC balance check runs before every
         * MQ commit: {@code mqConsumed == hdfsWritten + backout}, with each
         * side independently observed (batch size from the receive loop,
         * written count from the SequenceFile writer's per-append counter,
         * backout count from the poison screen's routing result).
         *
         * <p>Off by default; enabled per binding where an unbalanced batch
         * must never commit (RMS). When the check fails, the batch rolls back
         * and MQ redelivers — the same at-least-once path as any other
         * pre-commit failure. This is the transaction-time half of ABC; the
         * post-write reconciliation pass is the other half and is unaffected.
         */
        private boolean balanceCheckEnabled = false;

        /**
         * Whether a batch must roll back when its audit record cannot be
         * written.
         *
         * <p>Default true, because the audit record is a <em>control</em>
         * under ABC, not a diagnostic. Committing without one produces data
         * that no balance can account for: the messages are consumed and gone
         * from the queue, the file exists, and nothing records that it should.
         * A control that can be skipped when the audit store is unavailable is
         * not a control.
         *
         * <p>Rolling back instead means the messages stay on the queue and are
         * redelivered, so nothing is lost — ingestion stalls until the audit
         * path recovers. That is the correct trade for a feed where
         * completeness matters more than latency.
         *
         * <p>Set false only for a feed where an unaudited landing is
         * preferable to a stall.
         */
        private boolean failBatchOnError = true;

        public boolean isBalanceCheckEnabled() {
            return balanceCheckEnabled;
        }

        public void setBalanceCheckEnabled(boolean balanceCheckEnabled) {
            this.balanceCheckEnabled = balanceCheckEnabled;
        }

        public boolean isFailBatchOnError() {
            return failBatchOnError;
        }

        public void setFailBatchOnError(boolean failBatchOnError) {
            this.failBatchOnError = failBatchOnError;
        }
    }

    /** Degraded batch mode (§6.1). */
    public static class Degradation {

        private DegradationStrategy strategy = DegradationStrategy.BATCH_OF_ONE;
        private int successesRequiredToRestore = 10;

        public DegradationStrategy getStrategy() {
            return strategy;
        }

        public void setStrategy(DegradationStrategy strategy) {
            this.strategy = strategy;
        }

        public int getSuccessesRequiredToRestore() {
            return successesRequiredToRestore;
        }

        public void setSuccessesRequiredToRestore(int successesRequiredToRestore) {
            this.successesRequiredToRestore = successesRequiredToRestore;
        }
    }

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

    public int getListenerThreads() {
        return listenerThreads;
    }

    public void setListenerThreads(int listenerThreads) {
        this.listenerThreads = listenerThreads;
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Hdfs getHdfs() {
        return hdfs;
    }

    public void setHdfs(Hdfs hdfs) {
        this.hdfs = hdfs;
    }

    public Tracker getTracker() {
        return tracker;
    }

    public void setTracker(Tracker tracker) {
        this.tracker = tracker;
    }

    public Backout getBackout() {
        return backout;
    }

    public void setBackout(Backout backout) {
        this.backout = backout;
    }

    public Audit getAudit() {
        return audit;
    }

    public void setAudit(Audit audit) {
        this.audit = audit;
    }

    public Degradation getDegradation() {
        return degradation;
    }

    public void setDegradation(Degradation degradation) {
        this.degradation = degradation;
    }

    public boolean isAcceptPlaceholderSerializer() {
        return acceptPlaceholderSerializer;
    }

    public void setAcceptPlaceholderSerializer(boolean acceptPlaceholderSerializer) {
        this.acceptPlaceholderSerializer = acceptPlaceholderSerializer;
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
     * Returns the memory footprint of this binding: batch bytes * listener threads.
     */
    public long getMemoryFootprint() {
        return batch.getBytes() * listenerThreads;
    }
}
