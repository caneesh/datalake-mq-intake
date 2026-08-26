package com.hcsc.datalake.mqintake.core.audit;

import java.time.Instant;
import java.util.Objects;

/**
 * Audit record emitted after each successful commit.
 *
 * <p>From DESIGN.md §12: immediately after a successful commit, the writer emits
 * an audit record containing binding_id, partition path, filename, record count,
 * byte count, first/last identity value, instance ID, commit timestamp.
 *
 * <p>This record is immutable and contains all information needed for reconciliation.
 */
public final class AuditRecord {

    private final String bindingId;
    private final String partitionPath;
    private final String filename;
    private final int recordCount;
    private final long byteCount;
    private final String firstIdentity;
    private final String lastIdentity;

    /**
     * Messages consumed in this unit of work that were routed to the backout
     * queue instead of being landed.
     *
     * <p>Without it the balance equation does not close: a batch that consumed
     * 10 and landed 9 looks like a loss of 1, when the tenth was deliberately
     * set aside. ABC balance is
     * {@code consumed == recordCount + backoutCount}.
     */
    private final int backoutCount;
    private final String instanceId;
    private final Instant commitTimestamp;

    private AuditRecord(Builder builder) {
        this.bindingId = Objects.requireNonNull(builder.bindingId, "bindingId required");
        this.partitionPath = Objects.requireNonNull(builder.partitionPath, "partitionPath required");
        this.filename = Objects.requireNonNull(builder.filename, "filename required");
        this.recordCount = builder.recordCount;
        this.byteCount = builder.byteCount;
        this.firstIdentity = builder.firstIdentity;
        this.lastIdentity = builder.lastIdentity;
        this.backoutCount = builder.backoutCount;
        this.instanceId = Objects.requireNonNull(builder.instanceId, "instanceId required");
        this.commitTimestamp = Objects.requireNonNull(builder.commitTimestamp, "commitTimestamp required");

        if (recordCount < 0) {
            throw new IllegalArgumentException("recordCount must be non-negative");
        }
        if (backoutCount < 0) {
            throw new IllegalArgumentException("backoutCount must be non-negative");
        }
        // A record must account for something. recordCount alone used to have
        // to be positive, which was right while every record described a
        // landed file — but a unit of work whose messages were all poison
        // lands nothing and still consumed messages, and it needs a record or
        // those messages appear in no audit at all.
        if (recordCount + backoutCount <= 0) {
            throw new IllegalArgumentException(
                    "An audit record must account for at least one message: recordCount "
                            + recordCount + " + backoutCount " + backoutCount + " is zero");
        }
        if (byteCount < 0) {
            throw new IllegalArgumentException("byteCount must be non-negative");
        }
    }

    public String getBindingId() {
        return bindingId;
    }

    public String getPartitionPath() {
        return partitionPath;
    }

    public String getFilename() {
        return filename;
    }

    /**
     * Full file path (partitionPath + filename).
     */
    public String getFilePath() {
        return partitionPath + "/" + filename;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public long getByteCount() {
        return byteCount;
    }

    /**
     * First identity value in the batch (payload_guid or mq_message_id fallback).
     * May be null if no identity could be extracted.
     */
    public String getFirstIdentity() {
        return firstIdentity;
    }

    /**
     * Last identity value in the batch (payload_guid or mq_message_id fallback).
     * May be null if no identity could be extracted.
     */
    public String getLastIdentity() {
        return lastIdentity;
    }

    /** Messages routed to the backout queue in this unit of work. */
    public int getBackoutCount() {
        return backoutCount;
    }

    /**
     * Messages consumed from the queue in this unit of work.
     *
     * <p>The left-hand side of the balance equation: everything taken off the
     * source queue was either landed or set aside, and this is the total the
     * control compares against.
     */
    public int getConsumedCount() {
        return recordCount + backoutCount;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public Instant getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuditRecord that = (AuditRecord) o;
        return recordCount == that.recordCount &&
                byteCount == that.byteCount &&
                bindingId.equals(that.bindingId) &&
                partitionPath.equals(that.partitionPath) &&
                filename.equals(that.filename) &&
                Objects.equals(firstIdentity, that.firstIdentity) &&
                Objects.equals(lastIdentity, that.lastIdentity) &&
                backoutCount == that.backoutCount &&
                instanceId.equals(that.instanceId) &&
                commitTimestamp.equals(that.commitTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bindingId, partitionPath, filename, recordCount,
                byteCount, firstIdentity, lastIdentity, instanceId, commitTimestamp,
                backoutCount);
    }

    @Override
    public String toString() {
        return "AuditRecord{" +
                "bindingId='" + bindingId + '\'' +
                ", partitionPath='" + partitionPath + '\'' +
                ", filename='" + filename + '\'' +
                ", recordCount=" + recordCount +
                ", byteCount=" + byteCount +
                ", firstIdentity='" + firstIdentity + '\'' +
                ", lastIdentity='" + lastIdentity + '\'' +
                ", backoutCount=" + backoutCount +
                ", instanceId='" + instanceId + '\'' +
                ", commitTimestamp=" + commitTimestamp +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String bindingId;
        private String partitionPath;
        private String filename;
        private int recordCount;
        private long byteCount;
        private String firstIdentity;
        private String lastIdentity;
        private int backoutCount;
        private String instanceId;
        private Instant commitTimestamp;

        public Builder bindingId(String bindingId) {
            this.bindingId = bindingId;
            return this;
        }

        public Builder partitionPath(String partitionPath) {
            this.partitionPath = partitionPath;
            return this;
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder recordCount(int recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public Builder byteCount(long byteCount) {
            this.byteCount = byteCount;
            return this;
        }

        public Builder firstIdentity(String firstIdentity) {
            this.firstIdentity = firstIdentity;
            return this;
        }

        public Builder backoutCount(int backoutCount) {
            this.backoutCount = backoutCount;
            return this;
        }

        public Builder lastIdentity(String lastIdentity) {
            this.lastIdentity = lastIdentity;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder commitTimestamp(Instant commitTimestamp) {
            this.commitTimestamp = commitTimestamp;
            return this;
        }

        public AuditRecord build() {
            return new AuditRecord(this);
        }
    }
}
