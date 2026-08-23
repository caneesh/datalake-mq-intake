package com.hcsc.datalake.mqintake.core.serializer;

import java.time.Instant;

/**
 * Metadata captured for each record, passed to the RecordSerializer.
 *
 * <p>Contains transport-level identity and timing information that
 * the serializer may incorporate into the key or value.
 */
public class RecordMetadata {

    private final String bindingId;
    private final String mqMessageId;
    private final Instant mqPutTimestamp;
    private final Instant consumeTimestamp;
    private final String sourceFile;
    private final int recordOffset;
    private final long fileByteOffset;

    private RecordMetadata(Builder builder) {
        this.bindingId = builder.bindingId;
        this.mqMessageId = builder.mqMessageId;
        this.mqPutTimestamp = builder.mqPutTimestamp;
        this.consumeTimestamp = builder.consumeTimestamp;
        this.sourceFile = builder.sourceFile;
        this.recordOffset = builder.recordOffset;
        this.fileByteOffset = builder.fileByteOffset;
    }

    public String getBindingId() {
        return bindingId;
    }

    public String getMqMessageId() {
        return mqMessageId;
    }

    public Instant getMqPutTimestamp() {
        return mqPutTimestamp;
    }

    public Instant getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    /**
     * Byte position at which this record begins in the destination
     * SequenceFile — the writer's length immediately before the append.
     *
     * <p>This is the production key semantic: the legacy writer does
     * {@code long offset = sequenceFileWriter.getLength()} before each append
     * and uses it as the {@code LongWritable} key. It is distinct from
     * {@link #getRecordOffset()}, which is the record's index within its batch
     * and is traceability metadata rather than the key.
     *
     * <p>Only the component owning the writer can supply this, so it is set by
     * the batch writer rather than derived by the serializer.
     */
    public long getFileByteOffset() {
        return fileByteOffset;
    }

    public int getRecordOffset() {
        return recordOffset;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String bindingId;
        private String mqMessageId;
        private Instant mqPutTimestamp;
        private Instant consumeTimestamp;
        private String sourceFile;
        private int recordOffset;
        private long fileByteOffset;

        public Builder bindingId(String bindingId) {
            this.bindingId = bindingId;
            return this;
        }

        public Builder mqMessageId(String mqMessageId) {
            this.mqMessageId = mqMessageId;
            return this;
        }

        public Builder mqPutTimestamp(Instant mqPutTimestamp) {
            this.mqPutTimestamp = mqPutTimestamp;
            return this;
        }

        public Builder consumeTimestamp(Instant consumeTimestamp) {
            this.consumeTimestamp = consumeTimestamp;
            return this;
        }

        public Builder sourceFile(String sourceFile) {
            this.sourceFile = sourceFile;
            return this;
        }

        public Builder fileByteOffset(long fileByteOffset) {
            this.fileByteOffset = fileByteOffset;
            return this;
        }

        public Builder recordOffset(int recordOffset) {
            this.recordOffset = recordOffset;
            return this;
        }

        public RecordMetadata build() {
            return new RecordMetadata(this);
        }
    }
}
