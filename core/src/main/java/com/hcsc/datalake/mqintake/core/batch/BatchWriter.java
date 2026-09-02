package com.hcsc.datalake.mqintake.core.batch;

import javax.jms.Message;
import java.time.Instant;
import java.util.List;

/**
 * Interface for writing batches of messages to HDFS.
 * Implementations handle sequence file writing, _tmp staging, and atomic rename.
 */
public interface BatchWriter {

    /**
     * Writes a batch of messages. Must complete all writes before returning.
     * On success, the data is durably visible in the target partition.
     * On failure, throws an exception and the caller must roll back the JMS transaction.
     *
     * <p><strong>The partition comes from {@code partitionInstant}, not from
     * the implementation's own clock.</strong> A batch is bounded to one
     * partition window, and the partition trigger fires on the first poll
     * <em>after</em> that window closes — so at the moment of the write, "now"
     * is already the next window. An implementation that timestamps itself
     * files every partition-triggered batch one window late, which for a
     * low-volume feed is every batch. The caller supplies the window the
     * messages actually belong to; see
     * {@code FlushTrigger.getBatchAnchor()}.
     *
     * @param bindingId       the binding identifier
     * @param messages        the messages to write
     * @param partitionInstant an instant inside the partition window these
     *                        messages belong to — the batch's anchor, never
     *                        the flush time
     * @return result containing file path and record count
     * @throws BatchWriteException if the write fails
     */
    BatchWriteResult write(String bindingId, List<Message> messages, Instant partitionInstant)
            throws BatchWriteException;

    /**
     * Writes a batch into the partition current at this moment.
     *
     * <p>Convenience for callers with no batch to anchor to — retrospective
     * tooling and tests. The receive loop must NOT use it: it reintroduces the
     * one-window-late placement this parameter exists to prevent.
     */
    default BatchWriteResult write(String bindingId, List<Message> messages)
            throws BatchWriteException {
        return write(bindingId, messages, Instant.now());
    }

    /**
     * Result of a successful batch write.
     */
    class BatchWriteResult {
        private final String filePath;
        private final int recordCount;
        private final long byteCount;

        public BatchWriteResult(String filePath, int recordCount, long byteCount) {
            this.filePath = filePath;
            this.recordCount = recordCount;
            this.byteCount = byteCount;
        }

        public String getFilePath() {
            return filePath;
        }

        public int getRecordCount() {
            return recordCount;
        }

        public long getByteCount() {
            return byteCount;
        }
    }

    /**
     * Exception thrown when a batch write fails.
     */
    class BatchWriteException extends Exception {
        public BatchWriteException(String message) {
            super(message);
        }

        public BatchWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
