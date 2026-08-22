package com.hcsc.datalake.mqintake.core.batch;

import javax.jms.Message;
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
     * @param bindingId the binding identifier
     * @param messages  the messages to write
     * @return result containing file path and record count
     * @throws BatchWriteException if the write fails
     */
    BatchWriteResult write(String bindingId, List<Message> messages) throws BatchWriteException;

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
