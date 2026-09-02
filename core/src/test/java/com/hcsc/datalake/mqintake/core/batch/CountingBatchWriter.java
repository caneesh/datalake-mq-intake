package com.hcsc.datalake.mqintake.core.batch;

import javax.jms.Message;
import java.time.Instant;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stub BatchWriter that counts messages without writing to HDFS.
 * Used for testing the transacted receive loop in isolation.
 */
public class CountingBatchWriter implements BatchWriter {

    private final AtomicInteger batchCount = new AtomicInteger(0);
    private final AtomicLong totalMessageCount = new AtomicLong(0);
    private volatile boolean shouldFail = false;
    private volatile String failureMessage = "Simulated write failure";
    private volatile Throwable failureCause = null;

    /**
     * One entry per successful write: the partition instant the loop supplied
     * and how many messages the batch held. Lets a test assert which window a
     * batch was stamped with, which is otherwise invisible from outside.
     */
    private final List<Written> written = Collections.synchronizedList(new ArrayList<>());

    @Override
    public BatchWriteResult write(String bindingId, List<Message> messages,
                                 java.time.Instant partitionInstant) throws BatchWriteException {
        if (shouldFail) {
            if (failureCause != null) {
                throw new BatchWriteException(failureMessage, failureCause);
            }
            throw new BatchWriteException(failureMessage);
        }

        int count = messages.size();
        batchCount.incrementAndGet();
        totalMessageCount.addAndGet(count);
        written.add(new Written(partitionInstant, count));

        return new BatchWriteResult(
                "/data/raw/" + bindingId + "/test-file-" + batchCount.get() + ".seq",
                count,
                count * 100L // Simulated byte count
        );
    }

    public int getBatchCount() {
        return batchCount.get();
    }

    public long getTotalMessageCount() {
        return totalMessageCount.get();
    }

    public void reset() {
        batchCount.set(0);
        totalMessageCount.set(0);
        shouldFail = false;
        written.clear();
    }

    /** The writes so far, oldest first. */
    public List<Written> getWritten() {
        synchronized (written) {
            return List.copyOf(written);
        }
    }

    /** What one write was asked to do. */
    public static class Written {
        private final Instant partitionInstant;
        private final int messageCount;

        Written(Instant partitionInstant, int messageCount) {
            this.partitionInstant = partitionInstant;
            this.messageCount = messageCount;
        }

        public Instant getPartitionInstant() {
            return partitionInstant;
        }

        public int getMessageCount() {
            return messageCount;
        }
    }

    public void setFailOnNextWrite(boolean fail) {
        this.shouldFail = fail;
    }

    public void setFailOnNextWrite(boolean fail, String message) {
        this.shouldFail = fail;
        this.failureMessage = message;
        this.failureCause = null;
    }

    public void setFailOnNextWrite(boolean fail, Throwable cause) {
        this.shouldFail = fail;
        this.failureMessage = cause.getMessage();
        this.failureCause = cause;
    }
}
