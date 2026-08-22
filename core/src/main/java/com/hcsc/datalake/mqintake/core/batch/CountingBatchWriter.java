package com.hcsc.datalake.mqintake.core.batch;

import javax.jms.Message;
import java.util.List;
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

    @Override
    public BatchWriteResult write(String bindingId, List<Message> messages) throws BatchWriteException {
        if (shouldFail) {
            if (failureCause != null) {
                throw new BatchWriteException(failureMessage, failureCause);
            }
            throw new BatchWriteException(failureMessage);
        }

        int count = messages.size();
        batchCount.incrementAndGet();
        totalMessageCount.addAndGet(count);

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
