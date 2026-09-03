package com.hcsc.datalake.mqintake.core.batch;

import javax.jms.Message;
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
    /**
     * Remaining writes to fail before succeeding again. Bounded on purpose:
     * an always-failing writer plus an always-available queue is a rollback
     * storm the embedded broker does not survive reliably, which made tests
     * that used one flaky for reasons that had nothing to do with the code
     * under test.
     */
    private final AtomicInteger failuresRemaining = new AtomicInteger(0);
    private volatile String failureMessage = "Simulated write failure";
    private volatile Throwable failureCause = null;

    /**
     * Messages per successful write, oldest first. Lets a test assert how the
     * loop divided a stream into batches, which the aggregate counters hide.
     */
    private final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());

    @Override
    public BatchWriteResult write(String bindingId, List<Message> messages)
            throws BatchWriteException {
        if (shouldFail || failuresRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            if (failureCause != null) {
                throw new BatchWriteException(failureMessage, failureCause);
            }
            throw new BatchWriteException(failureMessage);
        }

        int count = messages.size();
        batchCount.incrementAndGet();
        totalMessageCount.addAndGet(count);
        batchSizes.add(count);

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
        failuresRemaining.set(0);
        batchSizes.clear();
    }

    /** Fails the next {@code count} writes, then succeeds. */
    public void failNextWrites(int count, String message) {
        this.failureMessage = message;
        this.failureCause = null;
        this.failuresRemaining.set(count);
    }

    /** Messages per write, oldest first. */
    public List<Integer> getBatchSizes() {
        synchronized (batchSizes) {
            return List.copyOf(batchSizes);
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
