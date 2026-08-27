package com.hcsc.datalake.mqintake.core.failuremode;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Receive loop with fault injection for failure-mode testing.
 *
 * <p>This is the real transacted receive loop with fault injection hooks
 * at critical boundaries. It uses real JMS transactions and real HDFS
 * writes — no mocking of transaction semantics.
 *
 * <p>Key injection points per §12.1:
 * <ul>
 *   <li>After tracker puts, before commit (Test 5)</li>
 *   <li>After commit, before audit (Test 4)</li>
 * </ul>
 */
public class FaultableReceiveLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FaultableReceiveLoop.class);

    private final BindingConfig config;
    private final Connection connection;
    private final BatchWriter batchWriter;
    private final TrackerMessageBuilder trackerMessageBuilder;
    private final FaultInjector faultInjector;
    private final long receiveTimeoutMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong trackerMessagesSent = new AtomicLong(0);

    private volatile Session session;
    private volatile MessageConsumer consumer;
    private volatile MessageProducer trackerProducer;
    private volatile Thread loopThread;
    private volatile Exception lastException;

    private Consumer<BatchWriter.BatchWriteResult> auditCallback;

    public FaultableReceiveLoop(BindingConfig config,
                                 Connection connection,
                                 BatchWriter batchWriter,
                                 TrackerMessageBuilder trackerMessageBuilder,
                                 FaultInjector faultInjector,
                                 long receiveTimeoutMs) {
        this.config = config;
        this.connection = connection;
        this.batchWriter = batchWriter;
        this.trackerMessageBuilder = trackerMessageBuilder;
        this.faultInjector = faultInjector;
        this.receiveTimeoutMs = receiveTimeoutMs;

        if (config.getMode() == BindingMode.TRACKED && trackerMessageBuilder == null) {
            throw new IllegalArgumentException(
                    "TRACKED binding '" + config.getId() + "' requires a TrackerMessageBuilder");
        }
    }

    public void setAuditCallback(Consumer<BatchWriter.BatchWriteResult> callback) {
        this.auditCallback = callback;
    }

    @Override
    public void run() {
        loopThread = Thread.currentThread();
        String threadName = "faultable-recv-" + config.getId();
        Thread.currentThread().setName(threadName);

        log.info("Starting faultable receive loop for binding '{}'", config.getId());

        try {
            initializeSession();
            running.set(true);
            runLoop();
        } catch (JMSException e) {
            log.error("Failed to initialize session for binding '{}': {}",
                    config.getId(), e.getMessage(), e);
            lastException = e;
        } finally {
            cleanup();
            log.info("Faultable receive loop stopped. Commits: {}, Rollbacks: {}, Messages: {}",
                    commitCount.get(), rollbackCount.get(), messageCount.get());
        }
    }

    private void initializeSession() throws JMSException {
        session = connection.createSession(true, Session.SESSION_TRANSACTED);

        Queue sourceQueue = session.createQueue(config.getSourceQueue());
        consumer = session.createConsumer(sourceQueue);

        if (config.getMode() == BindingMode.TRACKED) {
            Queue trackerQueue = session.createQueue(config.getTracker().getQueue());
            trackerProducer = session.createProducer(trackerQueue);
            log.debug("Created tracker producer for queue '{}'", config.getTracker().getQueue());
        }

        log.info("Initialized session for binding '{}': source='{}', mode={}",
                config.getId(), config.getSourceQueue(), config.getMode());
    }

    private void runLoop() {
        int batchSize = config.getBatch().getSize();
        List<Message> batch = new ArrayList<>(batchSize);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Message message = consumer.receive(receiveTimeoutMs);

                if (message != null) {
                    batch.add(message);

                    if (batch.size() >= batchSize) {
                        processBatch(batch);
                        batch.clear();
                    }
                } else {
                    if (!batch.isEmpty()) {
                        processBatch(batch);
                        batch.clear();
                    }
                }
            } catch (JMSException e) {
                if (running.get()) {
                    log.error("JMS error in receive loop: {}", e.getMessage(), e);
                    rollbackQuietly();
                    batch.clear();
                    lastException = e;
                }
            } catch (BatchWriter.BatchWriteException e) {
                log.error("Batch write failed, rolling back: {}", e.getMessage());
                rollbackQuietly();
                batch.clear();
                lastException = e;
            } catch (FaultInjector.FaultException e) {
                log.warn("Fault injected, rolling back: {}", e.getMessage());
                rollbackQuietly();
                batch.clear();
                lastException = e;
                if (e.isSimulateJvmCrash()) {
                    running.set(false);
                }
            }
        }

        if (!batch.isEmpty()) {
            log.info("Draining {} messages on shutdown", batch.size());
            try {
                processBatch(batch);
            } catch (Exception e) {
                log.warn("Failed to drain batch on shutdown: {}", e.getMessage());
                rollbackQuietly();
            }
        }
    }

    private void processBatch(List<Message> batch)
            throws BatchWriter.BatchWriteException, JMSException, FaultInjector.FaultException {

        int batchSize = batch.size();
        log.debug("Processing batch of {} messages", batchSize);

        BatchWriter.BatchWriteResult writeResult = batchWriter.write(config.getId(), batch);

        if (config.getMode() == BindingMode.TRACKED) {
            sendTrackerMessages(batch);
            faultInjector.afterTrackerPuts();
        }

        session.commit();
        commitCount.incrementAndGet();
        messageCount.addAndGet(batchSize);

        try {
            faultInjector.afterMqCommit();
        } catch (FaultInjector.FaultException e) {
            log.warn("Fault injected after commit (commit succeeded): {}", e.getMessage());
            throw e;
        }

        if (auditCallback != null) {
            auditCallback.accept(writeResult);
        }

        log.debug("Committed batch of {} messages", batchSize);
    }

    private void sendTrackerMessages(List<Message> batch) throws JMSException {
        for (Message sourceMessage : batch) {
            Optional<Message> trackerMessage = trackerMessageBuilder.build(session, sourceMessage);
            if (trackerMessage.isPresent()) {
                trackerProducer.send(trackerMessage.get());
                trackerMessagesSent.incrementAndGet();
            }
        }
    }

    private void rollbackQuietly() {
        try {
            if (session != null) {
                session.rollback();
                rollbackCount.incrementAndGet();
            }
        } catch (JMSException e) {
            log.warn("Failed to rollback session: {}", e.getMessage());
        }
    }

    public void stop() {
        log.info("Stopping faultable receive loop");
        running.set(false);
        if (loopThread != null) {
            loopThread.interrupt();
        }
    }

    private void cleanup() {
        running.set(false);

        try {
            if (trackerProducer != null) trackerProducer.close();
        } catch (JMSException e) {
            log.debug("Error closing tracker producer: {}", e.getMessage());
        }

        try {
            if (consumer != null) consumer.close();
        } catch (JMSException e) {
            log.debug("Error closing consumer: {}", e.getMessage());
        }

        try {
            if (session != null) session.close();
        } catch (JMSException e) {
            log.debug("Error closing session: {}", e.getMessage());
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getCommitCount() {
        return commitCount.get();
    }

    public long getRollbackCount() {
        return rollbackCount.get();
    }

    public long getMessageCount() {
        return messageCount.get();
    }

    public long getTrackerMessagesSent() {
        return trackerMessagesSent.get();
    }

    public Exception getLastException() {
        return lastException;
    }

    public Session getSession() {
        return session;
    }
}
