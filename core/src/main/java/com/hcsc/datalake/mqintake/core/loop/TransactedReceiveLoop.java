package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hand-rolled transacted receive loop for MQ message consumption.
 *
 * <p>This is the core of the system. Key invariants:
 * <ul>
 *   <li>One transacted Session per listener thread (no sharing)</li>
 *   <li>MessageConsumer and MessageProducer created from the same Session</li>
 *   <li>Bounded receive(timeout) for predictable shutdown interruption</li>
 *   <li>Any failure before commit rolls back the entire batch</li>
 *   <li>No partial commit, no per-message acknowledgement</li>
 * </ul>
 *
 * <p>DO NOT use @JmsListener, DefaultMessageListenerContainer, or JmsTemplate here.
 * They impose per-message transaction boundaries which defeats the batching design.
 */
public class TransactedReceiveLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TransactedReceiveLoop.class);

    private final BindingConfig config;
    private final Connection connection;
    private final BatchWriter batchWriter;
    private final TrackerMessageBuilder trackerMessageBuilder;
    private final PoisonMessageHandler poisonMessageHandler;
    private final DegradedModeManager degradedModeManager;
    private final AuditRecordEmitter auditRecordEmitter;
    private final BindingMetrics metrics;
    private final String instanceId;
    private final long receiveTimeoutMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);

    private volatile Session session;
    private volatile MessageConsumer consumer;
    private volatile MessageProducer trackerProducer;
    private volatile Thread loopThread;

    /**
     * Creates a receive loop for the given binding.
     *
     * @param config                the binding configuration
     * @param connection            shared JMS connection (thread-safe)
     * @param batchWriter           writer for HDFS batches
     * @param trackerMessageBuilder builder for tracker messages (null for LAND_ONLY)
     * @param poisonMessageHandler  handler for poison messages (null to disable)
     * @param degradedModeManager   manager for degraded batch mode (null to disable)
     * @param auditRecordEmitter    emitter for audit records (null to disable)
     * @param metrics               binding metrics (null to disable)
     * @param instanceId            instance identifier for audit records
     * @param receiveTimeoutMs      timeout for receive() calls
     */
    public TransactedReceiveLoop(BindingConfig config,
                                  Connection connection,
                                  BatchWriter batchWriter,
                                  TrackerMessageBuilder trackerMessageBuilder,
                                  PoisonMessageHandler poisonMessageHandler,
                                  DegradedModeManager degradedModeManager,
                                  AuditRecordEmitter auditRecordEmitter,
                                  BindingMetrics metrics,
                                  String instanceId,
                                  long receiveTimeoutMs) {
        this.config = config;
        this.connection = connection;
        this.batchWriter = batchWriter;
        this.trackerMessageBuilder = trackerMessageBuilder;
        this.poisonMessageHandler = poisonMessageHandler;
        this.degradedModeManager = degradedModeManager;
        this.auditRecordEmitter = auditRecordEmitter;
        this.metrics = metrics;
        this.instanceId = instanceId;
        this.receiveTimeoutMs = receiveTimeoutMs;

        if (config.getMode() == BindingMode.TRACKED && trackerMessageBuilder == null) {
            throw new IllegalArgumentException(
                    "TRACKED binding '" + config.getId() + "' requires a TrackerMessageBuilder");
        }
    }

    @Override
    public void run() {
        loopThread = Thread.currentThread();
        String threadName = "recv-" + config.getId() + "-" + Thread.currentThread().getId();
        Thread.currentThread().setName(threadName);

        log.info("Starting receive loop for binding '{}' on thread {}",
                config.getId(), threadName);

        try {
            initializeSession();
            running.set(true);
            runLoop();
        } catch (JMSException e) {
            log.error("Failed to initialize session for binding '{}': {}",
                    config.getId(), e.getMessage(), e);
        } finally {
            cleanup();
            log.info("Receive loop stopped for binding '{}'. Commits: {}, Rollbacks: {}, Messages: {}",
                    config.getId(), commitCount.get(), rollbackCount.get(), messageCount.get());
        }
    }

    private void initializeSession() throws JMSException {
        session = connection.createSession(true, Session.SESSION_TRANSACTED);

        Queue sourceQueue = session.createQueue(config.getSourceQueue());
        consumer = session.createConsumer(sourceQueue);

        if (config.getMode() == BindingMode.TRACKED) {
            Queue trackerQueue = session.createQueue(config.getTrackerQueue());
            trackerProducer = session.createProducer(trackerQueue);
            log.debug("Created tracker producer for binding '{}' on queue '{}'",
                    config.getId(), config.getTrackerQueue());
        }

        log.info("Initialized session for binding '{}': source='{}', mode={}",
                config.getId(), config.getSourceQueue(), config.getMode());
    }

    private void runLoop() {
        FlushTrigger flushTrigger = new FlushTrigger(
                config.getBatchSize(),
                config.getBatchBytes(),
                config.getBatchIntervalMs()
        );

        List<Message> batch = new ArrayList<>(config.getBatchSize());

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                int effectiveBatchSize = getEffectiveBatchSize();
                Message message = consumer.receive(receiveTimeoutMs);

                if (message != null) {
                    batch.add(message);
                    flushTrigger.trackMessage(message);

                    if (batch.size() >= effectiveBatchSize || flushTrigger.shouldFlush()) {
                        processBatch(batch);
                        batch.clear();
                        flushTrigger.reset();
                    }
                } else {
                    if (!batch.isEmpty() && flushTrigger.isTimeoutExpired()) {
                        processBatch(batch);
                        batch.clear();
                        flushTrigger.reset();
                    }
                }
            } catch (JMSException e) {
                if (running.get()) {
                    log.error("JMS error in receive loop for binding '{}': {}",
                            config.getId(), e.getMessage(), e);
                    handleFailure(e);
                    rollbackQuietly();
                    batch.clear();
                    flushTrigger.reset();
                }
            }
        }

        if (!batch.isEmpty()) {
            log.info("Draining {} messages on shutdown for binding '{}'",
                    batch.size(), config.getId());
            try {
                processBatch(batch);
            } catch (Exception e) {
                log.warn("Failed to drain batch on shutdown for binding '{}': {}",
                        config.getId(), e.getMessage());
                rollbackQuietly();
            }
        }
    }

    private int getEffectiveBatchSize() {
        if (degradedModeManager != null) {
            return degradedModeManager.getCurrentBatchSize();
        }
        return config.getBatchSize();
    }

    private void processBatch(List<Message> batch) {
        int batchSize = batch.size();
        log.debug("Processing batch of {} messages for binding '{}'",
                batchSize, config.getId());

        List<Message> cleanMessages = batch;
        try {
            if (poisonMessageHandler != null) {
                PoisonMessageHandler.BatchPoisonCheckResult poisonResult =
                        poisonMessageHandler.checkAndRoutePoisonMessages(session, batch);

                if (poisonResult.hasPoisonMessages()) {
                    log.warn("Routed {} poison messages to backout queue for binding '{}'",
                            poisonResult.getPoisonCount(), config.getId());
                    if (metrics != null) {
                        for (int i = 0; i < poisonResult.getPoisonCount(); i++) {
                            metrics.recordPoisonMessageRouted();
                        }
                    }
                }

                cleanMessages = poisonResult.getCleanMessages();
                if (cleanMessages.isEmpty()) {
                    session.commit();
                    commitCount.incrementAndGet();
                    if (degradedModeManager != null) {
                        degradedModeManager.recordSuccess();
                    }
                    return;
                }
            }

            BatchWriter.BatchWriteResult writeResult = batchWriter.write(config.getId(), cleanMessages);

            if (config.getMode() == BindingMode.TRACKED) {
                sendTrackerMessages(cleanMessages);
            }

            session.commit();
            commitCount.incrementAndGet();
            messageCount.addAndGet(cleanMessages.size());

            if (degradedModeManager != null) {
                degradedModeManager.recordSuccess();
            }

            if (metrics != null) {
                metrics.recordCommit();
                metrics.recordMessagesWritten(cleanMessages.size(), writeResult.getByteCount());
            }

            emitAuditRecord(cleanMessages, writeResult);

            log.debug("Committed batch of {} messages for binding '{}'",
                    cleanMessages.size(), config.getId());

        } catch (Exception e) {
            log.error("Batch processing failed for binding '{}', rolling back {} messages: {}",
                    config.getId(), batchSize, e.getMessage(), e);
            handleFailure(e);
            rollbackQuietly();

            if (metrics != null) {
                metrics.recordRollback();
            }
        }
    }

    private void handleFailure(Throwable e) {
        if (degradedModeManager != null) {
            FailureClass failureClass = degradedModeManager.recordFailure(e);
            log.debug("Failure classified as {} for binding '{}'", failureClass, config.getId());
        }
    }

    private void emitAuditRecord(List<Message> messages, BatchWriter.BatchWriteResult writeResult) {
        if (auditRecordEmitter == null) {
            return;
        }

        try {
            auditRecordEmitter.emit(config.getId(), writeResult, messages);
        } catch (Exception e) {
            log.warn("Failed to emit audit record for binding '{}': {}",
                    config.getId(), e.getMessage());
        }
    }

    private void sendTrackerMessages(List<Message> batch) throws JMSException {
        for (Message sourceMessage : batch) {
            Optional<Message> trackerMessage = trackerMessageBuilder.build(session, sourceMessage);
            if (trackerMessage.isPresent()) {
                trackerProducer.send(trackerMessage.get());
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
            log.warn("Failed to rollback session for binding '{}': {}",
                    config.getId(), e.getMessage());
        }
    }

    public void stop() {
        log.info("Stopping receive loop for binding '{}'", config.getId());
        running.set(false);

        if (loopThread != null) {
            loopThread.interrupt();
        }
    }

    public Session getSession() {
        return session;
    }

    private void cleanup() {
        running.set(false);

        try {
            if (trackerProducer != null) {
                trackerProducer.close();
            }
        } catch (JMSException e) {
            log.debug("Error closing tracker producer: {}", e.getMessage());
        }

        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (JMSException e) {
            log.debug("Error closing consumer: {}", e.getMessage());
        }

        try {
            if (session != null) {
                session.close();
            }
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

    public String getBindingId() {
        return config.getId();
    }
}
