package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    // Session recovery backoff constants
    private static final long INITIAL_BACKOFF_MS = 1000;
    private static final long MAX_BACKOFF_MS = 60000;
    private static final int MAX_RECONNECT_ATTEMPTS = 10;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final double JITTER_FACTOR = 0.2;

    private final BindingConfig config;
    private final Connection connection;
    private final BatchWriter batchWriter;
    private final TrackerMessageBuilder trackerMessageBuilder;
    private final PoisonMessageHandler poisonMessageHandler;
    private final DegradedModeManager degradedModeManager;
    private final BindingHealthManager healthManager;
    private final AuditRecordEmitter auditRecordEmitter;
    private final BindingMetrics metrics;
    private final String instanceId;
    private final long receiveTimeoutMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicLong reconnectCount = new AtomicLong(0);

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
     * @param healthManager         health manager for binding health updates (null to disable)
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
                                  BindingHealthManager healthManager,
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
        this.healthManager = healthManager;
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

                    // Check if session needs recovery
                    if (isSessionBroken(e)) {
                        if (!recoverSession()) {
                            log.error("Session recovery failed for binding '{}', stopping loop",
                                    config.getId());
                            running.set(false);
                            break;
                        }
                    }
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

        // Collect IDs BEFORE any send: routing a message to the BOQ via
        // producer.send() assigns it a NEW JMSMessageID, so IDs read afterward
        // would no longer match the suspect entries recorded at failure time
        List<String> batchMessageIds =
                degradedModeManager != null ? collectMessageIds(batch) : List.of();

        List<Message> cleanMessages = batch;
        try {
            if (poisonMessageHandler != null) {
                PoisonMessageHandler.BatchPoisonCheckResult poisonResult;
                try {
                    poisonResult = poisonMessageHandler.checkAndRoutePoisonMessages(session, batch);
                } catch (PoisonMessageHandler.BackoutFailureException e) {
                    // CRITICAL: BOQ routing failed - we MUST rollback to avoid losing messages
                    log.error("Backout queue routing failed for binding '{}', rolling back: {}",
                            config.getId(), e.getMessage(), e);
                    throw e; // Re-throw to trigger rollback in outer catch
                }

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
                        degradedModeManager.clearSuspects(batchMessageIds);
                    }
                    handleSuccess();
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

            // Committed messages (including any routed to BOQ in this unit of
            // work) are no longer suspects
            if (degradedModeManager != null) {
                degradedModeManager.clearSuspects(batchMessageIds);
            }

            handleSuccess();

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
            handleFailure(e, batchMessageIds);
            rollbackQuietly();

            if (metrics != null) {
                metrics.recordRollback();
            }
        }
    }

    private void handleFailure(Throwable e) {
        handleFailure(e, null);
    }

    private void handleFailure(Throwable e, List<String> failedBatchMessageIds) {
        if (degradedModeManager != null) {
            boolean wasDegraded = degradedModeManager.isInDegradedMode();
            FailureClass failureClass = degradedModeManager.recordFailure(e);
            log.debug("Failure classified as {} for binding '{}'", failureClass, config.getId());

            // Data failures mark the failed batch's message IDs as suspects so
            // the bisection coordinator can track them across redelivery to
            // any listener thread (§6.1)
            if (failureClass.triggersDegradedMode() && failedBatchMessageIds != null) {
                degradedModeManager.markBatchSuspect(failedBatchMessageIds);
            }

            // Update health if we just entered degraded mode
            if (!wasDegraded && degradedModeManager.isInDegradedMode() && healthManager != null) {
                healthManager.recordDegraded(config.getId(),
                        "Entered degraded mode due to " + failureClass + " failure");
                if (metrics != null) {
                    metrics.recordDegradedModeEntry();
                }
            }
        }
    }

    /**
     * Collects JMS message IDs from a batch, skipping unreadable ones.
     */
    private List<String> collectMessageIds(List<Message> batch) {
        List<String> ids = new ArrayList<>(batch.size());
        for (Message message : batch) {
            try {
                String id = message.getJMSMessageID();
                if (id != null) {
                    ids.add(id);
                }
            } catch (JMSException e) {
                log.debug("Could not read JMSMessageID: {}", e.getMessage());
            }
        }
        return ids;
    }

    private void handleSuccess() {
        if (degradedModeManager != null) {
            boolean wasDegraded = degradedModeManager.isInDegradedMode();
            degradedModeManager.recordSuccess();

            // Update health if we just exited degraded mode
            if (wasDegraded && !degradedModeManager.isInDegradedMode() && healthManager != null) {
                healthManager.recordHealthy(config.getId());
                if (metrics != null) {
                    metrics.recordDegradedModeExit();
                }
            }
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
            if (metrics != null) {
                metrics.recordAuditFailure();
            }
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
        closeSessionResources();
    }

    private void closeSessionResources() {
        try {
            if (trackerProducer != null) {
                trackerProducer.close();
            }
        } catch (JMSException e) {
            log.debug("Error closing tracker producer: {}", e.getMessage());
        }
        trackerProducer = null;

        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (JMSException e) {
            log.debug("Error closing consumer: {}", e.getMessage());
        }
        consumer = null;

        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException e) {
            log.debug("Error closing session: {}", e.getMessage());
        }
        session = null;
    }

    /**
     * Determines if a JMSException indicates a broken session/connection.
     */
    private boolean isSessionBroken(JMSException e) {
        // Check if the linked exception indicates connection loss
        Exception linked = e.getLinkedException();

        // Common indicators of broken session/connection
        String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        String linkedMsg = linked != null && linked.getMessage() != null
                ? linked.getMessage().toLowerCase() : "";

        return msg.contains("connection") || msg.contains("session")
                || msg.contains("closed") || msg.contains("disconnect")
                || msg.contains("broken") || msg.contains("reset")
                || linkedMsg.contains("connection") || linkedMsg.contains("socket")
                || e.getErrorCode() != null && e.getErrorCode().startsWith("MQRC");
    }

    /**
     * Determines if an exception indicates a non-recoverable error.
     */
    private boolean isNonRecoverableError(JMSException e) {
        String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        String errorCode = e.getErrorCode();

        // Auth/security failures
        if (msg.contains("authentication") || msg.contains("authorization")
                || msg.contains("security") || msg.contains("not authorized")
                || msg.contains("password") || msg.contains("credential")) {
            return true;
        }

        // IBM MQ specific non-recoverable codes
        if (errorCode != null) {
            // 2035 = not authorized, 2063 = security error
            if (errorCode.equals("MQRC_NOT_AUTHORIZED") || errorCode.equals("2035")
                    || errorCode.equals("MQRC_SECURITY_ERROR") || errorCode.equals("2063")) {
                return true;
            }
        }

        return false;
    }

    /**
     * Attempts to recover the session with bounded exponential backoff.
     *
     * @return true if recovery succeeded, false if recovery failed after max attempts
     *         or was interrupted
     */
    private boolean recoverSession() {
        int attempts = reconnectAttempts.incrementAndGet();

        if (attempts > MAX_RECONNECT_ATTEMPTS) {
            log.error("Max reconnect attempts ({}) exceeded for binding '{}'",
                    MAX_RECONNECT_ATTEMPTS, config.getId());
            if (healthManager != null) {
                healthManager.recordUnhealthy(config.getId(),
                        new RuntimeException("Max reconnect attempts exceeded"));
            }
            if (metrics != null) {
                metrics.recordReconnectFailure();
            }
            return false;
        }

        log.warn("Attempting session recovery for binding '{}' (attempt {}/{})",
                config.getId(), attempts, MAX_RECONNECT_ATTEMPTS);

        // Update health to RECOVERING
        if (healthManager != null) {
            healthManager.recordRecovering(config.getId(),
                    String.format("Session reconnect attempt %d/%d", attempts, MAX_RECONNECT_ATTEMPTS));
        }

        // Close existing resources
        closeSessionResources();

        // Calculate backoff with jitter
        long backoffMs = calculateBackoff(attempts);

        log.debug("Waiting {}ms before reconnect attempt {} for binding '{}'",
                backoffMs, attempts, config.getId());

        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("Reconnect wait interrupted for binding '{}'", config.getId());
            return false;
        }

        // Check if we should still be running
        if (!running.get() || Thread.currentThread().isInterrupted()) {
            log.info("Recovery aborted - loop stopping for binding '{}'", config.getId());
            return false;
        }

        try {
            initializeSession();
            reconnectAttempts.set(0); // Reset on success
            reconnectCount.incrementAndGet();
            log.info("Session recovered successfully for binding '{}' after {} attempt(s)",
                    config.getId(), attempts);

            if (metrics != null) {
                metrics.recordReconnect();
            }

            // Restore health to HEALTHY after successful recovery
            if (healthManager != null) {
                healthManager.recordHealthy(config.getId());
            }

            return true;
        } catch (JMSException e) {
            log.error("Session recovery attempt {} failed for binding '{}': {}",
                    attempts, config.getId(), e.getMessage());

            if (isNonRecoverableError(e)) {
                log.error("Non-recoverable error detected for binding '{}', stopping recovery",
                        config.getId());
                return false;
            }

            // Recursive retry (will increment attempt counter)
            return recoverSession();
        }
    }

    /**
     * Calculates exponential backoff with jitter.
     */
    private long calculateBackoff(int attempt) {
        double exponential = INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, attempt - 1);
        long baseBackoff = Math.min((long) exponential, MAX_BACKOFF_MS);

        // Add jitter: +/- JITTER_FACTOR * baseBackoff
        double jitter = (ThreadLocalRandom.current().nextDouble() * 2 - 1) * JITTER_FACTOR * baseBackoff;

        return Math.max(INITIAL_BACKOFF_MS, (long) (baseBackoff + jitter));
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

    public long getReconnectCount() {
        return reconnectCount.get();
    }

    public int getCurrentReconnectAttempts() {
        return reconnectAttempts.get();
    }
}
