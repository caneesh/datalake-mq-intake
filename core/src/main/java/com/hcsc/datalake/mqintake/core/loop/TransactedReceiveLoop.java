package com.hcsc.datalake.mqintake.core.loop;

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
    private final TrackerMessageBuilder trackerMessageBuilder; // null for LAND_ONLY
    private final long receiveTimeoutMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);

    private volatile Session session;
    private volatile MessageConsumer consumer;
    private volatile MessageProducer trackerProducer; // null for LAND_ONLY
    private volatile Thread loopThread;

    /**
     * Creates a receive loop for the given binding.
     *
     * @param config                the binding configuration
     * @param connection            shared JMS connection (thread-safe)
     * @param batchWriter           writer for HDFS batches
     * @param trackerMessageBuilder builder for tracker messages (null for LAND_ONLY)
     * @param receiveTimeoutMs      timeout for receive() calls (for shutdown interruption)
     */
    public TransactedReceiveLoop(BindingConfig config,
                                  Connection connection,
                                  BatchWriter batchWriter,
                                  TrackerMessageBuilder trackerMessageBuilder,
                                  long receiveTimeoutMs) {
        this.config = config;
        this.connection = connection;
        this.batchWriter = batchWriter;
        this.trackerMessageBuilder = trackerMessageBuilder;
        this.receiveTimeoutMs = receiveTimeoutMs;

        // Validate: TRACKED bindings must have a tracker message builder
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

    /**
     * Initializes the JMS session, consumer, and producer (if TRACKED).
     * All are created from the same session per the design requirement.
     */
    private void initializeSession() throws JMSException {
        // Create transacted session: createSession(transacted=true, acknowledgeMode=0)
        // acknowledgeMode is ignored when transacted=true
        session = connection.createSession(true, Session.SESSION_TRANSACTED);

        // Create consumer for the source queue
        Queue sourceQueue = session.createQueue(config.getSourceQueue());
        consumer = session.createConsumer(sourceQueue);

        // For TRACKED bindings, create producer for the tracker queue ON THE SAME SESSION
        if (config.getMode() == BindingMode.TRACKED) {
            Queue trackerQueue = session.createQueue(config.getTrackerQueue());
            trackerProducer = session.createProducer(trackerQueue);
            log.debug("Created tracker producer for binding '{}' on queue '{}'",
                    config.getId(), config.getTrackerQueue());
        }

        log.info("Initialized session for binding '{}': source='{}', mode={}",
                config.getId(), config.getSourceQueue(), config.getMode());
    }

    /**
     * Main receive loop. Runs until stop() is called.
     */
    private void runLoop() {
        FlushTrigger flushTrigger = new FlushTrigger(
                config.getBatchSize(),
                config.getBatchBytes(),
                config.getBatchIntervalMs()
        );

        List<Message> batch = new ArrayList<>(config.getBatchSize());

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Bounded receive with timeout for predictable shutdown
                Message message = consumer.receive(receiveTimeoutMs);

                if (message != null) {
                    batch.add(message);
                    flushTrigger.trackMessage(message);

                    if (flushTrigger.shouldFlush()) {
                        processBatch(batch);
                        batch.clear();
                        flushTrigger.reset();
                    }
                } else {
                    // Timeout - check if we should flush a partial batch on time trigger
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
                    rollbackQuietly();
                    batch.clear();
                    flushTrigger.reset();
                }
            }
        }

        // Drain: flush any remaining messages on shutdown
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

    /**
     * Processes a batch: write to HDFS, send tracker messages (if TRACKED), commit.
     * Any failure rolls back the entire batch.
     */
    private void processBatch(List<Message> batch) {
        int batchSize = batch.size();
        log.debug("Processing batch of {} messages for binding '{}'",
                batchSize, config.getId());

        try {
            // Step 1: Write batch to HDFS (stub for now)
            batchWriter.write(config.getId(), batch);

            // Step 2: For TRACKED bindings, send tracker messages
            if (config.getMode() == BindingMode.TRACKED) {
                sendTrackerMessages(batch);
            }

            // Step 3: Commit the transaction
            session.commit();
            commitCount.incrementAndGet();
            messageCount.addAndGet(batchSize);

            log.debug("Committed batch of {} messages for binding '{}'",
                    batchSize, config.getId());

        } catch (Exception e) {
            // ANY failure before commit: roll back the entire batch
            log.error("Batch processing failed for binding '{}', rolling back {} messages: {}",
                    config.getId(), batchSize, e.getMessage(), e);
            rollbackQuietly();
        }
    }

    /**
     * Sends tracker messages for each source message in the batch.
     * Uses the same session as the consumer, so all operations are in one transaction.
     */
    private void sendTrackerMessages(List<Message> batch) throws JMSException {
        for (Message sourceMessage : batch) {
            Optional<Message> trackerMessage = trackerMessageBuilder.build(session, sourceMessage);
            if (trackerMessage.isPresent()) {
                trackerProducer.send(trackerMessage.get());
            }
            // Empty optional suppresses the send for this message (e.g., null header guard)
        }
    }

    /**
     * Rolls back the session, swallowing any exception.
     */
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

    /**
     * Stops the receive loop gracefully.
     */
    public void stop() {
        log.info("Stopping receive loop for binding '{}'", config.getId());
        running.set(false);

        // Interrupt the thread if it's blocked on receive
        if (loopThread != null) {
            loopThread.interrupt();
        }
    }

    /**
     * Cleans up JMS resources.
     */
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
