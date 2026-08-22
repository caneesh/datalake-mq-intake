package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.batch.CountingBatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.*;
import java.util.Optional;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the transacted receive loop.
 * Uses embedded ActiveMQ to verify transactional semantics.
 */
class TransactedReceiveLoopTest {

    private static final String SOURCE_QUEUE = "TEST.SOURCE";
    private static final String TRACKER_QUEUE = "TEST.TRACKER";
    private static final long RECEIVE_TIMEOUT_MS = 100;

    private Connection connection;
    private Session producerSession;
    private MessageProducer producer;
    private CountingBatchWriter batchWriter;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        // Embedded ActiveMQ with vm:// transport
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();

        // Separate session for sending test messages
        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue sourceQueue = producerSession.createQueue(SOURCE_QUEUE);
        producer = producerSession.createProducer(sourceQueue);

        batchWriter = new CountingBatchWriter();
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (producer != null) producer.close();
        if (producerSession != null) producerSession.close();
        if (connection != null) connection.close();
    }

    @Test
    void nMessagesConsumedAndCommittedAsOneUnit() throws Exception {
        // Given: 5 messages on the queue, batch size = 5
        BindingConfig config = createLandOnlyConfig(5);
        sendMessages(5);

        // When: run the loop until batch is committed
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: all 5 messages committed as one unit
        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(loop.getMessageCount()).isEqualTo(5);
        assertThat(batchWriter.getBatchCount()).isEqualTo(1);
        assertThat(batchWriter.getTotalMessageCount()).isEqualTo(5);

        // Verify queue is empty (messages consumed)
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(0);
    }

    @Test
    void failureMidBatchRollsBackAllMessages() throws Exception {
        // Given: 5 messages on the queue, batch writer will fail
        BindingConfig config = createLandOnlyConfig(5);
        sendMessages(5);

        batchWriter.setFailOnNextWrite(true, "Simulated HDFS failure");

        // When: run the loop (batch will fail)
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: rollback occurred, no commit
        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(loop.getCommitCount()).isEqualTo(0);
        assertThat(loop.getMessageCount()).isEqualTo(0);

        // All 5 messages are still on the queue (rolled back)
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(5);
    }

    @Test
    void landOnlyModeSkipsTrackerProducerEntirely() throws Exception {
        // Given: LAND_ONLY config (no tracker queue)
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        // When: run with null tracker message builder (valid for LAND_ONLY)
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: messages committed, no tracker messages sent
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(0);
    }

    @Test
    void trackedModeSendsTrackerMessagesInSameTransaction() throws Exception {
        // Given: TRACKED config with tracker queue
        BindingConfig config = createTrackedConfig(3);
        sendMessages(3);

        // Simple tracker builder that echoes the message
        TrackerMessageBuilder builder = (session, source) -> {
            TextMessage tracker = session.createTextMessage("TRACKER:" + ((TextMessage) source).getText());
            return Optional.of(tracker);
        };

        // When: run the loop
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, builder, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: 3 tracker messages sent (one per source message)
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(3);
    }

    @Test
    void trackerFailureRollsBackEntireBatch() throws Exception {
        // Given: TRACKED config, tracker builder will fail on message 2
        BindingConfig config = createTrackedConfig(3);
        sendMessages(3);

        final int[] callCount = {0};
        TrackerMessageBuilder failingBuilder = (session, source) -> {
            callCount[0]++;
            if (callCount[0] == 2) {
                throw new JMSException("Simulated tracker failure");
            }
            return Optional.of(session.createTextMessage("TRACKER"));
        };

        // When: run the loop
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, failingBuilder, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: entire batch rolled back
        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(loop.getCommitCount()).isEqualTo(0);

        // All messages still on source queue
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(3);
        // No tracker messages (transaction rolled back)
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(0);
    }

    @Test
    void trackedBindingRequiresTrackerMessageBuilder() {
        BindingConfig config = createTrackedConfig(5);

        assertThatThrownBy(() -> new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TrackerMessageBuilder");
    }

    @Test
    void multipleBatchesProcessedSequentially() throws Exception {
        // Given: 9 messages, batch size = 3 (will produce 3 full batches)
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(9);

        // When: run until all messages processed
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForMessages(loop, 9, 5000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: 3 commits (batches of 3, 3, 3)
        assertThat(loop.getCommitCount()).isEqualTo(3);
        assertThat(loop.getMessageCount()).isEqualTo(9);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(0);
    }

    @Test
    void emptyQueueDoesNotCommitEmptyBatch() throws Exception {
        // Given: empty queue
        BindingConfig config = createLandOnlyConfig(5);

        // When: run briefly with no messages
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        Thread.sleep(300); // Wait a few receive timeouts
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: no commits (no empty batches)
        assertThat(loop.getCommitCount()).isEqualTo(0);
        assertThat(loop.getMessageCount()).isEqualTo(0);
    }

    @Test
    void gracefulShutdownDrainsRemainingMessages() throws Exception {
        // Given: 10 messages, batch size = 5 (2 full batches)
        // We'll verify that both batches are processed even though
        // we stop before the second batch would normally flush on time
        BindingConfig config = createLandOnlyConfig(5);
        sendMessages(10);

        // When: run until both batches complete
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForMessages(loop, 10, 5000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        // Then: all messages processed in 2 batches
        assertThat(loop.getCommitCount()).isEqualTo(2);
        assertThat(loop.getMessageCount()).isEqualTo(10);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(0);
    }

    @Test
    void partialBatchDrainedOnShutdown() throws Exception {
        // Given: 3 messages, batch size = 5 (partial batch never reaches size trigger)
        // Use very short timeout so time trigger fires quickly
        BindingConfig config = createLandOnlyConfig(5);
        config.setBatchIntervalMs(200); // Short time trigger
        sendMessages(3);

        // When: run briefly and stop
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        // Wait for time trigger to flush the partial batch
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        // Then: partial batch committed via time trigger
        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(0);
    }

    @Test
    void trackerBuilderCanSuppressIndividualMessages() throws Exception {
        // Given: TRACKED config, builder suppresses message 2
        BindingConfig config = createTrackedConfig(3);
        sendMessages(3);

        final int[] callCount = {0};
        TrackerMessageBuilder selectiveBuilder = (session, source) -> {
            callCount[0]++;
            if (callCount[0] == 2) {
                return Optional.empty(); // Suppress this one
            }
            return Optional.of(session.createTextMessage("TRACKER"));
        };

        // When: run the loop
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, selectiveBuilder, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: only 2 tracker messages (one suppressed)
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(2);
    }

    // --- Helper methods ---

    private BindingConfig createLandOnlyConfig(int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId("test-land-only");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath("/data/raw/test");
        config.setBatchSize(batchSize);
        config.setBatchBytes(128 * 1024 * 1024);
        config.setBatchIntervalMs(30000);
        config.setListenerThreads(1);
        return config;
    }

    private BindingConfig createTrackedConfig(int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId("test-tracked");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.TRACKED);
        config.setTrackerQueue(TRACKER_QUEUE);
        config.setHdfsBasePath("/data/raw/test");
        config.setBatchSize(batchSize);
        config.setBatchBytes(128 * 1024 * 1024);
        config.setBatchIntervalMs(30000);
        config.setListenerThreads(1);
        return config;
    }

    private void sendMessages(int count) throws JMSException {
        for (int i = 0; i < count; i++) {
            producer.send(producerSession.createTextMessage("Message-" + i));
        }
    }

    private int countMessagesOnQueue(String queueName) throws JMSException {
        Session countSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            Queue queue = countSession.createQueue(queueName);
            QueueBrowser browser = countSession.createBrowser(queue);
            int count = 0;
            var enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
                enumeration.nextElement();
                count++;
            }
            browser.close();
            return count;
        } finally {
            countSession.close();
        }
    }

    private void waitForCommits(TransactedReceiveLoop loop, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (loop.getCommitCount() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    private void waitForRollbacks(TransactedReceiveLoop loop, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (loop.getRollbackCount() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    private void waitForMessages(TransactedReceiveLoop loop, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (loop.getMessageCount() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    private void waitForQueueEmpty(String queueName, long timeoutMs)
            throws InterruptedException, JMSException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (countMessagesOnQueue(queueName) > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    @Test
    void dataExceptionInvokesFailureClassifier() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        DegradedModeManager degradedModeManager = new DegradedModeManager(
                "test", 3, DegradationStrategy.BATCH_OF_ONE, 5);

        batchWriter.setFailOnNextWrite(true, new RecordSerializer.SerializationException("Bad data"));

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, degradedModeManager, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(degradedModeManager.isInDegradedMode()).isTrue();
        assertThat(degradedModeManager.getCurrentBatchSize()).isEqualTo(1);
    }

    @Test
    void infrastructureExceptionDoesNotEnterDegradedMode() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        DegradedModeManager degradedModeManager = new DegradedModeManager(
                "test", 3, DegradationStrategy.BATCH_OF_ONE, 5);

        batchWriter.setFailOnNextWrite(true, new java.io.IOException("HDFS unavailable"));

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, degradedModeManager, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(degradedModeManager.isInDegradedMode()).isFalse();
        assertThat(degradedModeManager.getCurrentBatchSize()).isEqualTo(3);
    }

    @Test
    void unknownExceptionDoesNotEnterDegradedMode() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        DegradedModeManager degradedModeManager = new DegradedModeManager(
                "test", 3, DegradationStrategy.BATCH_OF_ONE, 5);

        batchWriter.setFailOnNextWrite(true, new RuntimeException("Unknown error"));

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, degradedModeManager, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(degradedModeManager.isInDegradedMode()).isFalse();
        assertThat(degradedModeManager.getCurrentBatchSize()).isEqualTo(3);
    }

    @Test
    void auditEmittedOnlyAfterSuccessfulCommit() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        AtomicBoolean auditEmitted = new AtomicBoolean(false);
        AuditRecordEmitter mockEmitter = new TestAuditRecordEmitter() {
            @Override
            public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult, List<Message> messages) {
                auditEmitted.set(true);
            }
        };

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, mockEmitter, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(auditEmitted.get()).isTrue();
    }

    @Test
    void auditFailureDoesNotUndoCommittedTransaction() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        AuditRecordEmitter failingEmitter = new TestAuditRecordEmitter() {
            @Override
            public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult, List<Message> messages) {
                throw new RuntimeException("Audit system down");
            }
        };

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, failingEmitter, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(0);
    }

    private static abstract class TestAuditRecordEmitter implements AuditRecordEmitter {
        @Override
        public void emit(com.hcsc.datalake.mqintake.core.audit.AuditRecord record) {
            // No-op for tests
        }
    }

    @Test
    void metricsUpdatedOnSuccessfulCommit() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        BindingMetrics metrics = new BindingMetrics("test-binding");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(metrics.getCommitCount()).isEqualTo(1);
        assertThat(metrics.getMessagesWritten()).isEqualTo(3);
    }

    @Test
    void metricsUpdatedOnRollback() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        BindingMetrics metrics = new BindingMetrics("test-binding");
        batchWriter.setFailOnNextWrite(true, "HDFS failure");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(metrics.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(metrics.getCommitCount()).isEqualTo(0);
    }

    // --- Session Recovery Tests ---

    @Test
    void sessionRecoveryExposesReconnectCount() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // No reconnects needed for normal operation
        assertThat(loop.getReconnectCount()).isEqualTo(0);
        assertThat(loop.getCurrentReconnectAttempts()).isEqualTo(0);
    }

    @Test
    void shutdownInterruptsLoop() throws Exception {
        BindingConfig config = createLandOnlyConfig(100);
        // Don't send messages - loop will just wait on receive()

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);

        // Wait briefly for loop to start
        Thread.sleep(100);
        assertThat(loop.isRunning()).isTrue();

        // Stop should interrupt cleanly
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(loop.isRunning()).isFalse();
    }

    @Test
    void reconnectMetricsRecorded() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        BindingMetrics metrics = new BindingMetrics("test-binding");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Normal operation - no reconnects
        assertThat(metrics.getReconnectSuccessCount()).isEqualTo(0);
        assertThat(metrics.getReconnectFailureCount()).isEqualTo(0);
    }
}
