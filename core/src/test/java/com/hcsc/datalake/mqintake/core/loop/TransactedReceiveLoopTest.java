package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.batch.CountingBatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, builder, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: 3 tracker messages sent (one per source message)
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(3);
    }

    @Test
    void trackerContentFailureDoesNotRollBackByDefault() throws Exception {
        // MDB parity, and only for a CONTENT failure: one message's own
        // payload broke the header rewrite (HeaderRewriter runs replaceAll, so
        // regex metacharacters in tag content throw). It affects nothing else,
        // and failing the batch for it has no isolation path — see
        // trackerPutFailureRollsBackEvenByDefault for the case that does roll
        // back. The landed data is kept; that one notification is lost.
        BindingConfig config = createTrackedConfig(3);
        sendMessages(3);

        final int[] callCount = {0};
        TrackerMessageBuilder failingBuilder = (session, source) -> {
            callCount[0]++;
            if (callCount[0] == 2) {
                throw new java.util.regex.PatternSyntaxException(
                        "Unclosed character class", "<MesgStatus>[RCVD", 12);
            }
            return Optional.of(session.createTextMessage("TRACKER"));
        };

        BindingMetrics metrics = new BindingMetrics("test-binding");
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, failingBuilder, null, null, null, null,
                metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // The batch commits: all three messages land, source queue drains
        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isZero();

        // Two trackers sent, the failed one simply lost — and counted, not
        // silently swallowed the way the MDB does
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(2);
        assertThat(metrics.getTrackerFailureCount()).isEqualTo(1);
    }

    @Test
    void trackerContentFailureRollsBackWhenConfiguredToFailTheBatch() throws Exception {
        // The stricter §2.2 reading remains available for content failures:
        // tracker and get in one unit of work, so losing a tracker means
        // replaying the message.
        BindingConfig config = createTrackedConfig(3);
        config.getTracker().setFailBatchOnError(true);
        sendMessages(3);

        final int[] callCount = {0};
        TrackerMessageBuilder failingBuilder = (session, source) -> {
            callCount[0]++;
            if (callCount[0] == 2) {
                throw new java.util.regex.PatternSyntaxException(
                        "Unclosed character class", "<MesgStatus>[RCVD", 12);
            }
            return Optional.of(session.createTextMessage("TRACKER"));
        };

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, failingBuilder, null, null, null, null, null,
                "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(loop.getCommitCount()).isEqualTo(0);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isEqualTo(3);
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isZero();
    }

    @Test
    void trackedBindingRequiresTrackerMessageBuilder() {
        BindingConfig config = createTrackedConfig(5);

        assertThatThrownBy(() -> new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS))
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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
        config.getBatch().setIntervalMs(200); // Short time trigger
        sendMessages(3);

        // When: run briefly and stop
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
    void trackerPutFailureRollsBackEvenByDefault() throws Exception {
        // A JMSException is the provider refusing the put — tracker queue
        // full, message too big for it, producer broken. It is not about this
        // message and will refuse the next one too, so committing would land
        // every message with its acknowledgement silently dropped for as long
        // as the condition lasts. Rolls back WITHOUT fail-batch-on-error,
        // which is the behaviour change: this used to commit.
        BindingConfig config = createTrackedConfig(3);
        assertThat(config.getTracker().isFailBatchOnError())
                .as("the default is what is under test")
                .isFalse();
        sendMessages(3);

        TrackerMessageBuilder queueFullBuilder = (session, source) -> {
            throw new JMSException("MQRC_Q_FULL");
        };

        BindingMetrics metrics = new BindingMetrics("test-binding");
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, queueFullBuilder, null, null, null, null,
                metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(loop.getCommitCount()).isZero();
        assertThat(countMessagesOnQueue(SOURCE_QUEUE))
                .as("nothing lands while its acknowledgement cannot be sent")
                .isEqualTo(3);
        assertThat(metrics.getTrackerFailureCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void everyTrackerOutcomeIsCounted() throws Exception {
        // Sent, suppressed and failed each have their own counter. Before
        // this, only failures were counted: an upstream that stopped setting
        // MessageHeaderDetails produced silent suppressions at DEBUG, and
        // there was no positive signal whose absence could be alerted on.
        BindingConfig config = createTrackedConfig(3);
        sendMessages(3);

        final int[] callCount = {0};
        TrackerMessageBuilder mixedBuilder = (session, source) -> {
            callCount[0]++;
            if (callCount[0] == 2) {
                return Optional.empty();                       // suppressed
            }
            if (callCount[0] == 3) {
                throw new java.util.regex.PatternSyntaxException(   // content failure
                        "Unclosed character class", "<MesgStatus>[RCVD", 12);
            }
            return Optional.of(session.createTextMessage("TRACKER"));
        };

        BindingMetrics metrics = new BindingMetrics("test-binding");
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, mixedBuilder, null, null, null, null,
                metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(metrics.getTrackerSentCount()).isEqualTo(1);
        assertThat(metrics.getTrackerSuppressedCount()).isEqualTo(1);
        assertThat(metrics.getTrackerFailureCount()).isEqualTo(1);
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(1);
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
        BindingMetrics metrics = new BindingMetrics("test-binding");
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, selectiveBuilder, null, null, null, null,
                metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Then: only 2 tracker messages (one suppressed)
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(TRACKER_QUEUE)).isEqualTo(2);
        assertThat(metrics.getTrackerSentCount()).isEqualTo(2);
        assertThat(metrics.getTrackerSuppressedCount()).isEqualTo(1);
    }

    // --- Helper methods ---

    private BindingConfig createLandOnlyConfig(int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId("test-land-only");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.getHdfs().setBasePath("/data/raw/test");
        config.getBatch().setSize(batchSize);
        config.getBatch().setBytes(128 * 1024 * 1024);
        config.getBatch().setIntervalMs(30000);
        config.setListenerThreads(1);
        return config;
    }

    private BindingConfig createTrackedConfig(int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId("test-tracked");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.TRACKED);
        config.getTracker().setQueue(TRACKER_QUEUE);
        config.getHdfs().setBasePath("/data/raw/test");
        config.getBatch().setSize(batchSize);
        config.getBatch().setBytes(128 * 1024 * 1024);
        config.getBatch().setIntervalMs(30000);
        config.setListenerThreads(1);
        return config;
    }

    // --- Partition window placement ---

    /** Quarter-hour partition window, in ms. */
    private static final long WINDOW = 15L * 60L * 1000L;

    @Test
    void batchIsStampedWithItsOwnWindowNotTheFlushWindow() throws Exception {
        // The partition trigger fires on the first poll AFTER the window
        // closes, so the flush always happens in the NEXT window. Before the
        // anchor was threaded through, the writer read its own clock at that
        // moment and filed every partition-triggered batch one window late —
        // which, with batch_interval_ms 0 and a low-volume feed, is every
        // batch.
        MutableClock clock = new MutableClock(WINDOW * 1000 + 60_000);

        BindingConfig config = createLandOnlyConfig(1000);
        config.getBatch().setIntervalMs(0);   // size is never reached; only the boundary flushes

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, null,
                "test-instance", RECEIVE_TIMEOUT_MS, null, null, null, clock);
        Future<?> future = executor.submit(loop);

        try {
            sendMessages(2);
            waitForQueueEmpty(SOURCE_QUEUE, 2000);   // both are in the batch

            // Window closes. The idle poll notices and flushes.
            clock.set(WINDOW * 1001 + 500);
            waitForCommits(loop, 1, 3000);

            // A message in the new window, flushed when that window closes.
            sendMessages(1);
            waitForQueueEmpty(SOURCE_QUEUE, 2000);
            clock.set(WINDOW * 1002 + 500);
            waitForCommits(loop, 2, 3000);
        } finally {
            loop.stop();
            future.get(2, TimeUnit.SECONDS);
        }

        List<CountingBatchWriter.Written> writes = batchWriter.getWritten();
        assertThat(writes).hasSize(2);

        assertThat(writes.get(0).getMessageCount()).isEqualTo(2);
        assertThat(PartitionPath.windowId(writes.get(0).getPartitionInstant()))
                .as("first batch belongs to the window it accumulated in, not the one it flushed in")
                .isEqualTo(1000L);

        assertThat(writes.get(1).getMessageCount()).isEqualTo(1);
        assertThat(PartitionPath.windowId(writes.get(1).getPartitionInstant()))
                .isEqualTo(1001L);
    }

    @Test
    void oneBatchNeverSpansTwoWindows() throws Exception {
        MutableClock clock = new MutableClock(WINDOW * 2000);

        BindingConfig config = createLandOnlyConfig(1000);
        config.getBatch().setIntervalMs(0);

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, null,
                "test-instance", RECEIVE_TIMEOUT_MS, null, null, null, clock);
        Future<?> future = executor.submit(loop);

        try {
            sendMessages(3);
            waitForQueueEmpty(SOURCE_QUEUE, 2000);
            clock.set(WINDOW * 2001);
            waitForCommits(loop, 1, 3000);

            sendMessages(4);
            waitForQueueEmpty(SOURCE_QUEUE, 2000);
            clock.set(WINDOW * 2002);
            waitForCommits(loop, 2, 3000);
        } finally {
            loop.stop();
            future.get(2, TimeUnit.SECONDS);
        }

        List<CountingBatchWriter.Written> writes = batchWriter.getWritten();
        assertThat(writes).hasSize(2);
        assertThat(writes.get(0).getMessageCount()).isEqualTo(3);
        assertThat(writes.get(1).getMessageCount()).isEqualTo(4);
        assertThat(PartitionPath.windowId(writes.get(0).getPartitionInstant()))
                .isNotEqualTo(PartitionPath.windowId(writes.get(1).getPartitionInstant()));
    }

    /** A clock a test can move across a partition boundary without waiting. */
    private static class MutableClock extends java.time.Clock {
        private final AtomicLong millis;

        MutableClock(long startMillis) {
            this.millis = new AtomicLong(startMillis);
        }

        void set(long value) {
            millis.set(value);
        }

        @Override
        public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override
        public java.time.Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public java.time.Instant instant() {
            return java.time.Instant.ofEpochMilli(millis.get());
        }

        @Override
        public long millis() {
            return millis.get();
        }
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
                config, connection, batchWriter, null, null, degradedModeManager, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, degradedModeManager, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, degradedModeManager, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, mockEmitter, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(auditEmitted.get()).isTrue();
    }

    @Test
    void auditFailureRollsBackSoNoUnauditedDataIsCommitted() throws Exception {
        // ABC posture, and the default. The audit record is a control: data
        // committed without one is data no balance can account for — consumed
        // and gone from the queue, landed on HDFS, and recorded nowhere.
        // Rolling back keeps the messages on the queue, so nothing is lost.
        BindingConfig config = createLandOnlyConfig(3);
        assertThat(config.getAudit().isFailBatchOnError())
                .as("audit must fail closed by default").isTrue();
        sendMessages(3);

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null,
                alwaysFailingEmitter(), null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 3000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(loop.getCommitCount()).isZero();
        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE))
                .as("messages stay on the queue — a stall, not a loss")
                .isEqualTo(3);
    }

    @Test
    void auditFailureCanBeConfiguredToCommitAnyway() throws Exception {
        // The opt-out, for a feed where an unaudited landing beats a stall.
        BindingConfig config = createLandOnlyConfig(3);
        config.getAudit().setFailBatchOnError(false);
        sendMessages(3);

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null,
                alwaysFailingEmitter(), null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 3000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(loop.getMessageCount()).isEqualTo(3);
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isZero();
    }

    private AuditRecordEmitter alwaysFailingEmitter() {
        return new TestAuditRecordEmitter() {
            @Override
            public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                             List<Message> messages) {
                throw new RuntimeException("Audit system down");
            }

            @Override
            public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                             List<Message> messages, int backoutCount) {
                throw new RuntimeException("Audit system down");
            }
        };
    }

    private static abstract class TestAuditRecordEmitter implements AuditRecordEmitter {
        @Override
        public void emit(com.hcsc.datalake.mqintake.core.audit.AuditRecord record) {
            // No-op for tests
        }

        @Override
        public void emitBackoutOnly(String bindingId, java.util.List<Message> messages,
                                    int backoutCount) {
            // No-op for tests
        }
    }

    @Test
    void metricsUpdatedOnSuccessfulCommit() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        BindingMetrics metrics = new BindingMetrics("test-binding");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null, metrics, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, null, metrics, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

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
                config, connection, batchWriter, null, null, null, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);

        // Poll with a deadline, like every other wait in this file — a fixed
        // sleep occasionally loses to a slow session open under CI load.
        long deadline = System.currentTimeMillis() + 5_000;
        while (!loop.isRunning() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
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
                config, connection, batchWriter, null, null, null, null, null, metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForCommits(loop, 1, 2000);
        loop.stop();
        future.get(1, TimeUnit.SECONDS);

        // Normal operation - no reconnects
        assertThat(metrics.getReconnectSuccessCount()).isEqualTo(0);
        assertThat(metrics.getReconnectFailureCount()).isEqualTo(0);
    }

    @Test
    void postCommitBookkeepingFailureDoesNotRollBackOrMarkSuspect() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);

        // clearSuspects() runs AFTER session.commit(). It used to sit inside
        // the try whose catch rolls back and marks the batch suspect, so a
        // throw here reported a rollback that could not happen (the commit
        // already succeeded) and marked message IDs that were already off the
        // queue. Those IDs can never be redelivered, so clearSuspects() could
        // never retire them and the binding would refuse to leave degraded
        // mode for the life of the process.
        DegradedModeManager degradedModeManager = new DegradedModeManager(
                "test", 3, DegradationStrategy.BATCH_OF_ONE, 5) {
            @Override
            public void clearSuspects(java.util.Collection<String> messageIds) {
                throw new java.lang.IllegalStateException("bookkeeping failed after commit");
            }
        };

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, degradedModeManager, null, null, null, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForQueueEmpty(SOURCE_QUEUE, 3000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        // The unit of work stands: committed, drained, nothing rolled back
        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(loop.getRollbackCount()).isZero();
        assertThat(countMessagesOnQueue(SOURCE_QUEUE)).isZero();

        // And the binding is not wedged
        assertThat(degradedModeManager.isInDegradedMode()).isFalse();
        assertThat(degradedModeManager.getSuspectCount()).isZero();
    }


    @Test
    void operationalMetricsArePopulatedOnTheProductionPath() throws Exception {
        // These five were defined but never called from production code, so a
        // dashboard built on them would have shown flat zeros forever.
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);
        BindingMetrics metrics = new BindingMetrics("test");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null,
                metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForQueueEmpty(SOURCE_QUEUE, 3000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(metrics.getMessagesConsumed())
                .as("counted at commit, so it matches what actually left the queue")
                .isEqualTo(3);
        assertThat(metrics.getFlushCount()).isEqualTo(1);
        assertThat(metrics.getLastFlushLatency()).isPositive();
        assertThat(metrics.getAverageFlushLatency()).isPositive();
        assertThat(metrics.isHealthy()).isTrue();

        // Reset to zero once the batch is flushed, so the gauge reads as
        // in-flight depth rather than sticking at the last batch's size
        assertThat(metrics.getCurrentBatchSize()).isZero();
    }

    @Test
    void consumedCountIsNotInflatedByRedelivery() throws Exception {
        // Counting on receive would tally the same message on every redelivery
        // and overstate throughput exactly when a poison message is churning.
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);
        BindingMetrics metrics = new BindingMetrics("test");

        batchWriter.setFailOnNextWrite(true, new java.io.IOException("HDFS unavailable"));

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, batchWriter, null, null, null, null, null,
                metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 3000);
        // Let the redelivery succeed (the stub's failure flag is sticky)
        batchWriter.setFailOnNextWrite(false);
        waitForCommits(loop, 1, 5000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        // One rollback then a successful redelivery: 3 messages, counted once
        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(metrics.getMessagesConsumed()).isEqualTo(3);
    }

    @Test
    void failureMarksMetricsUnhealthy() throws Exception {
        BindingConfig config = createLandOnlyConfig(3);
        sendMessages(3);
        BindingMetrics metrics = new BindingMetrics("test");

        // Always fails, so healthy=false is the final state rather than a
        // value a later successful redelivery would overwrite.
        BatchWriter alwaysFails = (bindingId, messages, partitionInstant) -> {
            throw new BatchWriter.BatchWriteException("HDFS down");
        };

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, alwaysFails, null, null, null, null, null,
                metrics, "test-instance", RECEIVE_TIMEOUT_MS);

        Future<?> future = executor.submit(loop);
        waitForRollbacks(loop, 1, 3000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(metrics.isHealthy()).isFalse();
    }

}
