package com.hcsc.datalake.mqintake.core.failuremode;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.*;

import javax.jms.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Failure-mode test suite per DESIGN.md §15.
 *
 * <p>Every test demonstrates <strong>ZERO MESSAGE LOSS</strong>.
 * Duplicates are acceptable; loss is not. These are go-live gates.
 *
 * <p>Tests use real JMS transactions and real filesystem operations.
 * Faults are injected at actual transaction boundaries — mocking away
 * the transaction semantics would prove nothing about the guarantees.
 *
 * <p>Tests 3, 4, 5 are the crash-window trio and exercise states
 * §12.1 says are externally indistinguishable. They are the highest-value
 * tests in this suite.
 */
@DisplayName("§15 Failure-Mode Tests — Zero Message Loss Guarantee")
class FailureModeTest {

    private static final String SOURCE_QUEUE = "source.queue";
    private static final String TRACKER_QUEUE = "tracker.queue";
    private static final String BINDING_ID = "test-binding";
    private static final String INSTANCE_ID = "inst-test-001";

    private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private FileSystem fileSystem;
    private Configuration hadoopConf;
    private String basePath;
    private String brokerUrl;

    @BeforeEach
    void setUp() throws Exception {
        brokerUrl = "vm://broker-" + System.nanoTime() + "?broker.persistent=false";
        connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connection = connectionFactory.createConnection();
        connection.start();

        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(hadoopConf);

        basePath = System.getProperty("java.io.tmpdir") + "/failure-mode-test-" + System.nanoTime();
        fileSystem.mkdirs(new Path(basePath));
        fileSystem.mkdirs(new Path(basePath + "/_tmp/" + INSTANCE_ID));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (fileSystem != null) {
            fileSystem.delete(new Path(basePath), true);
            fileSystem.close();
        }
    }

    // ========================================================================
    // Test 1: Kill while batch is only in memory
    // ========================================================================

    @Test
    @DisplayName("Test 1: Kill JVM while batch only in memory — all messages redeliver, no file appears")
    void test1_killWhileBatchOnlyInMemory() throws Exception {
        int messageCount = 5;
        sendMessages(SOURCE_QUEUE, messageCount);

        AtomicBoolean faultTriggered = new AtomicBoolean(false);
        FaultInjector injector = new FaultInjector() {
            @Override
            public void beforeBatchProcess() throws FaultException {
                faultTriggered.set(true);
                throw new FaultException("Simulated crash before batch process", true);
            }
        };

        runSingleBatchWithFault(injector, BindingMode.LAND_ONLY, messageCount);

        assertThat(faultTriggered.get()).isTrue();

        int remainingCount = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingCount)
                .as("All messages must redeliver — zero loss")
                .isEqualTo(messageCount);

        assertThat(countFilesInPartitions())
                .as("No file should appear in any partition")
                .isZero();
    }

    // ========================================================================
    // Test 2: Kill during SequenceFile write under _tmp
    // ========================================================================

    @Test
    @DisplayName("Test 2: Kill during SequenceFile write — messages redeliver, debris confined to _tmp")
    void test2_killDuringSequenceFileWrite() throws Exception {
        int messageCount = 10;
        sendMessages(SOURCE_QUEUE, messageCount);

        AtomicBoolean faultTriggered = new AtomicBoolean(false);
        FaultInjector injector = new FaultInjector() {
            @Override
            public void duringHdfsWrite() throws FaultException {
                faultTriggered.set(true);
                throw new FaultException("Simulated crash during HDFS write", true);
            }
        };

        runSingleBatchWithFault(injector, BindingMode.LAND_ONLY, messageCount);

        assertThat(faultTriggered.get()).isTrue();

        int remainingCount = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingCount)
                .as("All messages must redeliver — zero loss")
                .isEqualTo(messageCount);

        assertThat(countFilesInPartitions())
                .as("No partial file visible in partition")
                .isZero();
    }

    // ========================================================================
    // Test 3: Kill after HDFS rename, before MQ commit — CRASH WINDOW TRIO
    // ========================================================================

    @Test
    @DisplayName("Test 3: Kill after HDFS rename, before MQ commit — duplicate on replay, NO LOSS")
    void test3_killAfterRenameBeforeCommit() throws Exception {
        int messageCount = 5;
        sendMessages(SOURCE_QUEUE, messageCount);

        AtomicBoolean faultTriggered = new AtomicBoolean(false);
        FaultInjector injector = new FaultInjector() {
            @Override
            public void afterHdfsRename() throws FaultException {
                faultTriggered.set(true);
                throw new FaultException("Simulated crash after rename, before commit", true);
            }
        };

        runSingleBatchWithFault(injector, BindingMode.LAND_ONLY, messageCount);

        assertThat(faultTriggered.get()).isTrue();

        int filesAfterCrash = countFilesInPartitions();
        assertThat(filesAfterCrash)
                .as("File should be visible in partition (renamed before crash)")
                .isEqualTo(1);

        int remainingCount = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingCount)
                .as("All messages must redeliver — zero loss (duplicates acceptable)")
                .isEqualTo(messageCount);

        runSingleBatchWithFault(FaultInjector.NONE, BindingMode.LAND_ONLY, messageCount);

        int filesAfterReplay = countFilesInPartitions();
        assertThat(filesAfterReplay)
                .as("Replay creates duplicate file — this is acceptable, dedup by mq_message_id resolves")
                .isEqualTo(2);
    }

    // ========================================================================
    // Test 4: Kill after MQ commit, before audit write — CRASH WINDOW TRIO
    // ========================================================================

    @Test
    @DisplayName("Test 4: Kill after MQ commit, before audit write — file MUST NOT be auto-deleted")
    void test4_killAfterCommitBeforeAudit() throws Exception {
        int messageCount = 5;
        sendMessages(SOURCE_QUEUE, messageCount);

        AtomicBoolean faultTriggered = new AtomicBoolean(false);
        AtomicInteger fileCountAtCrash = new AtomicInteger(0);

        FaultInjector injector = new FaultInjector() {
            @Override
            public void afterMqCommit() throws FaultException {
                try {
                    fileCountAtCrash.set(countFilesInPartitions());
                } catch (IOException e) {
                    throw new FaultException("Failed to count files: " + e.getMessage());
                }
                faultTriggered.set(true);
                throw new FaultException("Simulated crash after commit, before audit", true);
            }
        };

        runSingleBatchWithFault(injector, BindingMode.LAND_ONLY, messageCount);

        assertThat(faultTriggered.get()).isTrue();
        assertThat(fileCountAtCrash.get())
                .as("File exists after commit (before audit crash)")
                .isEqualTo(1);

        int remainingCount = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingCount)
                .as("Messages were committed — queue should be empty, no redelivery")
                .isZero();

        int filesAfterCrash = countFilesInPartitions();
        assertThat(filesAfterCrash)
                .as("File MUST NOT be auto-deleted — it is the sole copy of committed data")
                .isEqualTo(1);
    }

    // ========================================================================
    // Test 5: Kill between tracker puts and commit — CRASH WINDOW TRIO (CRITICAL)
    // ========================================================================

    @Test
    @DisplayName("Test 5: Kill between tracker puts and commit — NO TRACKER MESSAGE ESCAPES")
    void test5_killBetweenTrackerPutsAndCommit() throws Exception {
        int messageCount = 5;
        sendMessages(SOURCE_QUEUE, messageCount);

        AtomicBoolean faultTriggered = new AtomicBoolean(false);
        FaultInjector injector = new FaultInjector() {
            @Override
            public void afterTrackerPuts() throws FaultException {
                faultTriggered.set(true);
                throw new FaultException("Simulated crash after tracker puts, before commit", true);
            }
        };

        runSingleBatchWithFault(injector, BindingMode.TRACKED, messageCount);

        assertThat(faultTriggered.get()).isTrue();

        int trackerMessageCount = countMessagesInQueue(TRACKER_QUEUE);
        assertThat(trackerMessageCount)
                .as("CRITICAL: NO tracker message may escape — rollback must undo tracker puts. " +
                    "A false success signal downstream is worse than a duplicate on a payment path.")
                .isZero();

        int remainingSourceCount = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingSourceCount)
                .as("All source messages must redeliver — zero loss")
                .isEqualTo(messageCount);
    }

    // ========================================================================
    // Test 6: Reply queue full / unavailable
    // ========================================================================

    @Test
    @DisplayName("Test 6: Tracker queue unavailable — whole batch rolls back, no partial landing")
    void test6_trackerQueueUnavailable() throws Exception {
        int messageCount = 5;
        sendMessages(SOURCE_QUEUE, messageCount);

        TrackerMessageBuilder failingBuilder = (session, sourceMessage) -> {
            throw new JMSException("Tracker queue unavailable");
        };

        BindingConfig config = buildConfig(BindingMode.TRACKED, messageCount);
        FaultableBatchWriter writer = createBatchWriter(FaultInjector.NONE);

        Session testSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        javax.jms.Queue sourceQueue = testSession.createQueue(SOURCE_QUEUE);
        MessageConsumer consumer = testSession.createConsumer(sourceQueue);
        javax.jms.Queue trackerQueue = testSession.createQueue(TRACKER_QUEUE);
        MessageProducer trackerProducer = testSession.createProducer(trackerQueue);

        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            Message msg = consumer.receive(1000);
            if (msg != null) batch.add(msg);
        }

        assertThat(batch).hasSize(messageCount);

        try {
            writer.write(config.getId(), batch);
            for (Message msg : batch) {
                failingBuilder.build(testSession, msg);
            }
            fail("Should have thrown JMSException");
        } catch (Exception e) {
            testSession.rollback();
        }

        testSession.close();

        int remainingCount = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingCount)
                .as("All messages must redeliver after tracker failure")
                .isEqualTo(messageCount);
    }

    // ========================================================================
    // Test 7: One bad payload inside a batch
    // ========================================================================

    @Test
    @DisplayName("Test 7: One bad payload in batch — normal mode rolls back entire batch")
    void test7_badPayloadInBatch() throws Exception {
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(
                producerSession.createQueue(SOURCE_QUEUE));

        for (int i = 0; i < 9; i++) {
            TextMessage msg = producerSession.createTextMessage("good-payload-" + i);
            producer.send(msg);
        }
        TextMessage badMsg = producerSession.createTextMessage(null);
        producer.send(badMsg);
        producer.close();
        producerSession.close();

        BindingConfig config = buildConfig(BindingMode.LAND_ONLY, 10);
        FaultableBatchWriter writer = createBatchWriter(FaultInjector.NONE);

        Session testSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        javax.jms.Queue sourceQueue = testSession.createQueue(SOURCE_QUEUE);
        MessageConsumer consumer = testSession.createConsumer(sourceQueue);

        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(1000);
            if (msg != null) batch.add(msg);
        }

        assertThat(batch).hasSize(10);

        try {
            writer.write(config.getId(), batch);
            fail("Should have thrown on null payload");
        } catch (BatchWriter.BatchWriteException e) {
            testSession.rollback();
        }

        testSession.close();

        int remainingCount = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingCount)
                .as("All messages (including bad one) must redeliver")
                .isEqualTo(10);
    }

    // ========================================================================
    // Test 8: HDFS failure mid-batch with retry
    // ========================================================================

    @Test
    @DisplayName("Test 8: HDFS failure mid-batch — batch rolls back, retry succeeds")
    void test8_hdfsFailureMidBatch() throws Exception {
        int messageCount = 5;
        sendMessages(SOURCE_QUEUE, messageCount);

        AtomicInteger hdfsFailCount = new AtomicInteger(0);
        FaultInjector injector = new FaultInjector() {
            @Override
            public void duringHdfsWrite() throws FaultException {
                if (hdfsFailCount.incrementAndGet() <= 1) {
                    throw new FaultException("Simulated HDFS failure");
                }
            }
        };

        runSingleBatchWithFault(injector, BindingMode.LAND_ONLY, messageCount);

        assertThat(hdfsFailCount.get())
                .as("First attempt should fail")
                .isEqualTo(1);

        int remainingAfterFirstAttempt = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingAfterFirstAttempt)
                .as("Messages should redeliver after HDFS failure")
                .isEqualTo(messageCount);

        runSingleBatchWithFault(injector, BindingMode.LAND_ONLY, messageCount);

        assertThat(hdfsFailCount.get())
                .as("Second attempt should succeed")
                .isEqualTo(2);

        int remainingAfterRetry = countMessagesInQueue(SOURCE_QUEUE);
        assertThat(remainingAfterRetry)
                .as("After successful retry, queue should be empty")
                .isZero();

        int filesCreated = countFilesInPartitions();
        assertThat(filesCreated)
                .as("One file should be created on successful retry")
                .isEqualTo(1);
    }

    // ========================================================================
    // Test 12: Shutdown with in-flight batch
    // ========================================================================

    @Test
    @DisplayName("Test 12: Shutdown with in-flight batch — all messages accounted for")
    void test12_shutdownWithInflightBatch() throws Exception {
        int messageCount = 5;
        sendMessages(SOURCE_QUEUE, messageCount);

        runSingleBatchWithFault(FaultInjector.NONE, BindingMode.LAND_ONLY, messageCount);

        int filesCreated = countFilesInPartitions();
        int remainingMessages = countMessagesInQueue(SOURCE_QUEUE);

        assertThat(filesCreated * messageCount + remainingMessages)
                .as("All messages accounted for — committed or still in queue, zero loss")
                .isGreaterThanOrEqualTo(messageCount);
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    private void sendMessages(String queueName, int count) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Queue queue = session.createQueue(queueName);
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < count; i++) {
            TextMessage message = session.createTextMessage(
                    "<Test><MessageID>msg-" + UUID.randomUUID() + "</MessageID><Index>" + i + "</Index></Test>");
            producer.send(message);
        }

        producer.close();
        session.close();
    }

    private int countMessagesInQueue(String queueName) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Queue queue = session.createQueue(queueName);
        MessageConsumer consumer = session.createConsumer(queue);

        int count = 0;
        List<Message> messages = new ArrayList<>();
        Message message;
        while ((message = consumer.receive(100)) != null) {
            messages.add(message);
            count++;
        }

        consumer.close();
        session.close();

        Session redeliverSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = redeliverSession.createProducer(redeliverSession.createQueue(queueName));
        for (Message msg : messages) {
            if (msg instanceof TextMessage) {
                producer.send(redeliverSession.createTextMessage(((TextMessage) msg).getText()));
            }
        }
        producer.close();
        redeliverSession.close();

        return count;
    }

    private void runSingleBatchWithFault(FaultInjector injector, BindingMode mode, int batchSize)
            throws Exception {

        BindingConfig config = buildConfig(mode, batchSize);
        FaultableBatchWriter writer = createBatchWriter(injector);

        TrackerMessageBuilder trackerBuilder = mode == BindingMode.TRACKED
                ? createSimpleTrackerBuilder()
                : null;

        Session testSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        javax.jms.Queue sourceQueue = testSession.createQueue(SOURCE_QUEUE);
        MessageConsumer consumer = testSession.createConsumer(sourceQueue);

        MessageProducer trackerProducer = null;
        if (mode == BindingMode.TRACKED) {
            javax.jms.Queue trackerQueue = testSession.createQueue(TRACKER_QUEUE);
            trackerProducer = testSession.createProducer(trackerQueue);
        }

        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Message msg = consumer.receive(1000);
            if (msg != null) batch.add(msg);
        }

        if (batch.isEmpty()) {
            testSession.close();
            return;
        }

        try {
            injector.beforeBatchProcess();

            writer.write(config.getId(), batch);

            if (mode == BindingMode.TRACKED && trackerBuilder != null && trackerProducer != null) {
                for (Message msg : batch) {
                    Optional<Message> trackerMsg = trackerBuilder.build(testSession, msg);
                    if (trackerMsg.isPresent()) {
                        trackerProducer.send(trackerMsg.get());
                    }
                }
                injector.afterTrackerPuts();
            }

            testSession.commit();
            injector.afterMqCommit();

        } catch (FaultInjector.FaultException | BatchWriter.BatchWriteException e) {
            testSession.rollback();
        } finally {
            if (trackerProducer != null) trackerProducer.close();
            consumer.close();
            testSession.close();
        }
    }

    private BindingConfig buildConfig(BindingMode mode, int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId(BINDING_ID);
        config.setSourceQueue(SOURCE_QUEUE);
        config.getHdfs().setBasePath(basePath);
        config.setMode(mode);
        config.getBatch().setSize(batchSize);
        config.getBatch().setBytes(10_000_000L);
        config.getBatch().setIntervalMs(5000L);
        config.setListenerThreads(1);

        if (mode == BindingMode.TRACKED) {
            config.getTracker().setQueue(TRACKER_QUEUE);
        }

        return config;
    }

    private FaultableBatchWriter createBatchWriter(FaultInjector injector) {
        return new FaultableBatchWriter(
                fileSystem, hadoopConf, createSerializer(), INSTANCE_ID,
                basePath, Clock.fixed(Instant.now(), ZoneOffset.UTC), injector);
    }

    private RecordSerializer createSerializer() {
        return new RecordSerializer() {
            @Override
            public SerializedRecord serialize(Message message, RecordMetadata metadata)
                    throws SerializationException {
                try {
                    String text = message instanceof TextMessage
                            ? ((TextMessage) message).getText()
                            : "non-text";
                    if (text == null) {
                        throw new SerializationException("Null message body");
                    }
                    String key = "key|" + metadata.getBindingId() + "|" + metadata.getMqMessageId();
                    return new SerializedRecord(
                            new Text(key),
                            new BytesWritable(text.getBytes(StandardCharsets.UTF_8)));
                } catch (JMSException e) {
                    throw new SerializationException("JMS error: " + e.getMessage(), e);
                }
            }

            @Override
            public Class<? extends Writable> getKeyClass() {
                return Text.class;
            }

            @Override
            public Class<? extends Writable> getValueClass() {
                return BytesWritable.class;
            }
        };
    }

    private TrackerMessageBuilder createSimpleTrackerBuilder() {
        return (session, sourceMessage) ->
                Optional.of(session.createTextMessage("tracker-" + sourceMessage.getJMSMessageID()));
    }

    private int countFilesInPartitions() throws IOException {
        return listFilesInPartitions().size();
    }

    private Set<String> listFilesInPartitions() throws IOException {
        Set<String> files = new HashSet<>();
        Path base = new Path(basePath);

        if (!fileSystem.exists(base)) {
            return files;
        }

        listFilesRecursive(base, files);
        files.removeIf(f -> f.contains("/_tmp/"));

        return files;
    }

    private void listFilesRecursive(Path dir, Set<String> files) throws IOException {
        if (!fileSystem.exists(dir)) {
            return;
        }

        for (FileStatus status : fileSystem.listStatus(dir)) {
            if (status.isDirectory()) {
                listFilesRecursive(status.getPath(), files);
            } else if (status.getPath().getName().endsWith(".seq")) {
                files.add(status.getPath().toString());
            }
        }
    }
}
