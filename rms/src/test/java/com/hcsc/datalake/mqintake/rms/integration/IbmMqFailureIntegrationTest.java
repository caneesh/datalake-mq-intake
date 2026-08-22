package com.hcsc.datalake.mqintake.rms.integration;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.reconciliation.SequenceFileIdentityReader;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

/**
 * Failure-path integration tests against real IBM MQ in Docker.
 *
 * <p>These close DESIGN §15 gaps that embedded ActiveMQ cannot prove, because
 * they depend on real queue-manager semantics:
 * <ul>
 *   <li>Real {@code JMSXDeliveryCount} accumulation across rollbacks — the
 *       input to application-owned poison detection (§4.2, §6.1)</li>
 *   <li>Real {@code JMSMessageID} stability across redelivery — the premise
 *       the bisection suspect-tracking coordinator relies on</li>
 *   <li>Real poison isolation ending in a backout-queue put on the same
 *       transaction, with real redelivery driving the bisection</li>
 *   <li>Real connection loss (MQRC_CONNECTION_BROKEN) and session recovery</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * docker-compose up -d ibm-mq
 * export MQ_USER=app MQ_PASSWORD=passw0rd
 * mvn test -pl rms -Dtest=IbmMqFailureIntegrationTest
 * </pre>
 *
 * <p>Skipped entirely when {@code MQ_USER} is unset.
 *
 * <p>Note on queue-manager configuration: BOTHRESH is deliberately 0 on these
 * queues. The application owns poison handling (§4.2) — it reads the backout
 * count and routes to the BOQ itself, rather than relying on queue-manager
 * requeue. The queue manager still increments the delivery count, which is all
 * the application needs.
 */
@EnabledIfEnvironmentVariable(named = "MQ_USER", matches = ".+")
@DisplayName("IBM MQ Failure-Path Integration Tests")
class IbmMqFailureIntegrationTest {

    private static final String HOST = "localhost";
    private static final int PORT = 1414;
    private static final String QUEUE_MANAGER = "QM1";
    private static final String CHANNEL = "DEV.APP.SVRCONN";
    private static final String SOURCE_QUEUE = "MQ.HPS.MEMBERSHIP.IN";
    private static final String BACKOUT_QUEUE = "MQ.HPS.MEMBERSHIP.BACKOUT";
    private static final String CONTAINER = "mq-intake-ibmmq";

    @TempDir
    Path tempDir;

    private Connection connection;
    private FileSystem fileSystem;
    private Configuration hadoopConf;
    private SequenceFileIdentityReader identityReader;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        String user = System.getenv("MQ_USER");
        String password = System.getenv("MQ_PASSWORD");
        assumeTrue(user != null && !user.isBlank(), "MQ_USER not set - skipping");

        try {
            connection = connectionFactory().createConnection(user, password);
            connection.start();
        } catch (JMSException e) {
            assumeTrue(false, "Cannot connect to MQ - is Docker running? " + e.getMessage());
        }

        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(hadoopConf);
        identityReader = new SequenceFileIdentityReader(hadoopConf);
        executor = Executors.newFixedThreadPool(2);

        drain(SOURCE_QUEUE);
        drain(BACKOUT_QUEUE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        // Safety net: never leave the channel stopped for the next test or run
        if (dockerAvailable()) {
            runmqsc("START CHANNEL(" + CHANNEL + ")");
        }
        try {
            drain(SOURCE_QUEUE);
            drain(BACKOUT_QUEUE);
        } catch (Exception ignored) {
        }
        try {
            if (connection != null) connection.close();
        } catch (Exception ignored) {
        }
    }

    private MQConnectionFactory connectionFactory() throws JMSException {
        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setHostName(HOST);
        factory.setPort(PORT);
        factory.setQueueManager(QUEUE_MANAGER);
        factory.setChannel(CHANNEL);
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        return factory;
    }

    // --- Real queue-manager semantics the application depends on ---

    @Test
    @DisplayName("Real MQ increments delivery count across rollbacks and PoisonMessageHandler reads it")
    void deliveryCountAccumulatesAcrossRollbacksOnRealMq() throws Exception {
        send(SOURCE_QUEUE, "POISON-PAYLOAD");

        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        // Each rollback returns the message; the queue manager increments the
        // delivery count. The unit tests can only assert the property NAME —
        // this asserts the real queue manager's behaviour.
        for (int expected = 1; expected <= 3; expected++) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(SOURCE_QUEUE))) {
                Message received = consumer.receive(5000);
                assertThat(received).as("delivery %d", expected).isNotNull();

                assertThat(handler.getDeliveryCount(received))
                        .as("delivery count on attempt %d", expected)
                        .isEqualTo(expected);
                // Not yet past BOTHRESH=3, so not poison
                assertThat(handler.isPoisonMessage(received)).isFalse();
            }
            session.rollback();
            session.close();
        }

        // Fourth delivery breaches the threshold
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        try (MessageConsumer consumer = session.createConsumer(session.createQueue(SOURCE_QUEUE))) {
            Message received = consumer.receive(5000);
            assertThat(handler.getDeliveryCount(received)).isEqualTo(4);
            assertThat(handler.isPoisonMessage(received)).isTrue();
        }
        session.rollback();
        session.close();
    }

    @Test
    @DisplayName("Real MQ keeps JMSMessageID stable across redelivery (bisection premise)")
    void messageIdIsStableAcrossRedeliveryOnRealMq() throws Exception {
        send(SOURCE_QUEUE, "STABLE-ID-PAYLOAD");

        Set<String> observedIds = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            try (MessageConsumer consumer = session.createConsumer(session.createQueue(SOURCE_QUEUE))) {
                Message received = consumer.receive(5000);
                assertThat(received).isNotNull();
                observedIds.add(received.getJMSMessageID());
            }
            session.rollback();
            session.close();
        }

        // DegradedModeManager tracks suspects by JMSMessageID across rollback
        // and redelivery to any listener thread. That is only sound if the id
        // is stable — which this asserts against the real queue manager.
        assertThat(observedIds)
                .as("JMSMessageID must not change across redelivery")
                .hasSize(1);
    }

    // --- §15.7 / §15.11 poison isolation on real MQ ---

    @Test
    @DisplayName("Poison message is bisected out and routed to the real backout queue")
    void poisonIsolatedToBackoutQueueOnRealMq() throws Exception {
        // 7 clean + 1 poison, batch of 8. BISECT requires
        // backout_threshold >= ceil(log2(8)) + 1 = 4, so clean messages that
        // share failing batches with the poison are never misrouted.
        for (int i = 0; i < 7; i++) {
            send(SOURCE_QUEUE, "CLEAN-" + i);
        }
        send(SOURCE_QUEUE, "POISON-A");

        BindingConfig config = bindingConfig(8, 4);
        DegradedModeManager degradedMode = new DegradedModeManager(
                "rms-it", 8, DegradationStrategy.BISECT, 2);
        BindingHealthManager health = new BindingHealthManager();
        BindingMetrics metrics = new BindingMetrics("rms-it");

        // Real SequenceFileBatchWriter; the injected serializer fails
        // deterministically on the poison payload (DESIGN §15.7)
        BatchWriter writer = new SequenceFileBatchWriter(
                fileSystem, hadoopConf, new PoisonSensitiveSerializer(),
                "it-instance", config.getId(), config.getHdfsBasePath());

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null,
                new PoisonMessageHandler(4, BACKOUT_QUEUE), degradedMode,
                health, null, metrics, "it-instance", 200);
        executor.submit(loop);

        awaitTrue(90_000, () -> landedIdentities(config).size() == 7 && depth(BACKOUT_QUEUE) == 1);
        loop.stop();

        // Every clean message landed exactly once; zero loss
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 7; i++) {
            expected.add("CLEAN-" + i);
        }
        assertThat(landedIdentities(config)).containsExactlyInAnyOrderElementsOf(expected);

        // Only the true poison reached the backout queue
        assertThat(drainBodies(BACKOUT_QUEUE)).containsExactly("POISON-A");

        // Source queue fully drained, poison metric recorded
        assertThat(depth(SOURCE_QUEUE)).isZero();
        assertThat(metrics.getPoisonMessagesRouted()).isEqualTo(1);
        assertThat(degradedMode.getSuspectCount()).isZero();
    }

    // --- §15.9 MQ connection loss and session recovery ---

    @Test
    @DisplayName("Loop recovers after real channel outage and resumes processing")
    void sessionRecoveryAfterRealChannelOutage() throws Exception {
        assumeTrue(dockerAvailable(), "docker CLI/container not available - skipping");

        BindingConfig config = bindingConfig(3, 5);
        BindingHealthManager health = new BindingHealthManager();
        BindingMetrics metrics = new BindingMetrics("rms-it");

        BatchWriter writer = new SequenceFileBatchWriter(
                fileSystem, hadoopConf, new PoisonSensitiveSerializer(),
                "it-instance", config.getId(), config.getHdfsBasePath());

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, null, null,
                health, null, metrics, "it-instance", 200);
        executor.submit(loop);

        // Confirm the loop is alive and landing before the outage
        send(SOURCE_QUEUE, "BEFORE-0");
        awaitTrue(30_000, () -> landedIdentities(config).contains("BEFORE-0"));

        // Break the connection underneath the running loop.
        // The channel MUST be restarted even if an assertion below fails —
        // otherwise a failing run leaves the queue manager unusable and every
        // later MQ test silently degrades to "skipped".
        boolean detectedRecovery;
        try {
            runmqsc("STOP CHANNEL(" + CHANNEL + ") MODE(FORCE)");

            // The loop must notice (MQRC_CONNECTION_BROKEN) and enter recovery
            awaitTrue(30_000, () -> isRecovering(loop, health));
            detectedRecovery = isRecovering(loop, health);
        } finally {
            runmqsc("START CHANNEL(" + CHANNEL + ")");
        }

        assertThat(detectedRecovery)
                .as("loop should detect the broken connection and start recovering")
                .isTrue();

        // Recovery recreates Session + Consumer from the same Connection and
        // resumes: messages sent after the outage must land.
        awaitTrue(90_000, () -> {
            try {
                send(SOURCE_QUEUE, "AFTER-0");
            } catch (Exception e) {
                return false; // queue manager may still be settling
            }
            return landedIdentities(config).contains("AFTER-0");
        });

        loop.stop();

        assertThat(landedIdentities(config))
                .as("messages from before and after the outage must both be landed")
                .contains("BEFORE-0", "AFTER-0");
        assertThat(loop.getReconnectCount())
                .as("at least one successful session recovery").isGreaterThan(0);
        assertThat(metrics.getReconnectSuccessCount()).isGreaterThan(0);
        assertThat(health.getStatus("rms-it"))
                .as("health returns to HEALTHY after recovery")
                .isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);
    }

    // --- helpers ---

    private BindingConfig bindingConfig(int batchSize, int backoutThreshold) {
        BindingConfig config = new BindingConfig();
        config.setId("rms-it");
        config.setMqConnection("primary");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath(tempDir.resolve("data").toString());
        config.setBatchSize(batchSize);
        config.setBatchBytes(64 * 1024 * 1024);
        config.setBatchIntervalMs(500);
        config.setListenerThreads(1);
        config.setBackoutQueue(BACKOUT_QUEUE);
        config.setBackoutThreshold(backoutThreshold);
        config.setDegradationStrategy(DegradationStrategy.BISECT);
        return config;
    }

    private Set<String> landedIdentities(BindingConfig config) throws Exception {
        Set<String> identities = new HashSet<>();
        org.apache.hadoop.fs.Path base = new org.apache.hadoop.fs.Path(config.getHdfsBasePath());
        if (!fileSystem.exists(base)) {
            return identities;
        }
        try {
            var iter = fileSystem.listFiles(base, true);
            while (iter.hasNext()) {
                String file = iter.next().getPath().toString();
                if (file.endsWith(".seq") && !file.contains("/_tmp/")) {
                    try {
                        identities.addAll(identityReader.extractIdentities(file));
                    } catch (Exception e) {
                        // mid-rename during polling; retry next poll
                    }
                }
            }
        } catch (RuntimeException | java.io.FileNotFoundException e) {
            // local FS listing race while the loop is active
        }
        return identities;
    }

    private void send(String queue, String body) throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageProducer p = s.createProducer(s.createQueue(queue))) {
            p.send(s.createTextMessage(body));
        }
    }

    private int depth(String queue) throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            QueueBrowser browser = s.createBrowser(s.createQueue(queue));
            int count = 0;
            var e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                e.nextElement();
                count++;
            }
            return count;
        }
    }

    private List<String> drainBodies(String queue) throws Exception {
        List<String> bodies = new ArrayList<>();
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageConsumer c = s.createConsumer(s.createQueue(queue))) {
            Message m;
            while ((m = c.receive(1000)) != null) {
                bodies.add(((TextMessage) m).getText());
            }
        }
        return bodies;
    }

    private void drain(String queue) throws Exception {
        drainBodies(queue);
    }

    private void awaitTrue(long timeoutMs, Check check) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (check.ok()) {
                    return;
                }
            } catch (Exception e) {
                // transient during outage windows — keep polling
            }
            Thread.sleep(250);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean ok() throws Exception;
    }

    private boolean isRecovering(TransactedReceiveLoop loop, BindingHealthManager health) {
        return loop.getCurrentReconnectAttempts() > 0
                || health.getStatus("rms-it") == BindingHealthManager.HealthStatus.RECOVERING;
    }

    private boolean dockerAvailable() {
        try {
            Process p = new ProcessBuilder("docker", "inspect", "-f", "{{.State.Running}}", CONTAINER)
                    .redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes()).trim();
            p.waitFor();
            return "true".equals(out);
        } catch (Exception e) {
            return false;
        }
    }

    private void runmqsc(String command) throws Exception {
        Process p = new ProcessBuilder("docker", "exec", CONTAINER, "bash", "-c",
                "echo \"" + command + "\" | runmqsc " + QUEUE_MANAGER)
                .redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        p.waitFor();
    }

    /**
     * Production-shaped serializer that fails deterministically on the poison
     * payload. SequenceFileBatchWriter wraps the failure as a
     * BatchWriteException whose message contains "serialize", which
     * FailureClassifier maps to MESSAGE_DATA — the classification that drives
     * degraded mode and bisection.
     */
    private static class PoisonSensitiveSerializer implements RecordSerializer {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata)
                throws SerializationException {
            try {
                String body = ((TextMessage) message).getText();
                if (body.startsWith("POISON")) {
                    throw new SerializationException("malformed payload: " + body);
                }
                Text key = new Text("binding_id=" + metadata.getBindingId()
                        + "|payload_guid=" + body
                        + "|mq_message_id=" + metadata.getMqMessageId()
                        + "|consume_ts_utc=" + metadata.getConsumeTimestamp());
                return new SerializedRecord(key,
                        new BytesWritable(body.getBytes(StandardCharsets.UTF_8)));
            } catch (JMSException e) {
                throw new SerializationException("cannot read message", e);
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
    }
}
