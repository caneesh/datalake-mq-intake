package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.health.BindingsHealthIndicator;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.runtime.IntakeRuntimeManager;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.jms.*;
import java.nio.file.Files;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Real @SpringBootTest for the RMS application (round 2 prompt 11).
 *
 * <p>Boots the ACTUAL RmsApplication context: Spring startup validation,
 * HDFS FileSystem init, IntakeRuntimeManager SmartLifecycle start,
 * BindingRuntimeFactory, listener threads with dedicated transacted sessions,
 * RmsRecordSerializer, tracker send, commit, audit, health indicator.
 * Only the MQ connection itself is substituted (embedded ActiveMQ standing in
 * for IBM MQ) — everything else is the production wiring.
 */
@SpringBootTest(classes = {RmsApplication.class,
        RmsApplicationSpringBootTest.EmbeddedBrokerMqConfig.class})
class RmsApplicationSpringBootTest {

    private static final String SOURCE_QUEUE = "MQ.HPS.MEMBERSHIP.IN";
    private static final String TRACKER_QUEUE = "MQ.HPS.MEMBERSHIP.TRACKER";

    private static java.nio.file.Path dataDir;
    private static java.nio.file.Path auditDir;
    private static Connection sharedConnection;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) throws Exception {
        dataDir = Files.createTempDirectory("rms-sbt-data");
        auditDir = Files.createTempDirectory("rms-sbt-audit");
        // Lists are not merged across property sources — the whole binding
        // must be redefined here, mirroring application.yml with test paths
        registry.add("intake.bindings[0].id", () -> "rms");
        registry.add("intake.bindings[0].mq-connection", () -> "primary");
        registry.add("intake.bindings[0].source-queue", () -> SOURCE_QUEUE);
        registry.add("intake.bindings[0].mode", () -> "TRACKED");
        registry.add("intake.bindings[0].tracker-queue", () -> TRACKER_QUEUE);
        registry.add("intake.bindings[0].tracker-body-mode", () -> "FULL_COPY");
        registry.add("intake.bindings[0].hdfs-base-path", () -> dataDir.toString());
        registry.add("intake.bindings[0].batch-size", () -> 5);
        registry.add("intake.bindings[0].batch-bytes", () -> 64 * 1024 * 1024);
        registry.add("intake.bindings[0].batch-interval-ms", () -> 400);
        registry.add("intake.bindings[0].listener-threads", () -> 2);
        registry.add("intake.bindings[0].backout-queue", () -> "MQ.HPS.MEMBERSHIP.BACKOUT");
        registry.add("intake.bindings[0].backout-threshold", () -> 5);
        registry.add("intake.bindings[0].degradation-strategy", () -> "BATCH_OF_ONE");
        registry.add("intake.hdfs.audit-base-path", () -> auditDir.toString());
        registry.add("intake.instance-id", () -> "rms-sbt");
    }

    @TestConfiguration
    static class EmbeddedBrokerMqConfig {
        @Bean
        @Primary
        MqConnectionManager testMqConnectionManager() throws Exception {
            ActiveMQConnectionFactory factory =
                    new ActiveMQConnectionFactory("vm://rms-sbt?broker.persistent=false");
            sharedConnection = factory.createConnection();
            sharedConnection.start();

            MqConnectionConfig config = new MqConnectionConfig();
            config.setId("primary");
            config.setReceiveTimeoutMs(200);

            MqConnectionManager manager = mock(MqConnectionManager.class);
            when(manager.hasConnection("primary")).thenReturn(true);
            when(manager.getConnection("primary")).thenReturn(sharedConnection);
            when(manager.getConfig("primary")).thenReturn(Optional.of(config));
            return manager;
        }
    }

    @AfterAll
    static void closeBroker() throws Exception {
        if (sharedConnection != null) {
            sharedConnection.close();
        }
    }

    @Autowired
    private IntakeRuntimeManager runtimeManager;

    @Autowired
    private BindingsHealthIndicator healthIndicator;

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @Test
    void rmsProductionPathLandsMessagesAndSendsTrackers() throws Exception {
        assertThat(runtimeManager.isRunning()).isTrue();
        assertThat(runtimeManager.getRuntime("rms")).isNotNull();
        assertThat(runtimeManager.getRuntime("rms").hasTrackerBuilder()).isTrue();

        // Send 3 RMS-shaped messages with MessageHeaderDetails so the
        // tracker builder emits (null header would suppress per §20.3)
        try (Session session = sharedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageProducer producer = session.createProducer(session.createQueue(SOURCE_QUEUE))) {
            for (int i = 0; i < 3; i++) {
                String guid = UUID.randomUUID().toString();
                TextMessage message = session.createTextMessage(
                        "<Member><MessageID>" + guid + "</MessageID></Member>");
                message.setStringProperty("MessageHeaderDetails",
                        "<MessageHeaderDetailsType><Origin>test</Origin>"
                                + "</MessageHeaderDetailsType>");
                producer.send(message);
            }
        }

        // Await landing through the real chain: receive → serialize →
        // _tmp write → close → rename → tracker send → commit → audit
        awaitTrue(15_000, () -> countSeqFiles() > 0 && countOnQueue(TRACKER_QUEUE) == 3);

        assertThat(countSeqFiles()).isGreaterThan(0);
        assertThat(countOnQueue(TRACKER_QUEUE)).isEqualTo(3);
        assertThat(countOnQueue(SOURCE_QUEUE)).isZero();

        // Immutable audit written for the committed batch
        awaitTrue(5_000, this::auditRecordExists);
        assertThat(auditRecordExists()).isTrue();

        // The legacy header rewrite ran end to end through the real context:
        // all five tags injected inside the root element with the configured
        // values from application.yml.
        String header = firstTrackerHeader();
        assertThat(header).contains("<ReportingSystem>DMIH/DL</ReportingSystem>");
        assertThat(header).contains("<SourceSystem>IIB</SourceSystem>");
        assertThat(header).contains("<MesgStatus>RCVD</MesgStatus>");
        assertThat(header).contains("<DestSystem></DestSystem>");
        assertThat(header).containsPattern(
                "<CreatedTimeStamp>\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}</CreatedTimeStamp>");
        assertThat(header).contains("<Origin>test</Origin>");   // original content kept
        assertThat(header).endsWith("</MessageHeaderDetailsType>");

        // Health indicator reports the binding
        assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.UP);
        assertThat(healthIndicator.health().getDetails()).containsKey("rms");
    }

    /** Browses (does not consume) the first tracker message's rewritten header. */
    private String firstTrackerHeader() throws Exception {
        try (Session s = sharedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            QueueBrowser browser = s.createBrowser(s.createQueue(TRACKER_QUEUE));
            var e = browser.getEnumeration();
            assertThat(e.hasMoreElements()).as("a tracker message to inspect").isTrue();
            return ((Message) e.nextElement()).getStringProperty("MessageHeaderDetails");
        }
    }

    private boolean auditRecordExists() {
        try (var stream = Files.walk(auditDir)) {
            return stream.anyMatch(p -> p.getFileName().toString().startsWith("audit_"));
        } catch (java.io.IOException | java.io.UncheckedIOException e) {
            return false;
        }
    }

    private long countSeqFiles() throws Exception {
        // The writer renames out of _tmp while this walks, so entries (and the
        // local filesystem's .crc sidecars) can vanish mid-stream. Treat that
        // as "not counted yet" and let the caller poll again.
        try (var stream = Files.walk(dataDir)) {
            return stream.filter(p -> p.toString().endsWith(".seq")
                    && !p.toString().contains("/_tmp/")).count();
        } catch (java.io.UncheckedIOException e) {
            return 0;
        }
    }

    private int countOnQueue(String queueName) throws Exception {
        try (Session s = sharedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            QueueBrowser browser = s.createBrowser(s.createQueue(queueName));
            int count = 0;
            var e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                e.nextElement();
                count++;
            }
            return count;
        }
    }

    private void awaitTrue(long timeoutMs, Check check) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(100);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean ok() throws Exception;
    }

    /**
     * BindingMetrics was fully populated but read by nothing — no monitoring
     * system could see a single counter. This asserts the meters are actually
     * published through Actuator's registry, tagged per binding, and that they
     * carry the values the run produced rather than registering as zeros.
     */
    @Test
    void intakeMetricsArePublishedThroughActuator() throws Exception {
        // Drive real traffic first so the counters have something to report
        try (Session session = sharedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageProducer producer = session.createProducer(session.createQueue(SOURCE_QUEUE))) {
            for (int i = 0; i < 3; i++) {
                TextMessage message = session.createTextMessage(
                        "<Member><MessageID>" + UUID.randomUUID() + "</MessageID></Member>");
                message.setStringProperty("MessageHeaderDetails",
                        "<MessageHeaderDetailsType><Origin>metrics</Origin>"
                                + "</MessageHeaderDetailsType>");
                producer.send(message);
            }
        }

        awaitTrue(15_000, () -> countSeqFiles() > 0);

        var consumed = meterRegistry.find("mq_intake_messages_consumed_total")
                .tag("binding", "rms").functionCounter();
        assertThat(consumed).as("counter must be registered, tagged by binding").isNotNull();

        awaitTrue(10_000, () -> consumed.count() > 0);
        assertThat(consumed.count()).isGreaterThan(0);

        // The rest of the operational surface is registered too
        assertThat(meterRegistry.find("mq_intake_batches_committed_total")
                .tag("binding", "rms").functionCounter()).isNotNull();
        assertThat(meterRegistry.find("mq_intake_poison_routed_total")
                .tag("binding", "rms").functionCounter()).isNotNull();
        assertThat(meterRegistry.find("mq_intake_backout_queue_depth")
                .tag("binding", "rms").gauge()).isNotNull();
        assertThat(meterRegistry.find("mq_intake_flush_latency_seconds")
                .tag("binding", "rms").gauge()).isNotNull();

        // Flush latency is a real measurement, not a placeholder zero
        assertThat(meterRegistry.find("mq_intake_flush_latency_seconds")
                .tag("binding", "rms").gauge().value()).isGreaterThan(0.0);

        // Untagged publication would let one stalled binding hide behind the
        // aggregate, which DESIGN §14 calls out explicitly
        assertThat(meterRegistry.find("mq_intake_messages_consumed_total").meters())
                .allSatisfy(m -> assertThat(m.getId().getTag("binding")).isNotNull());
    }

}
