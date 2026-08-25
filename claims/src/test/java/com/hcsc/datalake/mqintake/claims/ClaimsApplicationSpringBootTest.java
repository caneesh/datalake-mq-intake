package com.hcsc.datalake.mqintake.claims;

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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Real @SpringBootTest for the Claims application (round 2 prompt 11).
 *
 * <p>Boots the ACTUAL ClaimsApplication context and proves the LAND_ONLY
 * production path: startup validation → IntakeRuntimeManager → listener
 * threads → ClaimsRecordSerializer → SequenceFile _tmp/close/rename →
 * commit → audit → health, with NO tracker producer anywhere.
 * Only the MQ connection is substituted (embedded ActiveMQ).
 */
@SpringBootTest(classes = {ClaimsApplication.class,
        ClaimsApplicationSpringBootTest.EmbeddedBrokerMqConfig.class},
        properties = "claims.identity-field=CLM_XMITSN_ID")
class ClaimsApplicationSpringBootTest {

    private static final String SOURCE_QUEUE = "MQ.DMIH.CLAIMS.IN";

    private static java.nio.file.Path dataDir;
    private static java.nio.file.Path auditDir;
    private static Connection sharedConnection;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) throws Exception {
        dataDir = Files.createTempDirectory("claims-sbt-data");
        auditDir = Files.createTempDirectory("claims-sbt-audit");
        // Lists are not merged across property sources — redefine the binding
        registry.add("intake.bindings[0].id", () -> "claims");
        registry.add("intake.bindings[0].mq-connection", () -> "primary");
        registry.add("intake.bindings[0].source-queue", () -> SOURCE_QUEUE);
        registry.add("intake.bindings[0].mode", () -> "LAND_ONLY");
        registry.add("intake.bindings[0].hdfs-base-path", () -> dataDir.toString());
        registry.add("intake.bindings[0].batch-size", () -> 4);
        registry.add("intake.bindings[0].batch-bytes", () -> 64 * 1024 * 1024);
        registry.add("intake.bindings[0].batch-interval-ms", () -> 400);
        registry.add("intake.bindings[0].listener-threads", () -> 2);
        registry.add("intake.bindings[0].backout-queue", () -> "MQ.DMIH.CLAIMS.BACKOUT");
        // Production default is 30s; sampled fast here so the test does not
        // have to wait out a real interval
        registry.add("intake.bindings[0].backout-depth-poll-interval-ms", () -> 200);
        // BISECT with batch 4 requires threshold >= ceil(log2(4)) + 1 = 3
        registry.add("intake.bindings[0].backout-threshold", () -> 3);
        registry.add("intake.bindings[0].degradation-strategy", () -> "BISECT");
        registry.add("intake.hdfs.audit-base-path", () -> auditDir.toString());
        registry.add("intake.instance-id", () -> "claims-sbt");
    }

    @TestConfiguration
    static class EmbeddedBrokerMqConfig {
        @Bean
        @Primary
        MqConnectionManager testMqConnectionManager() throws Exception {
            ActiveMQConnectionFactory factory =
                    new ActiveMQConnectionFactory("vm://claims-sbt?broker.persistent=false");
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

    /**
     * The backout-depth gauge is what DESIGN §14 nominates as the pager
     * condition. It previously shipped as a permanent zero because nothing
     * wrote to it, so this asserts the whole chain through the real context —
     * BindingRuntimeFactory built a monitor, BindingRuntime started it, and it
     * moves the gauge — rather than just that the monitor class works.
     */
    @Test
    void backoutQueueDepthGaugeIsPopulatedThroughTheRealWiring() throws Exception {
        var runtime = runtimeManager.getRuntime("claims");
        assertThat(runtime.getBackoutDepthMonitor()).isNotNull();
        assertThat(runtime.getBackoutDepthMonitor().isRunning()).isTrue();

        var metrics = runtimeManager.getMetricsRegistry().forBinding("claims");

        // An empty backout queue must read as an observed zero, not an
        // unwritten default
        awaitTrue(5_000, () -> runtime.getBackoutDepthMonitor().isDepthAvailable());
        assertThat(runtime.getBackoutDepthMonitor().isDepthAvailable()).isTrue();
        assertThat(metrics.getBackoutQueueDepth()).isZero();

        // Put something on the backout queue and the gauge must follow
        try (Session session = sharedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageProducer producer = session.createProducer(
                     session.createQueue("MQ.DMIH.CLAIMS.BACKOUT"))) {
            producer.send(session.createTextMessage("<Claim>poison</Claim>"));
        }

        awaitTrue(5_000, () -> metrics.getBackoutQueueDepth() > 0);
        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(1);

        // And sampling must not have consumed it — the operator still has the
        // message to inspect
        assertThat(countOnQueue("MQ.DMIH.CLAIMS.BACKOUT")).isEqualTo(1);
    }

    @Test
    void claimsProductionPathLandsMessagesWithoutTracker() throws Exception {
        assertThat(runtimeManager.isRunning()).isTrue();
        assertThat(runtimeManager.getRuntime("claims")).isNotNull();
        // LAND_ONLY: no tracker producer anywhere in the runtime
        assertThat(runtimeManager.getRuntime("claims").hasTrackerBuilder()).isFalse();

        try (Session session = sharedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageProducer producer = session.createProducer(session.createQueue(SOURCE_QUEUE))) {
            for (int i = 0; i < 3; i++) {
                producer.send(session.createTextMessage(
                        "<Claim><CLM_XMITSN_ID>CLM-" + i + "</CLM_XMITSN_ID></Claim>"));
            }
        }

        awaitTrue(15_000, () -> countSeqFiles() > 0 && countOnQueue(SOURCE_QUEUE) == 0);

        assertThat(countSeqFiles()).isGreaterThan(0);
        assertThat(countOnQueue(SOURCE_QUEUE)).isZero();

        awaitTrue(5_000, this::auditRecordExists);
        assertThat(auditRecordExists()).isTrue();

        assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.UP);
        assertThat(healthIndicator.health().getDetails()).containsKey("claims");
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
}
