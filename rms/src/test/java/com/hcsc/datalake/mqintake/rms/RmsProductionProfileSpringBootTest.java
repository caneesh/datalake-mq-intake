package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.runtime.IntakeRuntimeManager;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.jms.Connection;
import java.nio.file.Files;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Boots the real RMS application with the {@code prod} profile active.
 *
 * <p>This is the test that would have caught the blocker. RMS could not start
 * in production mode at all: {@code RmsRecordSerializer} carried the
 * {@code PlaceholderSerializer} marker, and {@code SerializerValidator}
 * rejects those in production — so {@code validateOrFail} threw during
 * {@code IntakeRuntimeManager.start()} and the context never came up. Nothing
 * caught it because every existing boot test ran without a production profile,
 * where the same condition only logs a warning.
 *
 * <p>It also covers the detection half: production mode is enabled here purely
 * by the Spring profile, with {@code MQ_INTAKE_PRODUCTION} unset. Before, the
 * gates read only that environment variable, so this context would have booted
 * with every production check quietly disabled — passing for the wrong reason.
 */
@SpringBootTest(classes = {RmsApplication.class,
        RmsProductionProfileSpringBootTest.EmbeddedBrokerMqConfig.class})
@ActiveProfiles("prod")
class RmsProductionProfileSpringBootTest {

    private static final String SOURCE_QUEUE = "MQ.HPS.MEMBERSHIP.IN";

    private static java.nio.file.Path dataDir;
    private static java.nio.file.Path auditDir;
    private static Connection sharedConnection;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) throws Exception {
        dataDir = Files.createTempDirectory("rms-prod-data");
        auditDir = Files.createTempDirectory("rms-prod-audit");
        // Production mode refuses the yaml's dev-placeholder connection
        // defaults (DevDefaultConnectionGate), so this prod-profile boot must
        // carry non-dev values. The connection manager is mocked; the values
        // are never dialled.
        registry.add("intake.mq-connections.primary.host", () -> "mq-prod-test.internal");
        registry.add("intake.mq-connections.primary.queue-manager", () -> "QMTEST");
        registry.add("intake.mq-connections.primary.channel", () -> "TEST.SVRCONN");
        registry.add("intake.bindings[0].id", () -> "rms");
        registry.add("intake.bindings[0].mq-connection", () -> "primary");
        registry.add("intake.bindings[0].source-queue", () -> SOURCE_QUEUE);
        registry.add("intake.bindings[0].mode", () -> "TRACKED");
        registry.add("intake.bindings[0].tracker.queue", () -> "MQ.HPS.MEMBERSHIP.TRACKER");
        registry.add("intake.bindings[0].tracker.body-mode", () -> "FULL_COPY");
        registry.add("intake.bindings[0].hdfs.base-path", () -> dataDir.toString());
        registry.add("intake.bindings[0].batch.size", () -> 5);
        registry.add("intake.bindings[0].batch.bytes", () -> 64 * 1024 * 1024);
        registry.add("intake.bindings[0].batch.interval-ms", () -> 400);
        registry.add("intake.bindings[0].listener-threads", () -> 1);
        registry.add("intake.bindings[0].backout.queue", () -> "MQ.HPS.MEMBERSHIP.BACKOUT");
        registry.add("intake.bindings[0].backout.threshold", () -> 5);
        registry.add("intake.bindings[0].degradation.strategy", () -> "BATCH_OF_ONE");
        registry.add("intake.hdfs.audit-base-path", () -> auditDir.toString());
        // This test lands in a temp directory, which is the local filesystem —
        // exactly what production mode refuses by default, since on a real
        // server it means the cluster config was never found. The acknowledgement
        // belongs here rather than in the guard; see LocalFilesystemGateTest.
        registry.add("intake.hdfs.allow-local-filesystem", () -> true);
        registry.add("intake.instance-id", () -> "rms-prod-sbt");
    }

    @TestConfiguration
    static class EmbeddedBrokerMqConfig {
        @Bean
        @Primary
        MqConnectionManager testMqConnectionManager() throws Exception {
            ActiveMQConnectionFactory factory =
                    new ActiveMQConnectionFactory("vm://rms-prod-sbt?broker.persistent=false");
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
    private ProductionMode productionMode;

    @Test
    void rmsStartsWithTheProductionProfileActive() {
        // The profile alone must enable production mode — MQ_INTAKE_PRODUCTION
        // is not set in this JVM
        assertThat(productionMode.isEnabled())
                .as("prod profile must enable production mode on its own")
                .isTrue();
        assertThat(productionMode.getReason()).contains("prod");

        // And the application actually came up under those stricter gates
        assertThat(runtimeManager.isRunning()).isTrue();
        assertThat(runtimeManager.getRuntime("rms")).isNotNull();
        assertThat(runtimeManager.getRuntime("rms").isRunning()).isTrue();
        assertThat(runtimeManager.getRuntime("rms").hasTrackerBuilder()).isTrue();
    }
}
