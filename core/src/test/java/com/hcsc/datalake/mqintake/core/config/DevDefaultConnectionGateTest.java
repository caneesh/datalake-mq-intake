package com.hcsc.datalake.mqintake.core.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The production gate against dev-placeholder MQ connection values.
 *
 * <p>The yaml defaults ({@code localhost}/{@code QM1}/{@code DEV.APP.SVRCONN})
 * are non-blank, so the sanity rules pass them — a production manifest that
 * forgets MQ_HOST/MQ_QUEUE_MANAGER/MQ_CHANNEL used to boot pointing at a dev
 * queue manager over IBM's unauthenticated dev channel instead of failing.
 */
class DevDefaultConnectionGateTest {

    private MqConnectionConfig connection(String host, String qm, String channel) {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId("primary");
        config.setHost(host);
        config.setQueueManager(qm);
        config.setChannel(channel);
        return config;
    }

    @Test
    void productionModeRefusesEveryDevPlaceholder() {
        assertThatThrownBy(() -> DevDefaultConnectionGate.failOnDevDefaults(
                ProductionMode.enabled(),
                Map.of("primary", connection("localhost", "QM1", "DEV.APP.SVRCONN"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("primary.host = localhost")
                .hasMessageContaining("primary.queue-manager = QM1")
                .hasMessageContaining("primary.channel = DEV.APP.SVRCONN")
                .hasMessageContaining("MQ_HOST");
    }

    @Test
    void oneDevPlaceholderIsEnoughToRefuse() {
        // Real host and QM, but the channel default leaked through — still a
        // manifest error, still refused.
        assertThatThrownBy(() -> DevDefaultConnectionGate.failOnDevDefaults(
                ProductionMode.enabled(),
                Map.of("primary", connection("mq.prod.internal", "PRODQM", "DEV.APP.SVRCONN"))))
                .hasMessageContaining("primary.channel");
    }

    @Test
    void productionModeAcceptsRealValues() {
        assertThatCode(() -> DevDefaultConnectionGate.failOnDevDefaults(
                ProductionMode.enabled(),
                Map.of("primary", connection("mq.prod.internal", "PRODQM", "PROD.APP.SVRCONN"))))
                .doesNotThrowAnyException();
    }

    @Test
    void devPlaceholdersAreFineOutsideProductionMode() {
        assertThatCode(() -> DevDefaultConnectionGate.failOnDevDefaults(
                ProductionMode.disabled(),
                Map.of("primary", connection("localhost", "QM1", "DEV.APP.SVRCONN"))))
                .doesNotThrowAnyException();
    }
}
