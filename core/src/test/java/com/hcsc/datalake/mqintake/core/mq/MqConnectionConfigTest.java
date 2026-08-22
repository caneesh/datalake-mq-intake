package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MqConnectionConfigTest {

    @Test
    void configuresHost() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setHost("mq.example.com");

        assertEquals("mq.example.com", config.getHost());
    }

    @Test
    void configuresPort() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setPort(1415);

        assertEquals(1415, config.getPort());
    }

    @Test
    void defaultPortIs1414() {
        MqConnectionConfig config = new MqConnectionConfig();

        assertEquals(1414, config.getPort());
    }

    @Test
    void configuresQueueManager() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setQueueManager("QM1");

        assertEquals("QM1", config.getQueueManager());
    }

    @Test
    void configuresChannel() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setChannel("DEV.APP.SVRCONN");

        assertEquals("DEV.APP.SVRCONN", config.getChannel());
    }

    @Test
    void configuresTransportType() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setTransportType("BINDINGS");

        assertEquals("BINDINGS", config.getTransportType());
    }

    @Test
    void defaultTransportTypeIsClient() {
        MqConnectionConfig config = new MqConnectionConfig();

        assertEquals("CLIENT", config.getTransportType());
    }

    @Test
    void configuresCredentialRef() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setCredentialRef("env:MQ_USER,MQ_PASS");

        assertEquals("env:MQ_USER,MQ_PASS", config.getCredentialRef());
    }

    @Test
    void configuresReceiveTimeout() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setReceiveTimeoutMs(2000);

        assertEquals(2000, config.getReceiveTimeoutMs());
    }

    @Test
    void defaultReceiveTimeoutIs1000() {
        MqConnectionConfig config = new MqConnectionConfig();

        assertEquals(1000, config.getReceiveTimeoutMs());
    }

    @Test
    void configuresReconnectAttempts() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setReconnectAttempts(5);

        assertEquals(5, config.getReconnectAttempts());
    }

    @Test
    void defaultReconnectAttemptsIs3() {
        MqConnectionConfig config = new MqConnectionConfig();

        assertEquals(3, config.getReconnectAttempts());
    }

    @Test
    void configuresReconnectDelay() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setReconnectDelayMs(10000);

        assertEquals(10000, config.getReconnectDelayMs());
    }

    @Test
    void defaultReconnectDelayIs5000() {
        MqConnectionConfig config = new MqConnectionConfig();

        assertEquals(5000, config.getReconnectDelayMs());
    }

    @Test
    void configuresConnectionId() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId("primary");

        assertEquals("primary", config.getId());
    }

    @Test
    void fullConfigurationCapture() {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId("prod-mq");
        config.setHost("mq-prod.internal");
        config.setPort(1414);
        config.setQueueManager("PRODQM");
        config.setChannel("APP.PROD.SVRCONN");
        config.setTransportType("CLIENT");
        config.setCredentialRef("env:PROD_MQ_USER,PROD_MQ_PASS");
        config.setReceiveTimeoutMs(500);
        config.setReconnectAttempts(5);
        config.setReconnectDelayMs(3000);

        assertAll(
            () -> assertEquals("prod-mq", config.getId()),
            () -> assertEquals("mq-prod.internal", config.getHost()),
            () -> assertEquals(1414, config.getPort()),
            () -> assertEquals("PRODQM", config.getQueueManager()),
            () -> assertEquals("APP.PROD.SVRCONN", config.getChannel()),
            () -> assertEquals("CLIENT", config.getTransportType()),
            () -> assertEquals("env:PROD_MQ_USER,PROD_MQ_PASS", config.getCredentialRef()),
            () -> assertEquals(500, config.getReceiveTimeoutMs()),
            () -> assertEquals(5, config.getReconnectAttempts()),
            () -> assertEquals(3000, config.getReconnectDelayMs())
        );
    }
}
