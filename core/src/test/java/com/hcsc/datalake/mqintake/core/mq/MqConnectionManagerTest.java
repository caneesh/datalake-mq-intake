package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MqConnectionManagerTest {

    @Test
    void hasConnectionReturnsTrueForConfiguredConnection() {
        MqConnectionConfig config = createConfig("primary", "localhost", 1414, "QM1", "CHANNEL1");

        Map<String, MqConnectionConfig> connections = new HashMap<>();
        connections.put("primary", config);

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());

        assertTrue(manager.hasConnection("primary"));
    }

    @Test
    void hasConnectionReturnsFalseForMissingConnection() {
        Map<String, MqConnectionConfig> connections = new HashMap<>();

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());

        assertFalse(manager.hasConnection("nonexistent"));
    }

    @Test
    void getConfigReturnsConfigurationForKnownConnection() {
        MqConnectionConfig config = createConfig("primary", "mq.example.com", 1415, "PRODQM", "APP.CHANNEL");

        Map<String, MqConnectionConfig> connections = new HashMap<>();
        connections.put("primary", config);

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());

        Optional<MqConnectionConfig> result = manager.getConfig("primary");

        assertTrue(result.isPresent());
        assertAll(
            () -> assertEquals("primary", result.get().getId()),
            () -> assertEquals("mq.example.com", result.get().getHost()),
            () -> assertEquals(1415, result.get().getPort()),
            () -> assertEquals("PRODQM", result.get().getQueueManager()),
            () -> assertEquals("APP.CHANNEL", result.get().getChannel())
        );
    }

    @Test
    void getConfigReturnsEmptyForUnknownConnection() {
        Map<String, MqConnectionConfig> connections = new HashMap<>();

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());

        Optional<MqConnectionConfig> result = manager.getConfig("unknown");

        assertTrue(result.isEmpty());
    }

    @Test
    void getConnectionThrowsForMissingConnection() {
        Map<String, MqConnectionConfig> connections = new HashMap<>();

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());

        MqConnectionManager.MqConnectionException ex = assertThrows(
            MqConnectionManager.MqConnectionException.class,
            () -> manager.getConnection("missing")
        );

        assertTrue(ex.getMessage().contains("No configuration found"));
        assertTrue(ex.getMessage().contains("missing"));
    }

    @Test
    void getConnectionThrowsAfterClose() {
        MqConnectionConfig config = createConfig("primary", "localhost", 1414, "QM1", "CHANNEL1");

        Map<String, MqConnectionConfig> connections = new HashMap<>();
        connections.put("primary", config);

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());
        manager.close();

        MqConnectionManager.MqConnectionException ex = assertThrows(
            MqConnectionManager.MqConnectionException.class,
            () -> manager.getConnection("primary")
        );

        assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void multipleConnectionsCanBeConfigured() {
        MqConnectionConfig primary = createConfig("primary", "mq-primary.internal", 1414, "QM1", "APP.CHANNEL1");
        MqConnectionConfig secondary = createConfig("secondary", "mq-secondary.internal", 1415, "QM2", "APP.CHANNEL2");

        Map<String, MqConnectionConfig> connections = new HashMap<>();
        connections.put("primary", primary);
        connections.put("secondary", secondary);

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());

        assertTrue(manager.hasConnection("primary"));
        assertTrue(manager.hasConnection("secondary"));
        assertFalse(manager.hasConnection("tertiary"));
    }

    @Test
    void constructorRequiresCredentialProvider() {
        Map<String, MqConnectionConfig> connections = new HashMap<>();

        assertThrows(NullPointerException.class, () ->
            new MqConnectionManager(connections, null)
        );
    }

    @Test
    void closeIsIdempotent() {
        MqConnectionConfig config = createConfig("primary", "localhost", 1414, "QM1", "CHANNEL1");

        Map<String, MqConnectionConfig> connections = new HashMap<>();
        connections.put("primary", config);

        MqConnectionManager manager = new MqConnectionManager(connections, emptyCredentialProvider());

        assertDoesNotThrow(() -> {
            manager.close();
            manager.close();
            manager.close();
        });
    }

    private MqConnectionConfig createConfig(String id, String host, int port, String queueManager, String channel) {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId(id);
        config.setHost(host);
        config.setPort(port);
        config.setQueueManager(queueManager);
        config.setChannel(channel);
        return config;
    }

    private CredentialProvider emptyCredentialProvider() {
        return ref -> Optional.empty();
    }
}
