package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MqConfigurationTest {

    @Test
    void bindingMustSpecifyMqConnection() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", null, BindingMode.LAND_ONLY);
        props.setBindings(List.of(binding));

        MqConnectionManager manager = createManager(Map.of());

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        IllegalStateException ex = assertThrows(IllegalStateException.class, validator::validate);

        assertTrue(ex.getMessage().contains("test"));
        assertTrue(ex.getMessage().contains("mq-connection"));
    }

    @Test
    void bindingWithBlankMqConnectionFails() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", "   ", BindingMode.LAND_ONLY);
        props.setBindings(List.of(binding));

        MqConnectionManager manager = createManager(Map.of());

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        IllegalStateException ex = assertThrows(IllegalStateException.class, validator::validate);

        assertTrue(ex.getMessage().contains("mq-connection"));
    }

    @Test
    void bindingReferencingUnknownConnectionFails() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", "nonexistent", BindingMode.LAND_ONLY);
        props.setBindings(List.of(binding));

        MqConnectionConfig config = createMqConfig("primary", "localhost", 1414);
        MqConnectionManager manager = createManager(Map.of("primary", config));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        IllegalStateException ex = assertThrows(IllegalStateException.class, validator::validate);

        assertTrue(ex.getMessage().contains("test"));
        assertTrue(ex.getMessage().contains("nonexistent"));
        assertTrue(ex.getMessage().contains("unknown"));
    }

    @Test
    void bindingWithValidMqConnectionPasses() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", "primary", BindingMode.LAND_ONLY);
        props.setBindings(List.of(binding));

        MqConnectionConfig config = createMqConfig("primary", "localhost", 1414);
        MqConnectionManager manager = createManager(Map.of("primary", config));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        assertDoesNotThrow(validator::validate);
    }

    @Test
    void trackedBindingRequiresTrackerQueue() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", "primary", BindingMode.TRACKED);
        binding.setTrackerQueue(null);
        props.setBindings(List.of(binding));

        MqConnectionConfig config = createMqConfig("primary", "localhost", 1414);
        MqConnectionManager manager = createManager(Map.of("primary", config));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        IllegalStateException ex = assertThrows(IllegalStateException.class, validator::validate);

        assertTrue(ex.getMessage().contains("TRACKED"));
        assertTrue(ex.getMessage().contains("tracker-queue"));
    }

    @Test
    void trackedBindingWithBlankTrackerQueueFails() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", "primary", BindingMode.TRACKED);
        binding.setTrackerQueue("   ");
        props.setBindings(List.of(binding));

        MqConnectionConfig config = createMqConfig("primary", "localhost", 1414);
        MqConnectionManager manager = createManager(Map.of("primary", config));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        IllegalStateException ex = assertThrows(IllegalStateException.class, validator::validate);

        assertTrue(ex.getMessage().contains("tracker-queue"));
    }

    @Test
    void trackedBindingWithTrackerQueuePasses() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", "primary", BindingMode.TRACKED);
        binding.setTrackerQueue("TRACKER.Q");
        props.setBindings(List.of(binding));

        MqConnectionConfig config = createMqConfig("primary", "localhost", 1414);
        MqConnectionManager manager = createManager(Map.of("primary", config));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        assertDoesNotThrow(validator::validate);
    }

    @Test
    void landOnlyBindingDoesNotRequireTrackerQueue() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding = createBinding("test", "primary", BindingMode.LAND_ONLY);
        binding.setTrackerQueue(null);
        props.setBindings(List.of(binding));

        MqConnectionConfig config = createMqConfig("primary", "localhost", 1414);
        MqConnectionManager manager = createManager(Map.of("primary", config));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        assertDoesNotThrow(validator::validate);
    }

    @Test
    void multipleBindingsCanShareSameConnection() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding1 = createBinding("binding1", "primary", BindingMode.LAND_ONLY);
        BindingConfig binding2 = createBinding("binding2", "primary", BindingMode.LAND_ONLY);
        props.setBindings(List.of(binding1, binding2));

        MqConnectionConfig config = createMqConfig("primary", "localhost", 1414);
        MqConnectionManager manager = createManager(Map.of("primary", config));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        assertDoesNotThrow(validator::validate);
    }

    @Test
    void multipleBindingsCanUseDifferentConnections() {
        IntakeProperties props = new IntakeProperties();
        BindingConfig binding1 = createBinding("binding1", "conn1", BindingMode.LAND_ONLY);
        BindingConfig binding2 = createBinding("binding2", "conn2", BindingMode.LAND_ONLY);
        props.setBindings(List.of(binding1, binding2));

        MqConnectionConfig config1 = createMqConfig("conn1", "host1", 1414);
        MqConnectionConfig config2 = createMqConfig("conn2", "host2", 1415);
        MqConnectionManager manager = createManager(Map.of("conn1", config1, "conn2", config2));

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        assertDoesNotThrow(validator::validate);
    }

    @Test
    void noBindingsIsValid() {
        IntakeProperties props = new IntakeProperties();
        props.setBindings(new ArrayList<>());

        MqConnectionManager manager = createManager(Map.of());

        MqConfiguration.BindingConfigurationValidator validator =
            new MqConfiguration.BindingConfigurationValidator(props, manager);

        assertDoesNotThrow(validator::validate);
    }

    @Test
    void duplicateConnectionIdDetection() {
        MqConfiguration config = new MqConfiguration();

        MqConnectionConfig conn1 = createMqConfig("primary", "host1", 1414);
        MqConnectionConfig conn2 = createMqConfig("primary", "host2", 1415);

        Map<String, MqConnectionConfig> connections = new HashMap<>();
        connections.put("key1", conn1);
        connections.put("key2", conn2);

        CredentialProvider credentialProvider = ref -> Optional.empty();

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            config.mqConnectionManager(createProps(connections), credentialProvider)
        );

        assertTrue(ex.getMessage().contains("Duplicate"));
        assertTrue(ex.getMessage().contains("primary"));
    }

    @Test
    void mapKeyUsedAsIdWhenIdNotSet() {
        MqConfiguration config = new MqConfiguration();

        MqConnectionConfig conn = new MqConnectionConfig();
        conn.setHost("localhost");
        conn.setPort(1414);
        conn.setQueueManager("QM1");
        conn.setChannel("CHANNEL1");

        Map<String, MqConnectionConfig> connections = new HashMap<>();
        connections.put("myconnection", conn);

        CredentialProvider credentialProvider = ref -> Optional.empty();

        MqConnectionManager manager = config.mqConnectionManager(createProps(connections), credentialProvider);

        assertTrue(manager.hasConnection("myconnection"));
        assertEquals("myconnection", manager.getConfig("myconnection").get().getId());
    }

    private BindingConfig createBinding(String id, String mqConnection, BindingMode mode) {
        BindingConfig binding = new BindingConfig();
        binding.setId(id);
        binding.setMqConnection(mqConnection);
        binding.setMode(mode);
        binding.setSourceQueue("TEST.Q");
        binding.setHdfsBasePath("/data/test");
        binding.setBatchSize(100);
        binding.setBatchBytes(1024 * 1024);
        binding.setBatchIntervalMs(1000);
        return binding;
    }

    private MqConnectionConfig createMqConfig(String id, String host, int port) {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId(id);
        config.setHost(host);
        config.setPort(port);
        config.setQueueManager("QM1");
        config.setChannel("CHANNEL1");
        return config;
    }

    private MqConnectionManager createManager(Map<String, MqConnectionConfig> connections) {
        return new MqConnectionManager(connections, ref -> Optional.empty());
    }

    private IntakeProperties createProps(Map<String, MqConnectionConfig> connections) {
        IntakeProperties props = new IntakeProperties();
        props.setMqConnections(new HashMap<>(connections));
        return props;
    }
}
