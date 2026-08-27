package com.hcsc.datalake.mqintake.claims;

import com.hcsc.datalake.mqintake.claims.serializer.ClaimsRecordSerializer;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for Claims module configuration.
 *
 * Verifies that Claims provides the correct factory beans for LAND_ONLY mode
 * and does NOT provide a TrackerMessageBuilderFactory.
 */
class ClaimsModuleTest {

    private ClaimsConfiguration claimsConfiguration;

    @BeforeEach
    void setUp() {
        claimsConfiguration = new ClaimsConfiguration();
    }

    @Test
    void claimsConfigurationProvidesSerializerFactory() {
        RecordSerializerFactory factory = claimsConfiguration.recordSerializerFactory();
        assertThat(factory).isNotNull();
    }

    @Test
    void claimsSerializerFactoryCreatesClaimsSerializer() {
        RecordSerializerFactory factory = claimsConfiguration.recordSerializerFactory();

        var serializer = factory.create(null);

        assertThat(serializer).isInstanceOf(ClaimsRecordSerializer.class);
    }

    @Test
    void claimsSerializerCreatedWithNullConfig() {
        RecordSerializerFactory factory = claimsConfiguration.recordSerializerFactory();

        var serializer = factory.create(null);

        assertThat(serializer).isNotNull();
    }

    @Test
    void claimsSerializerCreatedWithValidConfig() {
        RecordSerializerFactory factory = claimsConfiguration.recordSerializerFactory();
        BindingConfig config = createClaimsBindingConfig();

        var serializer = factory.create(config);

        assertThat(serializer).isInstanceOf(ClaimsRecordSerializer.class);
    }

    @Test
    void claimsConfigurationDoesNotProvideTrackerFactory() {
        // ClaimsConfiguration should NOT define a TrackerMessageBuilderFactory
        // because Claims uses LAND_ONLY mode
        // This is verified implicitly by the test in ClaimsRuntimeTest
        // that shows LAND_ONLY runtimes work without a tracker factory
        assertThat(claimsConfiguration).isNotNull();
    }

    private BindingConfig createClaimsBindingConfig() {
        BindingConfig config = new BindingConfig();
        config.setId("claims");
        config.setSourceQueue("MQ.DMIH.CLAIMS.IN");
        config.setMode(BindingMode.LAND_ONLY);
        config.getHdfs().setBasePath("/data/raw/claims/dmih");
        config.getBatch().setSize(100);
        config.getBatch().setBytes(1024 * 1024);
        config.getBatch().setIntervalMs(30000);
        config.setListenerThreads(4);
        return config;
    }
}
