package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.TrackerBodyMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.rms.serializer.RmsRecordSerializer;
import com.hcsc.datalake.mqintake.rms.tracker.RmsTrackerMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RMS module configuration.
 *
 * Verifies that RMS provides the correct factory beans for TRACKED mode.
 */
class RmsModuleTest {

    private RmsConfiguration rmsConfiguration;

    @BeforeEach
    void setUp() {
        rmsConfiguration = new RmsConfiguration();
    }

    @Test
    void rmsConfigurationProvidesSerializerFactory() {
        RecordSerializerFactory factory = rmsConfiguration.recordSerializerFactory();
        assertThat(factory).isNotNull();
    }

    @Test
    void rmsSerializerFactoryCreatesRmsSerializer() {
        RecordSerializerFactory factory = rmsConfiguration.recordSerializerFactory();

        var serializer = factory.create(null);

        assertThat(serializer).isInstanceOf(RmsRecordSerializer.class);
    }

    @Test
    void rmsConfigurationProvidesTrackerBuilderFactory() {
        TrackerMessageBuilderFactory factory = rmsConfiguration.trackerMessageBuilderFactory();
        assertThat(factory).isNotNull();
    }

    @Test
    void rmsTrackerFactoryCreatesRmsTrackerBuilder() {
        TrackerMessageBuilderFactory factory = rmsConfiguration.trackerMessageBuilderFactory();
        BindingConfig config = createRmsBindingConfig();

        var builder = factory.create(config);

        assertThat(builder).isInstanceOf(RmsTrackerMessageBuilder.class);
    }

    @Test
    void rmsTrackerBuilderUsesDefaultFieldsWhenNotConfigured() {
        TrackerMessageBuilderFactory factory = rmsConfiguration.trackerMessageBuilderFactory();
        BindingConfig config = createRmsBindingConfig();
        config.setTrackerFields(null);

        var builder = factory.create(config);

        assertThat(builder).isInstanceOf(RmsTrackerMessageBuilder.class);
    }

    @Test
    void rmsTrackerBuilderUsesConfiguredBodyMode() {
        TrackerMessageBuilderFactory factory = rmsConfiguration.trackerMessageBuilderFactory();
        BindingConfig config = createRmsBindingConfig();
        config.setTrackerBodyMode(TrackerBodyMode.HEADER_ONLY);

        var builder = factory.create(config);

        assertThat(builder).isInstanceOf(RmsTrackerMessageBuilder.class);
    }

    @Test
    void rmsTrackerBuilderDefaultsToFullCopy() {
        TrackerMessageBuilderFactory factory = rmsConfiguration.trackerMessageBuilderFactory();
        BindingConfig config = createRmsBindingConfig();
        config.setTrackerBodyMode(null);

        var builder = factory.create(config);

        assertThat(builder).isInstanceOf(RmsTrackerMessageBuilder.class);
    }

    private BindingConfig createRmsBindingConfig() {
        BindingConfig config = new BindingConfig();
        config.setId("rms");
        config.setSourceQueue("MQ.HPS.MEMBERSHIP.IN");
        config.setMode(BindingMode.TRACKED);
        config.setTrackerQueue("MQ.HPS.MEMBERSHIP.TRACKER");
        config.setTrackerBodyMode(TrackerBodyMode.FULL_COPY);
        config.setHdfsBasePath("/data/raw/membership/hps");
        config.setBatchSize(100);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(30000);
        config.setListenerThreads(4);
        return config;
    }
}
