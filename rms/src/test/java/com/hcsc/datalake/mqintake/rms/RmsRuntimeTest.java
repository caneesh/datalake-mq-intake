package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.TrackerBodyMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.runtime.BindingRuntime;
import com.hcsc.datalake.mqintake.core.runtime.BindingRuntimeFactory;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
import com.hcsc.datalake.mqintake.rms.serializer.RmsRecordSerializer;
import com.hcsc.datalake.mqintake.rms.tracker.RmsTrackerMessageBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.Connection;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RMS runtime creation.
 *
 * Verifies that the RMS module correctly creates TRACKED binding runtimes
 * with RmsRecordSerializer and RmsTrackerMessageBuilder.
 */
class RmsRuntimeTest {

    @TempDir
    Path tempDir;

    private FileSystem fileSystem;
    private Configuration hadoopConf;
    private Connection jmsConnection;

    @BeforeEach
    void setUp() throws Exception {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(hadoopConf);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        jmsConnection = factory.createConnection();
        jmsConnection.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (jmsConnection != null) jmsConnection.close();
        if (fileSystem != null) fileSystem.close();
    }

    @Test
    void rmsRuntimeCreatesTrackedBindingWithTrackerBuilder() throws Exception {
        RmsConfiguration rmsConfig = new RmsConfiguration();
        RecordSerializerFactory serializerFactory = rmsConfig.recordSerializerFactory();
        TrackerMessageBuilderFactory trackerFactory = rmsConfig.trackerMessageBuilderFactory();

        MetricsRegistry metricsRegistry = new MetricsRegistry();

        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, jmsConnection,
                serializerFactory, trackerFactory,
                metricsRegistry, null,
                "rms-instance", 1000);

        BindingConfig config = createRmsBindingConfig();
        BindingRuntime runtime = factory.create(config);

        assertThat(runtime).isNotNull();
        assertThat(runtime.getMode()).isEqualTo(BindingMode.TRACKED);
        assertThat(runtime.hasTrackerBuilder()).isTrue();
        assertThat(runtime.getLoopCount()).isEqualTo(4);

        runtime.stop(1000);
    }

    @Test
    void rmsSerializerFactoryCreatesRmsSerializer() {
        RmsConfiguration rmsConfig = new RmsConfiguration();
        RecordSerializerFactory factory = rmsConfig.recordSerializerFactory();

        BindingConfig config = createRmsBindingConfig();
        var serializer = factory.create(config);

        assertThat(serializer).isInstanceOf(RmsRecordSerializer.class);
    }

    @Test
    void rmsTrackerFactoryCreatesRmsTrackerBuilder() {
        RmsConfiguration rmsConfig = new RmsConfiguration();
        TrackerMessageBuilderFactory factory = rmsConfig.trackerMessageBuilderFactory();

        BindingConfig config = createRmsBindingConfig();
        var builder = factory.create(config);

        assertThat(builder).isInstanceOf(RmsTrackerMessageBuilder.class);
    }

    @Test
    void rmsTrackerBuilderUsesConfiguredFields() {
        RmsConfiguration rmsConfig = new RmsConfiguration();
        TrackerMessageBuilderFactory factory = rmsConfig.trackerMessageBuilderFactory();

        BindingConfig config = createRmsBindingConfig();
        config.setTrackerBodyMode(TrackerBodyMode.HEADER_ONLY);

        var trackerFields = new com.hcsc.datalake.mqintake.core.config.TrackerFields();
        trackerFields.setReportingSystem("CUSTOM/SYS");
        trackerFields.setSourceSystem("CUSTOM_SRC");
        trackerFields.setMessageStatus("PROCESSED");
        trackerFields.setDestinationStatus("DELIVERED");
        config.setTrackerFields(trackerFields);

        var builder = factory.create(config);

        assertThat(builder).isInstanceOf(RmsTrackerMessageBuilder.class);
    }

    @Test
    void rmsListenerThreadsCreatesCorrectNumberOfLoops() throws Exception {
        RmsConfiguration rmsConfig = new RmsConfiguration();

        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, jmsConnection,
                rmsConfig.recordSerializerFactory(),
                rmsConfig.trackerMessageBuilderFactory(),
                new MetricsRegistry(), null,
                "rms-instance", 1000);

        BindingConfig config = createRmsBindingConfig();
        config.setListenerThreads(8);

        BindingRuntime runtime = factory.create(config);

        assertThat(runtime.getLoopCount()).isEqualTo(8);
        assertThat(runtime.getLoops()).hasSize(8);

        runtime.stop(1000);
    }

    private BindingConfig createRmsBindingConfig() {
        BindingConfig config = new BindingConfig();
        config.setId("rms");
        config.setSourceQueue("MQ.HPS.MEMBERSHIP.IN");
        config.setMode(BindingMode.TRACKED);
        config.setTrackerQueue("MQ.HPS.MEMBERSHIP.TRACKER");
        config.setTrackerBodyMode(TrackerBodyMode.FULL_COPY);
        config.setHdfsBasePath(tempDir.toString());
        config.setBatchSize(100);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(30000);
        config.setListenerThreads(4);
        return config;
    }
}
