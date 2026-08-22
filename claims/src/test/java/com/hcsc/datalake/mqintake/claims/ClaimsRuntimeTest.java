package com.hcsc.datalake.mqintake.claims;

import com.hcsc.datalake.mqintake.claims.serializer.ClaimsRecordSerializer;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.runtime.BindingRuntime;
import com.hcsc.datalake.mqintake.core.runtime.BindingRuntimeFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.Connection;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for Claims runtime creation.
 *
 * Verifies that the Claims module correctly creates LAND_ONLY binding runtimes
 * with ClaimsRecordSerializer and no TrackerMessageBuilder.
 */
class ClaimsRuntimeTest {

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
    void claimsRuntimeCreatesLandOnlyBindingWithoutTrackerBuilder() throws Exception {
        ClaimsConfiguration claimsConfig = new ClaimsConfiguration();
        RecordSerializerFactory serializerFactory = claimsConfig.recordSerializerFactory();

        MetricsRegistry metricsRegistry = new MetricsRegistry();

        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, jmsConnection,
                serializerFactory, null, // No tracker factory for LAND_ONLY
                metricsRegistry, null,
                "claims-instance", 1000);

        BindingConfig config = createClaimsBindingConfig();
        BindingRuntime runtime = factory.create(config);

        assertThat(runtime).isNotNull();
        assertThat(runtime.getMode()).isEqualTo(BindingMode.LAND_ONLY);
        assertThat(runtime.hasTrackerBuilder()).isFalse();
        assertThat(runtime.getLoopCount()).isEqualTo(4);

        runtime.stop(1000);
    }

    @Test
    void claimsSerializerFactoryCreatesClaimsSerializer() {
        ClaimsConfiguration claimsConfig = new ClaimsConfiguration();
        RecordSerializerFactory factory = claimsConfig.recordSerializerFactory();

        BindingConfig config = createClaimsBindingConfig();
        var serializer = factory.create(config);

        assertThat(serializer).isInstanceOf(ClaimsRecordSerializer.class);
    }

    @Test
    void claimsLandOnlyDoesNotRequireTrackerFactory() throws Exception {
        ClaimsConfiguration claimsConfig = new ClaimsConfiguration();

        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, jmsConnection,
                claimsConfig.recordSerializerFactory(),
                null, // Explicitly no tracker factory
                new MetricsRegistry(), null,
                "claims-instance", 1000);

        BindingConfig config = createClaimsBindingConfig();
        BindingRuntime runtime = factory.create(config);

        assertThat(runtime.hasTrackerBuilder()).isFalse();

        runtime.stop(1000);
    }

    @Test
    void claimsListenerThreadsCreatesCorrectNumberOfLoops() throws Exception {
        ClaimsConfiguration claimsConfig = new ClaimsConfiguration();

        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, jmsConnection,
                claimsConfig.recordSerializerFactory(),
                null,
                new MetricsRegistry(), null,
                "claims-instance", 1000);

        BindingConfig config = createClaimsBindingConfig();
        config.setListenerThreads(6);

        BindingRuntime runtime = factory.create(config);

        assertThat(runtime.getLoopCount()).isEqualTo(6);
        assertThat(runtime.getLoops()).hasSize(6);

        runtime.stop(1000);
    }

    @Test
    void claimsRuntimeStartsAndStopsCleanly() throws Exception {
        ClaimsConfiguration claimsConfig = new ClaimsConfiguration();

        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, jmsConnection,
                claimsConfig.recordSerializerFactory(),
                null,
                new MetricsRegistry(), null,
                "claims-instance", 1000);

        BindingConfig config = createClaimsBindingConfig();
        config.setListenerThreads(2);

        BindingRuntime runtime = factory.create(config);

        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.CREATED);

        runtime.start();
        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.RUNNING);

        Thread.sleep(100);

        runtime.stop(2000);
        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.STOPPED);
    }

    private BindingConfig createClaimsBindingConfig() {
        BindingConfig config = new BindingConfig();
        config.setId("claims");
        config.setSourceQueue("MQ.DMIH.CLAIMS.IN");
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath(tempDir.toString());
        config.setBatchSize(100);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(30000);
        config.setListenerThreads(4);
        return config;
    }
}
