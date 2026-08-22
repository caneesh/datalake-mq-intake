package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.serializer.TestRecordSerializer;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Session;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for BindingRuntimeFactory.
 */
class BindingRuntimeFactoryTest {

    @TempDir
    Path tempDir;

    private FileSystem fileSystem;
    private Configuration hadoopConf;
    private Connection jmsConnection;
    private MetricsRegistry metricsRegistry;
    private RecordSerializerFactory serializerFactory;
    private TrackerMessageBuilderFactory trackerBuilderFactory;

    @BeforeEach
    void setUp() throws Exception {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(hadoopConf);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        jmsConnection = factory.createConnection();
        jmsConnection.start();

        metricsRegistry = new MetricsRegistry();
        serializerFactory = config -> new TestRecordSerializer();
        trackerBuilderFactory = config -> (Session session, Message source) -> Optional.empty();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (jmsConnection != null) jmsConnection.close();
        if (fileSystem != null) fileSystem.close();
    }

    @Test
    void createsLandOnlyRuntimeWithoutTrackerBuilder() throws Exception {
        BindingRuntimeFactory factory = createFactory(null);

        BindingConfig config = createLandOnlyConfig();

        BindingRuntime runtime = factory.create(config);

        assertThat(runtime).isNotNull();
        assertThat(runtime.getMode()).isEqualTo(BindingMode.LAND_ONLY);
        assertThat(runtime.hasTrackerBuilder()).isFalse();
        assertThat(runtime.getLoopCount()).isEqualTo(2);

        runtime.stop(1000);
    }

    @Test
    void createsTrackedRuntimeWithTrackerBuilder() throws Exception {
        BindingRuntimeFactory factory = createFactory(trackerBuilderFactory);

        BindingConfig config = createTrackedConfig();

        BindingRuntime runtime = factory.create(config);

        assertThat(runtime).isNotNull();
        assertThat(runtime.getMode()).isEqualTo(BindingMode.TRACKED);
        assertThat(runtime.hasTrackerBuilder()).isTrue();
        assertThat(runtime.getLoopCount()).isEqualTo(3);

        runtime.stop(1000);
    }

    @Test
    void trackedBindingWithoutTrackerFactoryThrows() {
        BindingRuntimeFactory factory = createFactory(null);

        BindingConfig config = createTrackedConfig();

        assertThatThrownBy(() -> factory.create(config))
                .isInstanceOf(BindingRuntimeFactory.BindingRuntimeCreationException.class)
                .hasMessageContaining("TRACKED")
                .hasMessageContaining("TrackerMessageBuilderFactory");
    }

    @Test
    void noJmsConnectionThrows() throws Exception {
        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, null, // no JMS connection
                serializerFactory, trackerBuilderFactory,
                metricsRegistry, null, "test-instance", 1000);

        BindingConfig config = createLandOnlyConfig();

        assertThatThrownBy(() -> factory.create(config))
                .isInstanceOf(BindingRuntimeFactory.BindingRuntimeCreationException.class)
                .hasMessageContaining("No JMS connection");
    }

    @Test
    void listenerThreadsNCreatesNLoops() throws Exception {
        BindingRuntimeFactory factory = createFactory(null);

        BindingConfig config = createLandOnlyConfig();
        config.setListenerThreads(5);

        BindingRuntime runtime = factory.create(config);

        assertThat(runtime.getLoopCount()).isEqualTo(5);

        runtime.stop(1000);
    }

    @Test
    void metricsRegisteredForBinding() throws Exception {
        BindingRuntimeFactory factory = createFactory(null);

        BindingConfig config = createLandOnlyConfig();
        config.setId("metrics-test-binding");

        factory.create(config);

        assertThat(metricsRegistry.hasBinding("metrics-test-binding")).isTrue();
        assertThat(metricsRegistry.forBinding("metrics-test-binding")).isNotNull();
    }

    @Test
    void poisonHandlerCreatedWhenBackoutQueueConfigured() throws Exception {
        BindingRuntimeFactory factory = createFactory(null);

        BindingConfig config = createLandOnlyConfig();
        config.setBackoutQueue("TEST.BACKOUT");
        config.setBackoutThreshold(5);

        BindingRuntime runtime = factory.create(config);

        assertThat(runtime).isNotNull();

        runtime.stop(1000);
    }

    private BindingRuntimeFactory createFactory(TrackerMessageBuilderFactory trackerFactory) {
        return new BindingRuntimeFactory(
                fileSystem, hadoopConf, jmsConnection,
                serializerFactory, trackerFactory,
                metricsRegistry, null,
                "test-instance", 1000);
    }

    private BindingConfig createLandOnlyConfig() {
        BindingConfig config = new BindingConfig();
        config.setId("test-land-only");
        config.setSourceQueue("TEST.SOURCE");
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath(tempDir.toString());
        config.setBatchSize(100);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(30000);
        config.setListenerThreads(2);
        return config;
    }

    private BindingConfig createTrackedConfig() {
        BindingConfig config = new BindingConfig();
        config.setId("test-tracked");
        config.setSourceQueue("TEST.SOURCE");
        config.setMode(BindingMode.TRACKED);
        config.setTrackerQueue("TEST.TRACKER");
        config.setHdfsBasePath(tempDir.toString());
        config.setBatchSize(100);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(30000);
        config.setListenerThreads(3);
        return config;
    }
}
