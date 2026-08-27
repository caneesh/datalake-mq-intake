package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingConfigValidator;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Application startup is transactional across bindings.
 *
 * <p>Previously, if binding C failed after A and B had started, the exception
 * propagated but A and B were left consuming — threads polling MQ, landing data
 * and committing, inside an application whose startup had failed. Spring did
 * not clean that up either: {@code running} is still false at that point, so
 * {@code DefaultLifecycleProcessor} does not consider the bean started and
 * never calls {@code stop()}. The result was a half-live service with no owner.
 */
class PartialStartupRollbackTest {

    /** Non-null so SerializerValidator can log its class name. */
    private static final com.hcsc.datalake.mqintake.core.serializer.RecordSerializer
            TRIVIAL_SERIALIZER = new com.hcsc.datalake.mqintake.core.serializer.RecordSerializer() {
        @Override
        public SerializedRecord serialize(javax.jms.Message message,
                com.hcsc.datalake.mqintake.core.serializer.RecordMetadata metadata) {
            return new SerializedRecord(new org.apache.hadoop.io.LongWritable(0),
                    new org.apache.hadoop.io.Text(""));
        }

        @Override
        public Class<? extends org.apache.hadoop.io.Writable> getKeyClass() {
            return org.apache.hadoop.io.LongWritable.class;
        }

        @Override
        public Class<? extends org.apache.hadoop.io.Writable> getValueClass() {
            return org.apache.hadoop.io.Text.class;
        }
    };

    private FileSystem fileSystem;
    private Configuration conf;
    private java.nio.file.Path dataDir;
    private java.nio.file.Path auditDir;
    private final List<ExecutorService> executors = new ArrayList<>();
    private final List<CountDownLatch> latches = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);
        dataDir = Files.createTempDirectory("rollback-data");
        auditDir = Files.createTempDirectory("rollback-audit");
    }

    @AfterEach
    void tearDown() throws Exception {
        latches.forEach(CountDownLatch::countDown);
        for (ExecutorService e : executors) {
            e.shutdownNow();
            e.awaitTermination(5, TimeUnit.SECONDS);
        }
        fileSystem.delete(new Path(dataDir.toString()), true);
        fileSystem.delete(new Path(auditDir.toString()), true);
    }

    @Test
    void allBindingsStartingSuccessfullyLeavesThemRunning() {
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = manager(properties("a", "b", "c"), null, created);

        assertThatCode(manager::start).doesNotThrowAnyException();

        assertThat(manager.isRunning()).isTrue();
        assertThat(created).hasSize(3);
        assertThat(created).allSatisfy(r ->
                assertThat(r.getState()).isEqualTo(BindingRuntime.State.RUNNING));
    }

    @Test
    void firstBindingFailingLeavesNothingRunning() {
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = manager(properties("a", "b", "c"), "a", created);

        assertThatThrownBy(manager::start).isInstanceOf(RuntimeException.class);

        assertThat(manager.isRunning()).isFalse();
        // The loop does not stop at the first failure — it collects errors and
        // continues — so b and c still started, and both must be rolled back.
        assertThat(created).hasSize(2);
        assertThat(created).allSatisfy(r ->
                assertThat(r.getState()).isEqualTo(BindingRuntime.State.STOPPED));
        assertThat(manager.getRuntime("b")).isNull();
    }

    @Test
    void bindingFailingAfterOthersStartedStopsTheOnesAlreadyRunning() {
        // The case that mattered: A and B are live when C fails.
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = manager(properties("a", "b", "c"), "c", created);

        assertThatThrownBy(manager::start)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("c");

        assertThat(created).hasSize(2);
        assertThat(created).allSatisfy(r ->
                assertThat(r.getState())
                        .as("binding started before the failure must be stopped")
                        .isEqualTo(BindingRuntime.State.STOPPED));

        assertThat(manager.isRunning()).isFalse();
        assertThat(manager.getRuntime("a")).isNull();
        assertThat(manager.getRuntime("b")).isNull();
    }

    @Test
    void middleBindingFailingStopsTheEarlierOne() {
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = manager(properties("a", "b", "c"), "b", created);

        assertThatThrownBy(manager::start).isInstanceOf(RuntimeException.class);

        // a and c started; b failed. Both survivors must be stopped.
        assertThat(created).hasSize(2);
        assertThat(created).allSatisfy(r ->
                assertThat(r.getState()).isEqualTo(BindingRuntime.State.STOPPED));
    }

    @Test
    void originalFailureIsPreservedNotReplacedByCleanupNoise() {
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = manager(properties("a", "c"), "c", created);

        assertThatThrownBy(manager::start)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Startup failed")
                .hasMessageContaining("c");
    }

    @Test
    void cleanupIsIdempotentSoALaterStopIsSafe() {
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = manager(properties("a", "c"), "c", created);

        assertThatThrownBy(manager::start).isInstanceOf(RuntimeException.class);

        // Spring may still call stop() while closing the failed context
        assertThatCode(manager::stop).doesNotThrowAnyException();
        assertThatCode(manager::stop).doesNotThrowAnyException();
        assertThat(created.get(0).getState()).isEqualTo(BindingRuntime.State.STOPPED);
    }

    @Test
    void aFailureAfterAllBindingsStartedStillStopsThem() {
        // The gap the review found: createAndStartRuntimes succeeded, then a
        // later startup step threw. running stays false so Spring never calls
        // stop() — without this rollback the bindings were left consuming
        // inside an application whose startup had failed.
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = new FailsAfterBindingsStart(
                properties("a", "b"), created);

        assertThatThrownBy(manager::start)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("reconciliation exploded");

        assertThat(created).hasSize(2);
        assertThat(created).allSatisfy(r ->
                assertThat(r.getState())
                        .as("bindings that started must be stopped by the late rollback")
                        .isEqualTo(BindingRuntime.State.STOPPED));
        assertThat(manager.isRunning()).isFalse();
        assertThat(manager.getRuntime("a")).isNull();
    }

    /** Every binding starts fine; the step after them throws. */
    private class FailsAfterBindingsStart extends IntakeRuntimeManager {
        private final List<BindingRuntime> created;
        private final IntakeProperties props;

        FailsAfterBindingsStart(IntakeProperties props, List<BindingRuntime> created) {
            super(props, fileSystem, conf, mock(MqConnectionManager.class),
                    config -> TRIVIAL_SERIALIZER,
                    new BindingConfigValidator(
                            path -> com.hcsc.datalake.mqintake.core.config.HdfsPathValidator
                                    .PathValidationResult.success()),
                    new BindingHealthManager(),
                    ProductionMode.disabled(),
                    com.hcsc.datalake.mqintake.core.config.InstanceId.of("rollback-test"), null);
            this.props = props;
            this.created = created;
        }

        @Override
        void initializeRuntimeFactory() {
            setRuntimeFactoryForTest(new BindingRuntimeFactory(
                    fileSystem, conf, mock(MqConnectionManager.class),
                    config -> TRIVIAL_SERIALIZER, null,
                    new com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry(),
                    new BindingHealthManager(), null, "test") {
                @Override
                public BindingRuntime create(BindingConfig binding)
                        throws BindingRuntimeCreationException {
                    BindingRuntime runtime = blockingRuntime(binding);
                    created.add(runtime);
                    return runtime;
                }
            });
        }

        @Override
        void startReconciliation() {
            throw new IllegalStateException("reconciliation exploded");
        }
    }

    // --- harness ---

    private IntakeProperties properties(String... bindingIds) {
        IntakeProperties props = new IntakeProperties();
        props.setInstanceId("rollback-test");
        props.getHdfs().setAuditBasePath(auditDir.toString());
        // The stub listeners block until released, so the default 30s drain
        // would be spent in full on every rollback.
        props.getShutdown().setDrainTimeoutMs(200);

        Map<String, MqConnectionConfig> connections = new LinkedHashMap<>();
        MqConnectionConfig primary = new MqConnectionConfig();
        primary.setId("primary");
        // MqConnectionSanityRule now validates these at startup
        primary.setHost("test-host");
        primary.setQueueManager("QM1");
        primary.setChannel("TEST.SVRCONN");
        connections.put("primary", primary);
        props.setMqConnections(connections);

        List<BindingConfig> bindings = new ArrayList<>();
        for (String id : bindingIds) {
            BindingConfig binding = new BindingConfig();
            binding.setId(id);
            binding.setMqConnection("primary");
            binding.setSourceQueue("QUEUE." + id.toUpperCase());
            binding.setMode(BindingMode.LAND_ONLY);
            String base = dataDir.toString() + "/" + id;
            try {
                Files.createDirectories(java.nio.file.Paths.get(base));
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
            binding.setHdfsBasePath(base);
            binding.setBatchSize(10);
            binding.setBatchBytes(1024 * 1024);
            binding.setBatchIntervalMs(1000);
            binding.setListenerThreads(1);
            bindings.add(binding);
        }
        props.setBindings(bindings);
        return props;
    }

    /**
     * Builds a manager whose runtime factory fails for one chosen binding.
     * The real factory needs a live MQ connection manager, which would put the
     * rollback path out of reach.
     */
    private IntakeRuntimeManager manager(IntakeProperties props, String failingBindingId,
                                         List<BindingRuntime> created) {
        return new IntakeRuntimeManager(
                props, fileSystem, conf, mock(MqConnectionManager.class),
                config -> TRIVIAL_SERIALIZER,
                new BindingConfigValidator(
                        path -> com.hcsc.datalake.mqintake.core.config.HdfsPathValidator
                                .PathValidationResult.success()),
                new BindingHealthManager(),
                ProductionMode.disabled(),
                com.hcsc.datalake.mqintake.core.config.InstanceId.of("rollback-test"), null) {
            @Override
            void initializeRuntimeFactory() {
                setRuntimeFactoryForTest(new BindingRuntimeFactory(
                        fileSystem, conf, mock(MqConnectionManager.class),
                        config -> TRIVIAL_SERIALIZER, null,
                        new com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry(),
                        new BindingHealthManager(), null, "test") {
                    @Override
                    public BindingRuntime create(BindingConfig binding)
                            throws BindingRuntimeCreationException {
                        if (binding.getId().equals(failingBindingId)) {
                            throw new BindingRuntimeCreationException(
                                    "simulated failure for binding '" + binding.getId() + "'");
                        }
                        BindingRuntime runtime = blockingRuntime(binding);
                        created.add(runtime);
                        return runtime;
                    }
                });
            }
        };
    }

    /** A runtime whose listener blocks until released, so it is genuinely alive. */
    private BindingRuntime blockingRuntime(BindingConfig binding) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executors.add(executor);
        CountDownLatch latch = new CountDownLatch(1);
        latches.add(latch);

        List<TransactedReceiveLoop> loops = List.of(new TransactedReceiveLoop(
                binding, mock(javax.jms.Connection.class), null, null, null, null,
                null, null, null, "test", 100));

        return new BindingRuntime(binding, loops, executor, false, null) {
            @Override
            List<Runnable> submittableTasks() {
                return List.of(() -> {
                    try {
                        latch.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        };
    }
}
