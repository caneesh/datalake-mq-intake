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
import com.hcsc.datalake.mqintake.core.reconciliation.ReconciliationScheduler;
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
import java.util.function.Supplier;

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
        IntakeRuntimeManager manager = manager(
                properties("a", "b"),
                () -> factoryOf(binding -> {
                    BindingRuntime runtime = blockingRuntime(binding);
                    created.add(runtime);
                    return runtime;
                }),
                () -> {
                    throw new IllegalStateException("reconciliation exploded");
                });

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

    @Test
    void aStartLevelFailureRollsBackAndLeavesNoFailedRuntimeInTheMap() {
        // The other rollback tests fail at factory-create time. This one fails
        // INSIDE BindingRuntime.start() — the case the review found uncovered:
        // the runtime used to be registered in the map before start(), so a
        // start-level failure left a FAILED runtime in the map that stop()'s
        // RUNNING→STOPPING guard could never reach.
        List<BindingRuntime> created = new ArrayList<>();
        IntakeRuntimeManager manager = managerWithStartFailure(properties("a", "b"), "b", created);

        assertThatThrownBy(manager::start).isInstanceOf(RuntimeException.class);

        assertThat(manager.getRuntime("b"))
                .as("a runtime whose start() failed must not linger in the map")
                .isNull();
        assertThat(manager.getRuntimes()).isEmpty();
        assertThat(created).hasSize(1); // only "a" got a real runtime
        assertThat(created.get(0).getState())
                .as("the binding that DID start must be rolled back")
                .isEqualTo(BindingRuntime.State.STOPPED);
        assertThat(manager.isRunning()).isFalse();
    }

    /** Like {@link #manager}, but the chosen binding fails inside start(). */
    @Test
    void theMetricsRegistryReadsKerberosFailuresFromTheLiveManager() {
        // The gauge published mq_intake_kerberos_relogin_failures from
        // MetricsRegistry's own counter, whose recordKerberosReloginFailure()
        // has no production caller — so it read 0.0 forever while real
        // failures accumulated on KerberosManager. This pins the wiring that
        // joins them.
        com.hcsc.datalake.mqintake.core.security.KerberosManager manager =
                new com.hcsc.datalake.mqintake.core.security.KerberosManager(
                        "svc@REALM", "/no/such.keytab", 3_600_000L) {
                    @Override
                    public long getReloginFailureCount() {
                        return 7L;
                    }
                };

        IntakeRuntimeManager runtime = new IntakeRuntimeManager(
                new IntakeProperties(), fileSystem, conf, mock(MqConnectionManager.class),
                config -> TRIVIAL_SERIALIZER,
                new BindingConfigValidator(
                        path -> com.hcsc.datalake.mqintake.core.config.HdfsPathValidator
                                .PathValidationResult.success()),
                new BindingHealthManager(),
                ProductionMode.disabled(),
                com.hcsc.datalake.mqintake.core.config.InstanceId.of("kerberos-test"),
                null, manager);

        assertThat(runtime.getMetricsRegistry().getKerberosReloginFailures()).isEqualTo(7L);
    }

    @Test
    void withoutKerberosTheGaugeKeepsReportingZero() {
        // Kerberos disabled means no manager bean, and a zero that is true.
        IntakeRuntimeManager runtime = new IntakeRuntimeManager(
                new IntakeProperties(), fileSystem, conf, mock(MqConnectionManager.class),
                config -> TRIVIAL_SERIALIZER,
                new BindingConfigValidator(
                        path -> com.hcsc.datalake.mqintake.core.config.HdfsPathValidator
                                .PathValidationResult.success()),
                new BindingHealthManager(),
                ProductionMode.disabled(),
                com.hcsc.datalake.mqintake.core.config.InstanceId.of("kerberos-test"),
                null, null);

        assertThat(runtime.getMetricsRegistry().getKerberosReloginFailures()).isZero();
    }

    private IntakeRuntimeManager managerWithStartFailure(IntakeProperties props,
                                                         String failingBindingId,
                                                         List<BindingRuntime> created) {
        return manager(props, () -> factoryOf(binding -> {
            if (binding.getId().equals(failingBindingId)) {
                return failsOnStartRuntime(binding);
            }
            BindingRuntime runtime = blockingRuntime(binding);
            created.add(runtime);
            return runtime;
        }), null);
    }

    /** A runtime whose start() throws, as if task submission had been refused. */
    private BindingRuntime failsOnStartRuntime(BindingConfig binding) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executors.add(executor);
        List<TransactedReceiveLoop> loops = List.of(new TransactedReceiveLoop(
                binding, mock(javax.jms.Connection.class), null, null, null, null,
                null, null, null, "test", 100));
        return new BindingRuntime(binding, loops, executor, false, null) {
            @Override
            public void start() throws BindingStartupException {
                throw new BindingStartupException(
                        "simulated start failure for '" + binding.getId() + "'",
                        new java.util.concurrent.RejectedExecutionException("no threads left"));
            }
        };
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
            binding.getHdfs().setBasePath(base);
            binding.getBatch().setSize(10);
            binding.getBatch().setBytes(1024 * 1024);
            binding.getBatch().setIntervalMs(1000);
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
        return manager(props, () -> factoryOf(binding -> {
            if (binding.getId().equals(failingBindingId)) {
                throw new BindingRuntimeFactory.BindingRuntimeCreationException(
                        "simulated failure for binding '" + binding.getId() + "'");
            }
            BindingRuntime runtime = blockingRuntime(binding);
            created.add(runtime);
            return runtime;
        }), null);
    }

    /**
     * A manager wired to test collaborators instead of a test subclass.
     *
     * <p>The real runtime factory needs a live MQ connection manager and the
     * real reconciliation scheduler a live filesystem, which is why both are
     * injectable at all; passing them here is what lets this test stop
     * extending the production lifecycle root.
     *
     * @param reconciliation null to use the real scheduler, which is inert
     *                       while reconciliation is disabled
     */
    private IntakeRuntimeManager manager(IntakeProperties props,
                                         Supplier<BindingRuntimeFactory> runtimeFactory,
                                         Supplier<ReconciliationScheduler> reconciliation) {
        return new IntakeRuntimeManager(
                props, fileSystem, conf, mock(MqConnectionManager.class),
                config -> TRIVIAL_SERIALIZER,
                new BindingConfigValidator(
                        path -> com.hcsc.datalake.mqintake.core.config.HdfsPathValidator
                                .PathValidationResult.success()),
                new BindingHealthManager(),
                ProductionMode.disabled(),
                com.hcsc.datalake.mqintake.core.config.InstanceId.of("rollback-test"),
                null, null, runtimeFactory, reconciliation);
    }

    /** A factory that creates runtimes the way the caller says, and nothing else. */
    private BindingRuntimeFactory factoryOf(RuntimeCreator creator) {
        return new BindingRuntimeFactory(
                fileSystem, conf, mock(MqConnectionManager.class),
                config -> TRIVIAL_SERIALIZER, null,
                new com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry(),
                new BindingHealthManager(), null, "test") {
            @Override
            public BindingRuntime create(BindingConfig binding)
                    throws BindingRuntimeCreationException {
                return creator.create(binding);
            }
        };
    }

    /** Like the factory's create(), including the failure it is allowed to signal. */
    @FunctionalInterface
    private interface RuntimeCreator {
        BindingRuntime create(BindingConfig binding)
                throws BindingRuntimeFactory.BindingRuntimeCreationException;
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
