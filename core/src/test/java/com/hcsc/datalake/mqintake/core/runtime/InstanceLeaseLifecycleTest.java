package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.audit.AuditPaths;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingConfigValidator;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.HdfsPathValidator;
import com.hcsc.datalake.mqintake.core.config.InstanceId;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.lifecycle.InstanceLease;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.Message;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The instance lease's lifecycle, which nothing held.
 *
 * <p>Both invariants here arrived with the staging-reclamation work and were
 * left untested: mutating them — taking the lease after the sweep instead of
 * before, and never releasing it on shutdown — broke no existing test. They are
 * also exactly what a lifecycle decomposition would move, so they are pinned
 * before any code moves.
 *
 * <p>The ordering is asserted from the filesystem calls themselves rather than
 * from the manager's internals: the lease file must be written before any
 * staging directory is listed. That is the observable form of "a peer starting
 * at the same moment sees our claim before it decides anything is abandoned",
 * and it survives the lease work being moved to a collaborator.
 */
class InstanceLeaseLifecycleTest {

    /** Non-null so the serializer gate can log its class name. */
    private static final RecordSerializer TRIVIAL = new RecordSerializer() {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata) {
            return new SerializedRecord(new LongWritable(0), new Text(""));
        }

        @Override
        public Class<? extends Writable> getKeyClass() {
            return LongWritable.class;
        }

        @Override
        public Class<? extends Writable> getValueClass() {
            return Text.class;
        }
    };

    private static final String INSTANCE = "lease-test";
    private static final String BINDING = "alpha";

    private Configuration conf;
    private RecordingFileSystem fileSystem;
    private java.nio.file.Path dataDir;
    private java.nio.file.Path auditDir;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = new RecordingFileSystem(FileSystem.get(conf));
        dataDir = Files.createTempDirectory("lease-data");
        auditDir = Files.createTempDirectory("lease-audit");
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.delete(new Path(dataDir.toString()), true);
        fileSystem.delete(new Path(auditDir.toString()), true);
    }

    @Test
    void theLeaseIsWrittenBeforeAnyStagingDirectoryIsSwept() {
        // The order only matters between instances, never within one. Sweeping
        // first leaves a window in which a peer starting at the same moment
        // finds an unclaimed directory and reclaims it.
        IntakeRuntimeManager manager = manager();
        try {
            manager.start();

            int leaseWritten = fileSystem.firstIndexOf("create", InstanceLease.LEASE_FILENAME);
            int stagingSwept = fileSystem.firstIndexOf("list", "/_tmp");

            assertThat(leaseWritten).as("a lease was written at all").isNotNegative();
            assertThat(stagingSwept).as("a staging directory was swept at all").isNotNegative();
            assertThat(leaseWritten)
                    .as("the claim must be visible before anything is reclaimed")
                    .isLessThan(stagingSwept);
        } finally {
            manager.stop();
        }
    }

    @Test
    void theLeaseIsHeldWhileRunningAndReleasedOnCleanShutdown() throws Exception {
        // Releasing it is what lets an ordinary restart reclaim its
        // predecessor's directory straight away instead of waiting out the
        // lease timeout. A kill leaves the lease behind, and the timeout is
        // what covers that case.
        IntakeRuntimeManager manager = manager();

        manager.start();
        for (Path lease : leasePaths()) {
            assertThat(fileSystem.exists(lease))
                    .as("held while running: %s", lease).isTrue();
        }

        manager.stop();
        for (Path lease : leasePaths()) {
            assertThat(fileSystem.exists(lease))
                    .as("released on clean shutdown: %s", lease).isFalse();
        }
    }

    // --- harness ---

    /**
     * Where a lease must exist: one per staging tree, data and audit alike.
     * Debris accumulates in both, so a tree with no claim is reclaimable by any
     * peer once its files age out.
     */
    private List<Path> leasePaths() {
        return List.of(
                new Path(PartitionPath.tempDir(dataDir.toString() + "/" + BINDING, INSTANCE),
                        InstanceLease.LEASE_FILENAME),
                new Path(PartitionPath.tempDir(
                        AuditPaths.bindingDir(auditDir.toString(), BINDING), INSTANCE),
                        InstanceLease.LEASE_FILENAME));
    }

    /**
     * A manager that runs startup as far as the staging work and no further.
     *
     * <p>Subclassed to neutralise binding startup, as
     * {@code PartialStartupRollbackTest} does. That seam is itself under
     * review; these two tests assert what the filesystem saw rather than how
     * the manager was built, so they keep holding once it is replaced.
     */
    private IntakeRuntimeManager manager() {
        return new IntakeRuntimeManager(
                properties(), fileSystem, conf, mock(MqConnectionManager.class),
                config -> TRIVIAL,
                new BindingConfigValidator(
                        path -> HdfsPathValidator.PathValidationResult.success()),
                new BindingHealthManager(),
                ProductionMode.disabled(),
                InstanceId.of(INSTANCE), null, null) {
            @Override
            void initializeRuntimeFactory() {
                setRuntimeFactoryForTest(inertRuntimeFactory());
            }
        };
    }

    /**
     * A factory whose runtimes start and stop without doing anything.
     *
     * <p>The real one needs a live connection manager, and this test is about
     * the staging lifecycle that surrounds the listeners rather than the
     * listeners themselves. Nothing is submitted to an executor, so no thread
     * outlives the test.
     */
    private BindingRuntimeFactory inertRuntimeFactory() {
        return new BindingRuntimeFactory(
                fileSystem, conf, mock(MqConnectionManager.class),
                config -> TRIVIAL, null,
                new com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry(),
                new BindingHealthManager(), null, "test") {
            @Override
            public BindingRuntime create(BindingConfig binding) {
                // One loop because BindingRuntime refuses to exist without
                // one; it is never submitted.
                List<TransactedReceiveLoop> loops = List.of(new TransactedReceiveLoop(
                        binding, mock(javax.jms.Connection.class), null, null, null, null,
                        null, null, null, "test", 100));
                return new BindingRuntime(binding, loops,
                        java.util.concurrent.Executors.newSingleThreadExecutor(), false, null) {
                    @Override
                    public void start() {
                    }

                    @Override
                    public void stop(long timeoutMs) {
                    }
                };
            }
        };
    }

    private IntakeProperties properties() {
        IntakeProperties props = new IntakeProperties();
        props.setInstanceId(INSTANCE);
        props.getHdfs().setAuditBasePath(auditDir.toString());
        props.getShutdown().setDrainTimeoutMs(200);

        Map<String, MqConnectionConfig> connections = new LinkedHashMap<>();
        MqConnectionConfig primary = new MqConnectionConfig();
        primary.setId("primary");
        primary.setHost("test-host");
        primary.setQueueManager("QM1");
        primary.setChannel("TEST.SVRCONN");
        connections.put("primary", primary);
        props.setMqConnections(connections);

        BindingConfig binding = new BindingConfig();
        binding.setId(BINDING);
        binding.setMqConnection("primary");
        binding.setMode(BindingMode.LAND_ONLY);
        binding.setSourceQueue("QUEUE.ALPHA");
        binding.setListenerThreads(1);
        binding.getBatch().setSize(10);
        binding.getBatch().setBytes(1024 * 1024);
        binding.getBatch().setIntervalMs(1000);
        String base = dataDir.toString() + "/" + BINDING;
        try {
            Files.createDirectories(java.nio.file.Paths.get(base));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        binding.getHdfs().setBasePath(base);
        props.setBindings(new ArrayList<>(List.of(binding)));
        return props;
    }

    /** Records the two filesystem calls whose order carries the invariant. */
    private static class RecordingFileSystem extends FilterFileSystem {

        private final List<String> operations = Collections.synchronizedList(new ArrayList<>());

        RecordingFileSystem(FileSystem delegate) {
            super(delegate);
        }

        @Override
        public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
            operations.add("create " + f);
            return super.create(f, overwrite);
        }

        @Override
        public FileStatus[] listStatus(Path f) throws IOException {
            operations.add("list " + f);
            return super.listStatus(f);
        }

        /** Position of the first call of this kind touching a path fragment, or -1. */
        int firstIndexOf(String op, String pathFragment) {
            synchronized (operations) {
                for (int i = 0; i < operations.size(); i++) {
                    String entry = operations.get(i);
                    if (entry.startsWith(op + " ") && entry.contains(pathFragment)) {
                        return i;
                    }
                }
            }
            return -1;
        }
    }
}
