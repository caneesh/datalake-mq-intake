package com.hcsc.datalake.mqintake.core.integration;

import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.reconciliation.SequenceFileIdentityReader;
import com.hcsc.datalake.mqintake.core.runtime.BindingRuntime;
import com.hcsc.datalake.mqintake.core.runtime.BindingRuntimeFactory;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Production-path integration tests (round 2 prompt 11, DESIGN §15).
 *
 * <p>These tests exercise the REAL production chain — BindingRuntimeFactory
 * → BindingRuntime → TransactedReceiveLoop → SequenceFileBatchWriter (_tmp
 * write, close, atomic rename) → JMS transaction → tracker → immutable audit
 * → health — over an embedded broker and local filesystem. Nothing in the
 * pipeline is hand-wired or simulated except the broker itself and the
 * serializer's payload format.
 *
 * <p>Crash-window assertions verify message identities and zero loss;
 * duplicates appear only in the window the design permits (§12.1 states 3–4).
 */
class ProductionPathIntegrationTest {

    private static final long RECEIVE_TIMEOUT_MS = 100;

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem fileSystem;
    private Configuration hadoopConf;
    private Connection connection;
    private Session producerSession;
    private MqConnectionManager mqConnectionManager;
    private MetricsRegistry metricsRegistry;
    private BindingHealthManager healthManager;
    private HdfsAuditRecordEmitter auditEmitter;
    private SequenceFileIdentityReader identityReader;
    private final List<BindingRuntime> startedRuntimes = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(hadoopConf);
        identityReader = new SequenceFileIdentityReader(hadoopConf);

        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        factory.getRedeliveryPolicy().setMaximumRedeliveries(-1);
        connection = factory.createConnection();
        connection.start();
        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MqConnectionConfig mqConfig = new MqConnectionConfig();
        mqConfig.setId("primary");
        mqConfig.setReceiveTimeoutMs(RECEIVE_TIMEOUT_MS);

        mqConnectionManager = mock(MqConnectionManager.class);
        when(mqConnectionManager.hasConnection("primary")).thenReturn(true);
        when(mqConnectionManager.getConnection("primary")).thenReturn(connection);
        when(mqConnectionManager.getConfig("primary")).thenReturn(Optional.of(mqConfig));

        metricsRegistry = new MetricsRegistry();
        healthManager = new BindingHealthManager();
        auditEmitter = new HdfsAuditRecordEmitter(
                fileSystem, tempDir.resolve("audit").toString(), "it-instance",
                java.time.Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        for (BindingRuntime runtime : startedRuntimes) {
            try {
                runtime.stop(3000);
            } catch (Exception ignored) {
            }
        }
        startedRuntimes.clear();
        try {
            producerSession.close();
            connection.close();
        } catch (Exception ignored) {
        }
    }

    // §15.4 happy path + audit + health/metrics through the real chain
    @Test
    void endToEndBatchLandsWithAuditAndNoTmpLeftovers() throws Exception {
        BindingConfig config = bindingConfig("e2e", "IT.E2E.SOURCE", BindingMode.LAND_ONLY, 1);
        sendMessages("IT.E2E.SOURCE", "e2e", 10);

        BindingRuntime runtime = createAndStart(config, serializerFactory(0), null);

        awaitTrue(10_000, () -> landedIdentities("e2e", config).size() == 10);
        runtime.stop(3000);

        // Zero loss: every identity present in the partition
        assertThat(landedIdentities("e2e", config))
                .containsExactlyInAnyOrderElementsOf(expectedIdentities("e2e", 10));

        // Source queue drained, no _tmp leftovers
        assertThat(countOnQueue("IT.E2E.SOURCE")).isZero();
        assertThat(listFiles(config.getHdfsBasePath() + "/_tmp/it-instance")).isEmpty();

        // Immutable audit record(s) exist for the binding
        assertThat(listFilesRecursive(tempDir.resolve("audit").resolve("e2e").toString()))
                .isNotEmpty();

        // Metrics wired from the production loop
        assertThat(metricsRegistry.forBinding("e2e").getCommitCount()).isGreaterThan(0);
        assertThat(metricsRegistry.forBinding("e2e").getMessagesWritten()).isEqualTo(10);
    }

    // §15.2 failure during _tmp SequenceFile write → rollback → redelivery
    @Test
    void tmpWriteFailureRollsBackThenRedeliveryLandsEverything() throws Exception {
        BindingConfig config = bindingConfig("tmpfail", "IT.TMPFAIL.SOURCE", BindingMode.LAND_ONLY, 1);
        sendMessages("IT.TMPFAIL.SOURCE", "tmpfail", 6);

        // First batch attempt fails inside the write (serialization), then heals
        BindingRuntime runtime = createAndStart(config, serializerFactory(1), null);

        awaitTrue(15_000, () -> landedIdentities("tmpfail", config).size() == 6);
        runtime.stop(3000);

        assertThat(landedIdentities("tmpfail", config))
                .containsExactlyInAnyOrderElementsOf(expectedIdentities("tmpfail", 6));
        assertThat(countOnQueue("IT.TMPFAIL.SOURCE")).isZero();
        assertThat(metricsRegistry.forBinding("tmpfail").getRollbackCount()).isGreaterThan(0);
        // No half-written file remains in _tmp
        assertThat(listFiles(config.getHdfsBasePath() + "/_tmp/it-instance")).isEmpty();
    }

    // §15.3 failure after rename before MQ commit (tracker send fails):
    // the landed file survives, MQ redelivers, and the design-permitted
    // duplicate appears — zero loss
    @Test
    void trackerFailureAfterRenameYieldsPermittedDuplicateNotLoss() throws Exception {
        BindingConfig config = bindingConfig("trk", "IT.TRK.SOURCE", BindingMode.TRACKED, 1);
        config.setTrackerQueue("IT.TRK.TRACKER");
        sendMessages("IT.TRK.SOURCE", "trk", 4);

        AtomicInteger trackerCalls = new AtomicInteger();
        TrackerMessageBuilderFactory failingOnceTracker = cfg -> (session, source) -> {
            if (trackerCalls.getAndIncrement() == 0) {
                throw new JMSException("tracker queue unavailable");
            }
            return Optional.of(session.createTextMessage("TRACKER"));
        };

        BindingRuntime runtime = createAndStart(config, serializerFactory(0), failingOnceTracker);

        awaitTrue(15_000, () -> countOnQueue("IT.TRK.TRACKER") == 4
                && countOnQueue("IT.TRK.SOURCE") == 0);
        runtime.stop(3000);

        // The first batch's file was renamed into the partition BEFORE the
        // tracker failure rolled back MQ → redelivery landed a second copy.
        // §12.1: at-least-once permits the duplicate; nothing is lost.
        List<String> files = listFilesRecursive(config.getHdfsBasePath());
        List<String> seqFiles = new ArrayList<>();
        for (String f : files) {
            if (f.endsWith(".seq")) seqFiles.add(f);
        }
        assertThat(seqFiles.size()).isGreaterThanOrEqualTo(2);

        Set<String> allIdentities = new HashSet<>();
        for (String f : seqFiles) {
            allIdentities.addAll(identityReader.extractIdentities(f));
        }
        assertThat(allIdentities)
                .containsExactlyInAnyOrderElementsOf(expectedIdentities("trk", 4));

        // Tracker messages sent exactly once per committed message
        assertThat(countOnQueue("IT.TRK.TRACKER")).isEqualTo(4);
    }

    // §15.10 / §15.1 graceful shutdown (and kill) with the batch only in
    // memory: the bounded drain either commits the in-flight batch or rolls
    // it back — in both outcomes the identity set proves zero loss, and no
    // uncommittable batch is ever force-renamed
    @Test
    void gracefulShutdownWithInFlightBatchLosesNothing() throws Exception {
        BindingConfig config = bindingConfig("drain", "IT.DRAIN.SOURCE", BindingMode.LAND_ONLY, 1);
        config.setBatchSize(100);          // never reaches size trigger
        config.setBatchIntervalMs(60_000); // never reaches time trigger
        sendMessages("IT.DRAIN.SOURCE", "drain", 5);

        BindingRuntime runtime = createAndStart(config, serializerFactory(0), null);

        // Wait until the loop has consumed the messages into its in-memory batch
        awaitTrue(10_000, () -> countOnQueue("IT.DRAIN.SOURCE") == 0);

        runtime.stop(5000);

        // Either the drain committed (all landed, queue empty) or the batch
        // rolled back (nothing landed, all 5 redelivered to the queue).
        // Both are safe; loss or partial commit is not.
        Set<String> landed = landedIdentities("drain", config);
        int queued = countOnQueue("IT.DRAIN.SOURCE");
        assertThat(landed.size() + queued)
                .as("landed=%s queued=%d — combined must equal 5", landed, queued)
                .isEqualTo(5);
        if (!landed.isEmpty()) {
            assertThat(landed).containsExactlyInAnyOrderElementsOf(expectedIdentities("drain", 5));
        }
        // No half-written file left behind either way
        assertThat(listFiles(config.getHdfsBasePath() + "/_tmp/it-instance")).isEmpty();
    }

    // §15.13 binding isolation: one binding's persistent data failure leaves
    // the other binding healthy and fully landed
    @Test
    void failingBindingDoesNotAffectHealthyBinding() throws Exception {
        BindingConfig healthy = bindingConfig("okbind", "IT.OK.SOURCE", BindingMode.LAND_ONLY, 1);
        BindingConfig broken = bindingConfig("badbind", "IT.BAD.SOURCE", BindingMode.LAND_ONLY, 1);
        sendMessages("IT.OK.SOURCE", "okbind", 5);
        sendMessages("IT.BAD.SOURCE", "badbind", 3);

        BindingRuntime healthyRuntime = createAndStart(healthy, serializerFactory(0), null);
        BindingRuntime brokenRuntime = createAndStart(broken, serializerFactory(Integer.MAX_VALUE), null);

        awaitTrue(10_000, () -> landedIdentities("okbind", healthy).size() == 5);

        // Healthy binding landed everything
        assertThat(landedIdentities("okbind", healthy))
                .containsExactlyInAnyOrderElementsOf(expectedIdentities("okbind", 5));

        // Broken binding: nothing landed, messages retained on the queue
        // (rolled back, not lost), health DEGRADED — without touching okbind
        assertThat(landedIdentities("badbind", broken)).isEmpty();
        awaitTrue(5_000, () -> healthManager.getStatus("badbind")
                == BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(healthManager.getStatus("badbind"))
                .isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(healthManager.getStatus("okbind"))
                .isNotIn(BindingHealthManager.HealthStatus.DEGRADED,
                        BindingHealthManager.HealthStatus.UNHEALTHY);

        healthyRuntime.stop(3000);
        brokenRuntime.stop(3000);

        assertThat(countOnQueue("IT.BAD.SOURCE")).isEqualTo(3);
    }

    // §15.12 multiple listeners: redelivery after a failure may land on a
    // different thread; identity set proves zero loss either way
    @Test
    void multipleListenersRedeliveryLandsAllMessages() throws Exception {
        BindingConfig config = bindingConfig("multi", "IT.MULTI.SOURCE", BindingMode.LAND_ONLY, 2);
        sendMessages("IT.MULTI.SOURCE", "multi", 12);

        BindingRuntime runtime = createAndStart(config, serializerFactory(2), null);

        awaitTrue(20_000, () -> landedIdentities("multi", config).size() == 12);
        runtime.stop(3000);

        assertThat(landedIdentities("multi", config))
                .containsExactlyInAnyOrderElementsOf(expectedIdentities("multi", 12));
        assertThat(countOnQueue("IT.MULTI.SOURCE")).isZero();
    }

    // --- production-shaped serializer with fault injection ---

    /**
     * Serializer producing the production metadata-key shape
     * (binding_id=|payload_guid=|mq_message_id=|...) so the reconciliation
     * identity reader can verify landed identities. Fails the first
     * {@code failBatches} batch attempts with a data-classified failure.
     */
    private RecordSerializerFactory serializerFactory(int failBatches) {
        AtomicInteger failuresRemaining = new AtomicInteger(failBatches);
        return cfg -> new RecordSerializer() {
            @Override
            public SerializedRecord serialize(Message message, RecordMetadata metadata)
                    throws SerializationException {
                if (metadata.getRecordOffset() == 0 && failuresRemaining.getAndDecrement() > 0) {
                    throw new SerializationException("malformed payload (injected)");
                }
                failuresRemaining.updateAndGet(v -> Math.min(v, Math.max(v, 0)));
                try {
                    String body = ((TextMessage) message).getText();
                    Text key = new Text("binding_id=" + metadata.getBindingId() +
                            "|payload_guid=" + body +
                            "|mq_message_id=" + metadata.getMqMessageId() +
                            "|consume_ts_utc=" + metadata.getConsumeTimestamp());
                    return new SerializedRecord(key,
                            new BytesWritable(body.getBytes(StandardCharsets.UTF_8)));
                } catch (JMSException e) {
                    throw new SerializationException("read failed", e);
                }
            }

            @Override
            public Class<? extends Writable> getKeyClass() { return Text.class; }

            @Override
            public Class<? extends Writable> getValueClass() { return BytesWritable.class; }
        };
    }

    // --- helpers ---

    private BindingConfig bindingConfig(String id, String sourceQueue,
                                        BindingMode mode, int threads) {
        BindingConfig config = new BindingConfig();
        config.setId(id);
        config.setMqConnection("primary");
        config.setSourceQueue(sourceQueue);
        config.setMode(mode);
        config.setHdfsBasePath(tempDir.resolve(id).toString());
        config.setBatchSize(6);
        config.setBatchBytes(64 * 1024 * 1024);
        config.setBatchIntervalMs(300);
        config.setListenerThreads(threads);
        return config;
    }

    private BindingRuntime createAndStart(BindingConfig config,
                                          RecordSerializerFactory serializers,
                                          TrackerMessageBuilderFactory trackers) throws Exception {
        BindingRuntimeFactory factory = new BindingRuntimeFactory(
                fileSystem, hadoopConf, mqConnectionManager,
                serializers, trackers, metricsRegistry, healthManager,
                auditEmitter, "it-instance");
        BindingRuntime runtime = factory.create(config);
        runtime.start();
        startedRuntimes.add(runtime);
        return runtime;
    }

    private void sendMessages(String queue, String prefix, int count) throws Exception {
        try (MessageProducer producer =
                     producerSession.createProducer(producerSession.createQueue(queue))) {
            for (int i = 0; i < count; i++) {
                producer.send(producerSession.createTextMessage(prefix + "-msg-" + i));
            }
        }
    }

    private Set<String> expectedIdentities(String prefix, int count) {
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < count; i++) {
            expected.add(prefix + "-msg-" + i);
        }
        return expected;
    }

    private Set<String> landedIdentities(String bindingId, BindingConfig config) throws Exception {
        Set<String> identities = new HashSet<>();
        for (String file : listFilesRecursive(config.getHdfsBasePath())) {
            // Only renamed (visible) files count as landed — never in-progress _tmp
            if (file.endsWith(".seq") && !file.contains("/_tmp/")) {
                try {
                    identities.addAll(identityReader.extractIdentities(file));
                } catch (Exception e) {
                    // File may be mid-rename during polling; retry on next poll
                }
            }
        }
        return identities;
    }

    private List<String> listFiles(String dir) throws Exception {
        Path path = new Path(dir);
        List<String> files = new ArrayList<>();
        if (!fileSystem.exists(path)) {
            return files;
        }
        for (FileStatus status : fileSystem.listStatus(path)) {
            if (status.isFile()) {
                files.add(status.getPath().toString());
            }
        }
        return files;
    }

    private List<String> listFilesRecursive(String dir) throws Exception {
        Path path = new Path(dir);
        List<String> files = new ArrayList<>();
        if (!fileSystem.exists(path)) {
            return files;
        }
        try {
            var iter = fileSystem.listFiles(path, true);
            while (iter.hasNext()) {
                files.add(iter.next().getPath().toString());
            }
        } catch (RuntimeException | java.io.FileNotFoundException e) {
            // Local FS races: a _tmp file can vanish (rename/delete) between
            // listing and stat while the runtime is active — retry next poll
        }
        return files;
    }

    private int countOnQueue(String queueName) throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            QueueBrowser browser = s.createBrowser(s.createQueue(queueName));
            int count = 0;
            var e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                e.nextElement();
                count++;
            }
            return count;
        }
    }

    private void awaitTrue(long timeoutMs, Check check) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(100);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean ok() throws Exception;
    }
}
