package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.metrics.BackoutQueueDepthMonitor;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionProvider;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for creating BindingRuntime instances.
 *
 * <p>Assembles all components needed for a binding's runtime:
 * <ul>
 *   <li>RecordSerializer (from module-provided factory)</li>
 *   <li>TrackerMessageBuilder (from module-provided factory, TRACKED only)</li>
 *   <li>BatchWriter (SequenceFileBatchWriter)</li>
 *   <li>PoisonMessageHandler (if backout queue configured)</li>
 *   <li>DegradedModeManager (one per binding, shared by all loops)</li>
 *   <li>TransactedReceiveLoop (one per listener thread)</li>
 * </ul>
 *
 * <p>This factory is stateless and thread-safe. Each call to create() produces
 * an independent BindingRuntime.
 */
public class BindingRuntimeFactory {

    private static final Logger log = LoggerFactory.getLogger(BindingRuntimeFactory.class);
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);

    private final FileSystem fileSystem;
    private final Configuration hadoopConf;
    private final MqConnectionProvider mqConnectionManager;
    private final RecordSerializerFactory serializerFactory;
    private final TrackerMessageBuilderFactory trackerBuilderFactory;
    private final MetricsRegistry metricsRegistry;
    private final BindingHealthManager healthManager;
    private final AuditRecordEmitter auditEmitter;
    private final String instanceId;

    public BindingRuntimeFactory(FileSystem fileSystem,
                                  Configuration hadoopConf,
                                  MqConnectionProvider mqConnectionManager,
                                  RecordSerializerFactory serializerFactory,
                                  TrackerMessageBuilderFactory trackerBuilderFactory,
                                  MetricsRegistry metricsRegistry,
                                  BindingHealthManager healthManager,
                                  AuditRecordEmitter auditEmitter,
                                  String instanceId) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf required");
        this.mqConnectionManager = Objects.requireNonNull(mqConnectionManager, "mqConnectionManager required");
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory required");
        this.trackerBuilderFactory = trackerBuilderFactory; // May be null for LAND_ONLY modules
        this.metricsRegistry = Objects.requireNonNull(metricsRegistry, "metricsRegistry required");
        this.healthManager = healthManager; // May be null for tests
        this.auditEmitter = auditEmitter; // May be null
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId required");
    }

    /**
     * Creates a BindingRuntime for the given configuration.
     *
     * @param config the binding configuration
     * @return a new BindingRuntime ready to start
     * @throws BindingRuntimeCreationException if creation fails
     */
    public BindingRuntime create(BindingConfig config) throws BindingRuntimeCreationException {
        Objects.requireNonNull(config, "config required");
        String bindingId = config.getId();

        log.debug("Creating runtime for binding '{}': mode={}, threads={}",
                bindingId, config.getMode(), config.getListenerThreads());

        try {
            validateConfig(config);

            Connection jmsConnection = getConnectionForBinding(config);
            long receiveTimeoutMs = getReceiveTimeoutForBinding(config);

            RecordSerializer serializer = createSerializer(config);
            BatchWriter batchWriter = createBatchWriter(config, serializer);
            TrackerMessageBuilder trackerBuilder = createTrackerBuilder(config);
            PoisonMessageHandler poisonHandler = createPoisonHandler(config);
            BindingMetrics metrics = metricsRegistry.forBinding(bindingId);

            List<TransactedReceiveLoop> loops = createLoops(
                    config, jmsConnection, receiveTimeoutMs, batchWriter, trackerBuilder, poisonHandler, metrics);

            ExecutorService executor = createExecutor(config);
            BackoutQueueDepthMonitor depthMonitor =
                    createBackoutDepthMonitor(config, jmsConnection, metrics);

            BindingRuntime runtime = new BindingRuntime(
                    config, loops, executor, trackerBuilder != null, depthMonitor);
            // Supervision needs the health manager, which the factory owns.
            runtime.configureSupervision(
                    healthManager, BindingRuntime.DEFAULT_SUPERVISION_INTERVAL_MS);
            return runtime;

        } catch (BindingRuntimeCreationException e) {
            throw e;
        } catch (Exception e) {
            throw new BindingRuntimeCreationException(
                    "Failed to create runtime for binding '" + bindingId + "': " + e.getMessage(), e);
        }
    }

    private void validateConfig(BindingConfig config) throws BindingRuntimeCreationException {
        if (config.getMode() == BindingMode.TRACKED && trackerBuilderFactory == null) {
            throw new BindingRuntimeCreationException(
                    "TRACKED binding '" + config.getId() + "' requires a TrackerMessageBuilderFactory, " +
                    "but none was provided. Ensure the module provides a TrackerMessageBuilderFactory bean.");
        }

        String mqConnId = config.getMqConnection();
        if (mqConnId == null || mqConnId.isBlank()) {
            throw new BindingRuntimeCreationException(
                    "Binding '" + config.getId() + "' does not specify mq-connection.");
        }

        if (!mqConnectionManager.hasConnection(mqConnId)) {
            throw new BindingRuntimeCreationException(
                    "Binding '" + config.getId() + "' references unknown mq-connection: " + mqConnId);
        }
    }

    private Connection getConnectionForBinding(BindingConfig config) throws BindingRuntimeCreationException {
        String mqConnId = config.getMqConnection();
        try {
            Connection connection = mqConnectionManager.getConnection(mqConnId);
            Optional<MqConnectionConfig> mqConfig = mqConnectionManager.getConfig(mqConnId);
            log.debug("Obtained connection for binding '{}' from mq-connection '{}'",
                    config.getId(), mqConnId);
            return connection;
        } catch (MqConnectionManager.MqConnectionException e) {
            throw new BindingRuntimeCreationException(
                    "Failed to get MQ connection for binding '" + config.getId() + "': " + e.getMessage(), e);
        }
    }

    private long getReceiveTimeoutForBinding(BindingConfig config) {
        String mqConnId = config.getMqConnection();
        Optional<MqConnectionConfig> mqConfig = mqConnectionManager.getConfig(mqConnId);
        return mqConfig.map(MqConnectionConfig::getReceiveTimeoutMs).orElse(1000L);
    }

    private RecordSerializer createSerializer(BindingConfig config) {
        return serializerFactory.create(config);
    }

    private BatchWriter createBatchWriter(BindingConfig config, RecordSerializer serializer) {
        return new SequenceFileBatchWriter(
                fileSystem, hadoopConf, serializer, instanceId,
                config.getId(), config.getHdfsBasePath());
    }

    private TrackerMessageBuilder createTrackerBuilder(BindingConfig config) {
        if (config.getMode() != BindingMode.TRACKED) {
            return null;
        }
        if (trackerBuilderFactory == null) {
            return null;
        }
        return trackerBuilderFactory.create(config);
    }

    private PoisonMessageHandler createPoisonHandler(BindingConfig config) {
        if (config.getBackoutQueue() == null || config.getBackoutQueue().isBlank()) {
            return null;
        }
        return new PoisonMessageHandler(
                config.getBackoutThreshold(),
                config.getBackoutQueue());
    }

    /**
     * Creates the backout-depth monitor, or null when the binding has no
     * backout queue to watch — the same guard used for the poison handler.
     */
    private BackoutQueueDepthMonitor createBackoutDepthMonitor(BindingConfig config,
                                                               Connection jmsConnection,
                                                               BindingMetrics metrics) {
        if (config.getBackoutQueue() == null || config.getBackoutQueue().isBlank()) {
            return null;
        }
        return new BackoutQueueDepthMonitor(
                config.getId(),
                config.getBackoutQueue(),
                jmsConnection,
                metrics,
                config.getBackoutDepthPollIntervalMs());
    }

    private List<TransactedReceiveLoop> createLoops(BindingConfig config,
                                                     Connection jmsConnection,
                                                     long receiveTimeoutMs,
                                                     BatchWriter batchWriter,
                                                     TrackerMessageBuilder trackerBuilder,
                                                     PoisonMessageHandler poisonHandler,
                                                     BindingMetrics metrics) {
        List<TransactedReceiveLoop> loops = new ArrayList<>();

        // CRITICAL: One DegradedModeManager per binding, shared by ALL loops.
        // When any thread hits a MESSAGE_DATA failure, ALL threads switch to
        // degraded batch sizes until the binding recovers.
        DegradedModeManager degradedModeManager = new DegradedModeManager(
                config.getId(),
                config.getBatchSize(),
                config.getDegradationStrategy(),
                config.getSuccessesRequiredToRestore());

        for (int i = 0; i < config.getListenerThreads(); i++) {
            TransactedReceiveLoop loop = new TransactedReceiveLoop(
                    config,
                    jmsConnection,
                    batchWriter,
                    trackerBuilder,
                    poisonHandler,
                    degradedModeManager,  // Shared instance
                    healthManager,
                    auditEmitter,
                    metrics,
                    instanceId,
                    receiveTimeoutMs);

            loops.add(loop);
        }

        return loops;
    }

    private ExecutorService createExecutor(BindingConfig config) {
        String bindingId = config.getId();
        return Executors.newFixedThreadPool(
                config.getListenerThreads(),
                r -> {
                    Thread t = new Thread(r, "recv-" + bindingId + "-" + THREAD_COUNTER.incrementAndGet());
                    t.setDaemon(false);
                    return t;
                });
    }

    public static class BindingRuntimeCreationException extends Exception {
        public BindingRuntimeCreationException(String message) {
            super(message);
        }

        public BindingRuntimeCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
