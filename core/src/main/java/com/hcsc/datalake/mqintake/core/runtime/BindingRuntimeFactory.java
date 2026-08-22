package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
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
 *   <li>DegradedModeManager (one per loop)</li>
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
    private final Connection jmsConnection;
    private final RecordSerializerFactory serializerFactory;
    private final TrackerMessageBuilderFactory trackerBuilderFactory;
    private final MetricsRegistry metricsRegistry;
    private final AuditRecordEmitter auditEmitter;
    private final String instanceId;
    private final long receiveTimeoutMs;

    public BindingRuntimeFactory(FileSystem fileSystem,
                                  Configuration hadoopConf,
                                  Connection jmsConnection,
                                  RecordSerializerFactory serializerFactory,
                                  TrackerMessageBuilderFactory trackerBuilderFactory,
                                  MetricsRegistry metricsRegistry,
                                  AuditRecordEmitter auditEmitter,
                                  String instanceId,
                                  long receiveTimeoutMs) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf required");
        this.jmsConnection = jmsConnection; // May be null if MQ not configured
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory required");
        this.trackerBuilderFactory = trackerBuilderFactory; // May be null for LAND_ONLY modules
        this.metricsRegistry = Objects.requireNonNull(metricsRegistry, "metricsRegistry required");
        this.auditEmitter = auditEmitter; // May be null
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId required");
        this.receiveTimeoutMs = receiveTimeoutMs;
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

            RecordSerializer serializer = createSerializer(config);
            BatchWriter batchWriter = createBatchWriter(config, serializer);
            TrackerMessageBuilder trackerBuilder = createTrackerBuilder(config);
            PoisonMessageHandler poisonHandler = createPoisonHandler(config);
            BindingMetrics metrics = metricsRegistry.forBinding(bindingId);

            List<TransactedReceiveLoop> loops = createLoops(
                    config, batchWriter, trackerBuilder, poisonHandler, metrics);

            ExecutorService executor = createExecutor(config);

            return new BindingRuntime(config, loops, executor, trackerBuilder != null);

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

        if (jmsConnection == null) {
            throw new BindingRuntimeCreationException(
                    "No JMS connection available for binding '" + config.getId() + "'. " +
                    "Configure intake.mq.host to enable MQ connectivity.");
        }
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

    private List<TransactedReceiveLoop> createLoops(BindingConfig config,
                                                     BatchWriter batchWriter,
                                                     TrackerMessageBuilder trackerBuilder,
                                                     PoisonMessageHandler poisonHandler,
                                                     BindingMetrics metrics) {
        List<TransactedReceiveLoop> loops = new ArrayList<>();

        for (int i = 0; i < config.getListenerThreads(); i++) {
            DegradedModeManager degradedModeManager = new DegradedModeManager(
                    config.getId(),
                    config.getBatchSize(),
                    config.getDegradationStrategy(),
                    config.getSuccessesRequiredToRestore());

            TransactedReceiveLoop loop = new TransactedReceiveLoop(
                    config,
                    jmsConnection,
                    batchWriter,
                    trackerBuilder,
                    poisonHandler,
                    degradedModeManager,
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
