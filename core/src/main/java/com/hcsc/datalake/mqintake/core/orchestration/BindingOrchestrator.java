package com.hcsc.datalake.mqintake.core.orchestration;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.lifecycle.GracefulShutdownHandler;
import com.hcsc.datalake.mqintake.core.lifecycle.StartupValidator;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import javax.jms.Connection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates the startup and shutdown of all binding receive loops.
 *
 * <p>Implements SmartLifecycle to integrate with Spring's container lifecycle.
 * Validates all bindings before starting any, per DESIGN.md §14.
 */
@Component
public class BindingOrchestrator implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BindingOrchestrator.class);

    private final IntakeProperties properties;
    private final FileSystem fileSystem;
    private final Configuration hadoopConf;
    private final Connection jmsConnection;
    private final RecordSerializerFactory serializerFactory;
    private final TrackerMessageBuilderFactory trackerBuilderFactory;

    private final MetricsRegistry metricsRegistry;
    private final BindingHealthManager healthManager;
    private final GracefulShutdownHandler shutdownHandler;
    private final AuditRecordEmitter auditEmitter;

    private final List<TransactedReceiveLoop> loops = new ArrayList<>();
    private final Map<String, ExecutorService> bindingExecutors = new HashMap<>();
    private volatile boolean running = false;

    @Autowired
    public BindingOrchestrator(IntakeProperties properties,
                                FileSystem fileSystem,
                                Configuration hadoopConf,
                                @Autowired(required = false) Connection jmsConnection,
                                @Autowired(required = false) RecordSerializerFactory serializerFactory,
                                @Autowired(required = false) TrackerMessageBuilderFactory trackerBuilderFactory) {
        this.properties = properties;
        this.fileSystem = fileSystem;
        this.hadoopConf = hadoopConf;
        this.jmsConnection = jmsConnection;
        this.serializerFactory = serializerFactory;
        this.trackerBuilderFactory = trackerBuilderFactory;

        this.metricsRegistry = new MetricsRegistry();
        this.healthManager = new BindingHealthManager();
        this.shutdownHandler = new GracefulShutdownHandler(
                properties.getShutdown().getDrainTimeoutMs());
        this.auditEmitter = new HdfsAuditRecordEmitter(
                fileSystem, properties.getHdfs().getAuditBasePath(),
                properties.getInstanceId(), java.time.Clock.systemUTC());
    }

    @Override
    public void start() {
        if (jmsConnection == null) {
            log.warn("No JMS connection available — bindings will not start. " +
                    "Configure intake.mq.host to enable MQ connectivity.");
            running = true;
            return;
        }

        if (properties.getBindings().isEmpty()) {
            log.warn("No bindings configured — nothing to start");
            running = true;
            return;
        }

        try {
            validateBindings();
            cleanupTempFiles();
            startBindings();
            shutdownHandler.registerShutdownHook();
            running = true;
            log.info("BindingOrchestrator started with {} bindings", properties.getBindings().size());
        } catch (Exception e) {
            log.error("Failed to start BindingOrchestrator: {}", e.getMessage(), e);
            throw new RuntimeException("Startup failed", e);
        }
    }

    private void validateBindings() throws StartupValidator.StartupValidationException {
        StartupValidator validator = new StartupValidator(fileSystem, properties.getInstanceId());
        validator.validateOrFail(properties.getBindings());
        log.info("All binding paths validated successfully");
    }

    private void cleanupTempFiles() {
        String instanceId = properties.getInstanceId();
        long maxAge = properties.getHdfs().getTempFileMaxAgeMs();

        for (BindingConfig binding : properties.getBindings()) {
            try {
                StartupValidator validator = new StartupValidator(fileSystem, instanceId);
                int deleted = validator.cleanupInstanceTempFiles(binding.getHdfsBasePath(), maxAge);
                if (deleted > 0) {
                    log.info("Cleaned up {} stale temp files for binding '{}'", deleted, binding.getId());
                }
            } catch (IOException e) {
                log.warn("Failed to cleanup temp files for binding '{}': {}",
                        binding.getId(), e.getMessage());
            }
        }
    }

    private void startBindings() {
        String instanceId = properties.getInstanceId();
        long receiveTimeoutMs = properties.getMq().getReceiveTimeoutMs();

        for (BindingConfig binding : properties.getBindings()) {
            startBinding(binding, instanceId, receiveTimeoutMs);
        }
    }

    private void startBinding(BindingConfig binding, String instanceId, long receiveTimeoutMs) {
        String bindingId = binding.getId();
        log.info("Starting binding '{}': mode={}, threads={}",
                bindingId, binding.getMode(), binding.getListenerThreads());

        RecordSerializer serializer = createSerializer(binding);
        SequenceFileBatchWriter batchWriter = new SequenceFileBatchWriter(
                fileSystem, hadoopConf, serializer, instanceId,
                bindingId, binding.getHdfsBasePath());

        TrackerMessageBuilder trackerBuilder = null;
        if (binding.getMode() == BindingMode.TRACKED && trackerBuilderFactory != null) {
            trackerBuilder = trackerBuilderFactory.create(binding);
        }

        PoisonMessageHandler poisonHandler = null;
        if (binding.getBackoutQueue() != null && !binding.getBackoutQueue().isBlank()) {
            poisonHandler = new PoisonMessageHandler(
                    binding.getBackoutThreshold(),
                    binding.getBackoutQueue());
        }

        BindingMetrics metrics = metricsRegistry.forBinding(bindingId);
        healthManager.recordHealthy(bindingId);

        ExecutorService executor = Executors.newFixedThreadPool(
                binding.getListenerThreads(),
                r -> new Thread(r, "recv-" + bindingId));
        bindingExecutors.put(bindingId, executor);

        for (int i = 0; i < binding.getListenerThreads(); i++) {
            DegradedModeManager degradedModeManager = new DegradedModeManager(
                    bindingId,
                    binding.getBatchSize(),
                    binding.getDegradationStrategy(),
                    binding.getSuccessesRequiredToRestore());

            TransactedReceiveLoop loop = new TransactedReceiveLoop(
                    binding,
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

            shutdownHandler.registerBinding(bindingId + "-" + i,
                    GracefulShutdownHandler.createSafeDrainTask(
                            loop.getSession(),
                            loop::stop,
                            () -> {}));

            executor.submit(loop);
            log.debug("Started listener thread {} for binding '{}'", i, bindingId);
        }

        healthManager.recordHealthy(bindingId);
        log.info("Binding '{}' started with {} listener threads", bindingId, binding.getListenerThreads());
    }

    private RecordSerializer createSerializer(BindingConfig binding) {
        if (serializerFactory != null) {
            return serializerFactory.create(binding);
        }
        throw new IllegalStateException(
                "No RecordSerializerFactory available for binding: " + binding.getId());
    }

    @Override
    public void stop() {
        log.info("Stopping BindingOrchestrator");
        running = false;

        GracefulShutdownHandler.ShutdownResult result = shutdownHandler.shutdown();
        log.info("Shutdown result: {} successful, {} timed out, {} failed",
                result.getSuccessful().size(),
                result.getTimedOut().size(),
                result.getFailed().size());

        for (ExecutorService executor : bindingExecutors.values()) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        loops.clear();
        bindingExecutors.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    public BindingHealthManager getHealthManager() {
        return healthManager;
    }

    public List<TransactedReceiveLoop> getLoops() {
        return List.copyOf(loops);
    }
}
