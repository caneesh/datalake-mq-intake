package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingConfigValidator;
import com.hcsc.datalake.mqintake.core.config.InstanceId;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.config.SerializerValidator;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.lifecycle.StartupValidator;
import com.hcsc.datalake.mqintake.core.metrics.MetricsRegistry;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionProvider;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.reconciliation.ReconciliationFactory;
import com.hcsc.datalake.mqintake.core.reconciliation.ReconciliationScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the runtime lifecycle for all configured bindings.
 *
 * <p>Implements Spring's SmartLifecycle to integrate with container lifecycle.
 *
 * <p>Startup sequence:
 * <ol>
 *   <li>Validate all binding configurations</li>
 *   <li>Initialize shared HDFS resources</li>
 *   <li>Clean up stale temp files from previous runs</li>
 *   <li>Create BindingRuntime for each configured binding</li>
 *   <li>Start all runtimes</li>
 * </ol>
 *
 * <p>Shutdown sequence:
 * <ol>
 *   <li>Stop all runtimes (graceful drain)</li>
 *   <li>Wait for executor termination</li>
 *   <li>Close resources</li>
 * </ol>
 */
@Component
public class IntakeRuntimeManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(IntakeRuntimeManager.class);

    private final IntakeProperties properties;
    private final FileSystem fileSystem;
    private final Configuration hadoopConf;
    private final MqConnectionProvider mqConnectionManager;
    private final RecordSerializerFactory serializerFactory;
    private final TrackerMessageBuilderFactory trackerBuilderFactory;
    private final BindingConfigValidator bindingConfigValidator;
    private final ProductionMode productionMode;
    private final InstanceId instanceId;

    private final MetricsRegistry metricsRegistry;
    private final BindingHealthManager healthManager;
    private final Map<String, BindingRuntime> runtimes = new ConcurrentHashMap<>();
    private final List<String> startupErrors = Collections.synchronizedList(new ArrayList<>());

    private volatile boolean running = false;
    private volatile BindingRuntimeFactory runtimeFactory;
    private volatile ReconciliationScheduler reconciliationScheduler;

    @Autowired
    public IntakeRuntimeManager(IntakeProperties properties,
                                 FileSystem fileSystem,
                                 Configuration hadoopConf,
                                 MqConnectionProvider mqConnectionManager,
                                 RecordSerializerFactory serializerFactory,
                                 BindingConfigValidator bindingConfigValidator,
                                 BindingHealthManager healthManager,
                                 ProductionMode productionMode,
                                 InstanceId instanceId,
                                 @Autowired(required = false) TrackerMessageBuilderFactory trackerBuilderFactory) {
        this.productionMode = productionMode;
        this.instanceId = instanceId;
        this.properties = properties;
        this.fileSystem = fileSystem;
        this.hadoopConf = hadoopConf;
        this.mqConnectionManager = mqConnectionManager;
        this.serializerFactory = serializerFactory;
        this.bindingConfigValidator = bindingConfigValidator;
        this.trackerBuilderFactory = trackerBuilderFactory;

        this.metricsRegistry = new MetricsRegistry();
        this.healthManager = healthManager;
    }

    @Override
    public void start() {
        if (running) {
            // Spring's lifecycle processor never double-starts, but an
            // out-of-band caller (admin hook, test misuse) would re-run
            // createAndStartRuntimes, silently overwriting the runtimes map —
            // leaking the old executors and JMS sessions and creating a second
            // competing consumer against the same queues.
            log.warn("IntakeRuntimeManager.start() called while already running — ignoring");
            return;
        }

        log.info("Starting IntakeRuntimeManager");

        if (properties.getBindings().isEmpty()) {
            log.warn("No bindings configured — nothing to start");
            running = true;
            return;
        }

        if (properties.getMqConnections().isEmpty()) {
            // Two independent reviewers flagged the old warn-and-pretend-
            // healthy behaviour here. In a real boot this branch is already
            // unreachable — MqConfiguration's @PostConstruct validator fails
            // the context first when bindings reference unknown connections —
            // but a belt should agree with its braces: bindings with nothing
            // to connect them is a misconfiguration, not a healthy idle state.
            throw new IllegalStateException(
                    "Bindings are configured but intake.mq-connections is empty — nothing "
                            + "could ever be consumed. Configure intake.mq-connections.");
        }

        try {
            validateBindingConfigurations();
            validateSerializers();
            validateAllBindings();
            cleanupTempFiles();
            initializeRuntimeFactory();
            createAndStartRuntimes();
            try {
                startReconciliation();
            } catch (RuntimeException e) {
                // createAndStartRuntimes rolls back its OWN failures, but a
                // failure here — after every binding started — used to escape
                // with the bindings left consuming and running still false, so
                // Spring's lifecycle processor would never call stop(): the
                // half-live-orphan bug class the rollback exists to prevent,
                // reintroduced one line later. Latent today (nothing in
                // startReconciliation currently throws), guarded so it stays
                // latent.
                log.error("Startup failed after bindings started — stopping them so no "
                        + "listener outlives a failed startup: {}", e.getMessage());
                stopAllRuntimesQuietly();
                throw e;
            }
            running = true;

            log.info("IntakeRuntimeManager started: {} bindings, {} total listener threads",
                    runtimes.size(), getTotalLoopCount());

        } catch (Exception e) {
            log.error("Failed to start IntakeRuntimeManager: {}", e.getMessage(), e);
            throw new RuntimeException("Startup failed: " + e.getMessage(), e);
        }
    }

    private void validateBindingConfigurations() {
        bindingConfigValidator.validate(properties);
        log.info("Binding configurations validated successfully");
    }

    /**
     * Enforces the production serializer gate (§9.1): placeholder serializers
     * fail startup in production mode; otherwise they run with a loud warning.
     * Never a silent fallback.
     */
    private void validateSerializers() throws SerializerValidator.SerializerValidationException {
        new SerializerValidator(serializerFactory, productionMode)
                .validateOrFail(properties.getBindings());
    }

    private void validateAllBindings() throws StartupValidator.StartupValidationException {
        // Includes the audit destination: audit is written BEFORE the commit
        // and fails the batch when unwritable, so without this check the
        // service would start and then stall on its very first batch instead
        // of refusing to start with a message naming the path.
        StartupValidator validator = new StartupValidator(
                fileSystem,
                instanceId.value(),
                properties.getHdfs().getAuditBasePath());
        validator.validateOrFail(properties.getBindings());
        log.info("All binding configurations validated successfully");
    }

    private void cleanupTempFiles() {
        String resolvedInstanceId = this.instanceId.value();
        long maxAge = properties.getHdfs().getTempFileMaxAgeMs();

        for (BindingConfig binding : properties.getBindings()) {
            try {
                StartupValidator validator = new StartupValidator(fileSystem, resolvedInstanceId);
                int deleted = validator.cleanupInstanceTempFiles(binding.getHdfs().getBasePath(), maxAge);
                if (deleted > 0) {
                    log.info("Cleaned up {} stale temp files for binding '{}'", deleted, binding.getId());
                }
            } catch (IOException e) {
                log.warn("Failed to cleanup temp files for binding '{}': {}",
                        binding.getId(), e.getMessage());
            }
        }
    }

    /**
     * Builds the factory used to create binding runtimes.
     *
     * <p>Package-private and overridable so startup-rollback behaviour can be
     * tested with a factory that fails for a chosen binding. Constructing the
     * real factory requires a live MQ connection manager, which would put the
     * rollback path out of reach of any test.
     */
    /** Installs the factory an overridden {@link #initializeRuntimeFactory()} wants to use. */
    void setRuntimeFactoryForTest(BindingRuntimeFactory factory) {
        this.runtimeFactory = factory;
    }

    void initializeRuntimeFactory() {
        AuditRecordEmitter auditEmitter = new HdfsAuditRecordEmitter(
                fileSystem,
                properties.getHdfs().getAuditBasePath(),
                instanceId.value(),
                Clock.systemUTC());

        this.runtimeFactory = new BindingRuntimeFactory(
                fileSystem,
                hadoopConf,
                mqConnectionManager,
                serializerFactory,
                trackerBuilderFactory,
                metricsRegistry,
                healthManager,
                auditEmitter,
                instanceId.value());
    }

    /**
     * Creates and starts every binding, or leaves none of them running.
     *
     * <p>Application startup is transactional. Previously, if binding C failed
     * after A and B had started, the exception propagated but A and B were left
     * consuming: threads polling MQ, landing data and committing, inside an
     * application whose startup had failed. Spring does not clean that up
     * either — {@code running} is still false at that point, so
     * {@code DefaultLifecycleProcessor} does not consider this bean started and
     * never calls {@link #stop()}. The result was a half-live service with no
     * owner.
     *
     * <p>Stopping the started bindings drains them through the normal path, so
     * in-flight batches are still resolved rather than abandoned. Shared
     * resources — the MQ connection manager and the HDFS {@code FileSystem} —
     * are deliberately left alone: they are context-scoped beans that Spring
     * disposes when the failed context closes, and they may be shared with
     * anything else in that context.
     */
    /** Stops every started binding; used when startup fails after they started. */
    private void stopAllRuntimesQuietly() {
        long drainTimeout = properties.getShutdown().getDrainTimeoutMs();
        if (reconciliationScheduler != null) {
            reconciliationScheduler.close();
        }
        for (BindingRuntime runtime : runtimes.values()) {
            try {
                runtime.stop(drainTimeout);
                healthManager.recordStopped(runtime.getBindingId());
            } catch (Exception e) {
                log.error("Failed to stop binding '{}' during late-startup rollback: {}",
                        runtime.getBindingId(), e.getMessage(), e);
            }
        }
        runtimes.clear();
    }

    /**
     * Starts periodic reconciliation — the check half of ABC.
     *
     * <p>Started after the bindings, and deliberately unable to affect them:
     * it holds no JMS session, and every failure inside it is contained. A
     * mechanism that checks ingestion must never be able to stop it.
     */
    /**
     * Package-private and overridable for the same reason as
     * {@link #initializeRuntimeFactory()}: the late-startup rollback path must
     * be testable, and nothing in the real method can be made to throw on
     * demand.
     */
    void startReconciliation() {
        reconciliationScheduler = ReconciliationFactory.createScheduler(
                fileSystem,
                hadoopConf,
                properties,
                instanceId.value(),
                metricsRegistry::getBindingMetrics,
                Clock.systemUTC());
        reconciliationScheduler.start();
    }

    private void createAndStartRuntimes() {
        List<BindingRuntime> startedThisAttempt = new ArrayList<>();

        for (BindingConfig binding : properties.getBindings()) {
            try {
                BindingRuntime runtime = runtimeFactory.create(binding);
                runtimes.put(binding.getId(), runtime);
                runtime.start();
                startedThisAttempt.add(runtime);
                healthManager.recordHealthy(binding.getId());

            } catch (BindingRuntimeFactory.BindingRuntimeCreationException e) {
                String error = "Failed to create runtime for binding '" + binding.getId() + "': " + e.getMessage();
                log.error(error, e);
                startupErrors.add(error);
                healthManager.recordUnhealthy(binding.getId(), e);

            } catch (BindingRuntime.BindingStartupException e) {
                String error = "Failed to start binding '" + binding.getId() + "': " + e.getMessage();
                log.error(error, e);
                startupErrors.add(error);
                healthManager.recordUnhealthy(binding.getId(), e);
            }
        }

        if (!startupErrors.isEmpty()) {
            RuntimeException failure = new RuntimeException(
                    "Failed to start " + startupErrors.size() + " binding(s): " + startupErrors);
            rollbackStartedBindings(startedThisAttempt, failure);
            throw failure;
        }
    }

    /**
     * Stops bindings that started before the failure, without losing the
     * original cause.
     *
     * <p>Cleanup failures are logged and attached as suppressed exceptions
     * rather than thrown: the reason startup failed is more useful to whoever
     * is reading the log than the reason cleanup was untidy, and throwing here
     * would replace the former with the latter.
     */
    private void rollbackStartedBindings(List<BindingRuntime> started, RuntimeException failure) {
        if (started.isEmpty()) {
            return;
        }

        log.error("Startup failed — stopping {} binding(s) that had already started, so the "
                + "application does not leave listeners consuming after a failed startup",
                started.size());

        long drainTimeout = properties.getShutdown().getDrainTimeoutMs();

        for (BindingRuntime runtime : started) {
            try {
                runtime.stop(drainTimeout);
                healthManager.recordStopped(runtime.getBindingId());
            } catch (Exception e) {
                log.error("Failed to stop binding '{}' while rolling back startup: {}",
                        runtime.getBindingId(), e.getMessage(), e);
                failure.addSuppressed(e);
            }
        }

        // Safe to call twice: BindingRuntime.stop() is a no-op once the state
        // is not RUNNING, so a later stop() during context shutdown is inert.
        runtimes.clear();
    }

    @Override
    public void stop() {
        log.info("Stopping IntakeRuntimeManager");
        running = false;

        // Stopped before the bindings: it only reads HDFS, and there is no
        // reason to have it examining partitions while they drain.
        if (reconciliationScheduler != null) {
            reconciliationScheduler.close();
        }

        long drainTimeout = properties.getShutdown().getDrainTimeoutMs();

        for (BindingRuntime runtime : runtimes.values()) {
            try {
                runtime.stop(drainTimeout);
            } catch (Exception e) {
                log.error("Error stopping binding '{}': {}", runtime.getBindingId(), e.getMessage(), e);
            } finally {
                // Recorded even when stop() failed: after shutdown, a snapshot
                // still showing UNHEALTHY/DEGRADED reads as a live problem.
                healthManager.recordStopped(runtime.getBindingId());
            }
        }

        runtimes.clear();
        log.info("IntakeRuntimeManager stopped");
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

    /**
     * Returns the runtime for a specific binding, or null if not found.
     */
    public BindingRuntime getRuntime(String bindingId) {
        return runtimes.get(bindingId);
    }

    /**
     * Returns all active runtimes.
     */
    public Map<String, BindingRuntime> getRuntimes() {
        return Collections.unmodifiableMap(runtimes);
    }

    /**
     * Returns the total number of receive loops across all bindings.
     */
    public int getTotalLoopCount() {
        return runtimes.values().stream()
                .mapToInt(BindingRuntime::getLoopCount)
                .sum();
    }

    /**
     * Returns the metrics registry.
     */
    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    /**
     * Returns the health manager.
     */
    public BindingHealthManager getHealthManager() {
        return healthManager;
    }

    /**
     * Returns any startup errors that occurred.
     */
    public List<String> getStartupErrors() {
        return List.copyOf(startupErrors);
    }

    /**
     * Returns true if the runtime factory has been initialized.
     */
    boolean isFactoryInitialized() {
        return runtimeFactory != null;
    }

    /**
     * Returns the runtime factory (for testing).
     */
    BindingRuntimeFactory getRuntimeFactory() {
        return runtimeFactory;
    }
}
