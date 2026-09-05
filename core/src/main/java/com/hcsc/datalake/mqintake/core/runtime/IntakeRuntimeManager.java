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
import java.util.function.Supplier;

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

    /**
     * Builds the two collaborators that need live infrastructure.
     *
     * <p>Injected rather than constructed inline so a test can supply its own
     * without subclassing this class. Both default to the real thing; see the
     * package-private constructor.
     */
    private final Supplier<BindingRuntimeFactory> runtimeFactorySupplier;
    private final Supplier<ReconciliationScheduler> reconciliationSchedulerSupplier;
    private final Map<String, BindingRuntime> runtimes = new ConcurrentHashMap<>();
    private final List<String> startupErrors = Collections.synchronizedList(new ArrayList<>());

    private volatile boolean running = false;
    private volatile BindingRuntimeFactory runtimeFactory;
    private volatile ReconciliationScheduler reconciliationScheduler;
    private volatile com.hcsc.datalake.mqintake.core.lifecycle.StagingLifecycleManager staging;

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
                                 @Autowired(required = false) TrackerMessageBuilderFactory trackerBuilderFactory,
                                 @Autowired(required = false)
                                 com.hcsc.datalake.mqintake.core.security.KerberosManager kerberosManager) {
        this(properties, fileSystem, hadoopConf, mqConnectionManager, serializerFactory,
                bindingConfigValidator, healthManager, productionMode, instanceId,
                trackerBuilderFactory, kerberosManager, null, null);
    }

    /**
     * The constructor everything else runs through, with both
     * infrastructure-bound collaborators injectable.
     *
     * <p>Either supplier may be null, which means "build the real one". The
     * real ones cannot be passed in from a test: a
     * {@link BindingRuntimeFactory} needs a live MQ connection manager, and
     * {@link ReconciliationFactory} needs a live filesystem, so both are built
     * here from state this class already holds — but building them is now the
     * default rather than the only option. That is what closes the two seams
     * this class used to leave open: package-private methods a test subclass
     * overrode, which made a subclass of the production lifecycle root part of
     * the test harness.
     *
     * <p>Suppliers rather than instances because both are built during
     * {@link #start()}, after validation has passed, and a manager that is
     * constructed but never started must not have created either.
     */
    IntakeRuntimeManager(IntakeProperties properties,
                         FileSystem fileSystem,
                         Configuration hadoopConf,
                         MqConnectionProvider mqConnectionManager,
                         RecordSerializerFactory serializerFactory,
                         BindingConfigValidator bindingConfigValidator,
                         BindingHealthManager healthManager,
                         ProductionMode productionMode,
                         InstanceId instanceId,
                         TrackerMessageBuilderFactory trackerBuilderFactory,
                         com.hcsc.datalake.mqintake.core.security.KerberosManager kerberosManager,
                         Supplier<BindingRuntimeFactory> runtimeFactorySupplier,
                         Supplier<ReconciliationScheduler> reconciliationSchedulerSupplier) {
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
        // The gauge read a counter nothing incremented. Real relogin failures
        // land on KerberosManager's own counter; MetricsRegistry has a
        // separate one whose recordKerberosReloginFailure() has no production
        // caller, so mq_intake_kerberos_relogin_failures published 0.0 for the
        // life of the process however many relogins failed — an alert that
        // could never fire, which reads as a healthy identity.
        //
        // Wired here rather than in the bridge because this class owns the
        // registry. Absent when Kerberos is disabled, in which case the gauge
        // keeps reporting the registry's own zero, which is then true.
        if (kerberosManager != null) {
            this.metricsRegistry.setKerberosReloginFailureSupplier(
                    kerberosManager::getReloginFailureCount);
        }
        this.healthManager = healthManager;

        // Bound to this instance here, not called here: both run during
        // start().
        this.runtimeFactorySupplier = runtimeFactorySupplier != null
                ? runtimeFactorySupplier : this::defaultRuntimeFactory;
        this.reconciliationSchedulerSupplier = reconciliationSchedulerSupplier != null
                ? reconciliationSchedulerSupplier : this::defaultReconciliationScheduler;
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
            // Claim before sweep — see StagingLifecycleManager, which is
            // where that ordering now lives.
            staging = new com.hcsc.datalake.mqintake.core.lifecycle.StagingLifecycleManager(
                    fileSystem, instanceId.value(), properties.getHdfs());
            staging.claim(properties.getBindings());
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
        // Dev-placeholder MQ defaults (localhost/QM1/DEV.APP.SVRCONN) resolve
        // when the manifest forgets the env vars; they are non-blank, so the
        // sanity rules alone would pass them and production would quietly
        // point at a dev queue manager.
        com.hcsc.datalake.mqintake.core.config.DevDefaultConnectionGate
                .failOnDevDefaults(productionMode, properties.getMqConnections());
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

    /** Obtains the factory used to create binding runtimes. */
    private void initializeRuntimeFactory() {
        this.runtimeFactory = runtimeFactorySupplier.get();
    }

    /** The real factory: everything a binding runtime needs, from live infrastructure. */
    private BindingRuntimeFactory defaultRuntimeFactory() {
        AuditRecordEmitter auditEmitter = new HdfsAuditRecordEmitter(
                fileSystem,
                properties.getHdfs().getAuditBasePath(),
                instanceId.value(),
                Clock.systemUTC());

        return new BindingRuntimeFactory(
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
    private void startReconciliation() {
        reconciliationScheduler = reconciliationSchedulerSupplier.get();
        reconciliationScheduler.start();
    }

    /** The real scheduler, read-only over the same filesystem the bindings write to. */
    private ReconciliationScheduler defaultReconciliationScheduler() {
        return ReconciliationFactory.createScheduler(
                fileSystem,
                hadoopConf,
                properties,
                instanceId.value(),
                metricsRegistry::getBindingMetrics,
                Clock.systemUTC());
    }

    private void createAndStartRuntimes() {
        List<BindingRuntime> startedThisAttempt = new ArrayList<>();

        for (BindingConfig binding : properties.getBindings()) {
            try {
                BindingRuntime runtime = runtimeFactory.create(binding);
                runtime.start();
                // In the map only once RUNNING. Registered before start(), a
                // runtime whose start() failed stayed in the map in FAILED
                // state — unreachable by stop()'s RUNNING→STOPPING guard and
                // invisible to the rollback list, which only holds successful
                // starts. (The runtime also cleans itself up on a failed
                // start; this ordering keeps the map an inventory of running
                // bindings rather than attempts.)
                runtimes.put(binding.getId(), runtime);
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

        // Released last, after the drain: while a batch may still be staging a
        // file, this instance's directory must not look abandoned to a peer.
        // Dropping it here makes the directory reclaimable immediately on a
        // clean shutdown rather than after the lease timeout.
        if (staging != null) {
            staging.close();
            staging = null;
        }

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
        // Preflight probes dependencies and exits. Starting listeners would
        // consume real messages from a real queue, which a diagnostic must
        // never do.
        if (properties.getPreflight().isEnabled()) {
            log.info("Preflight mode: listeners will NOT start and nothing will be consumed");
            return false;
        }
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
