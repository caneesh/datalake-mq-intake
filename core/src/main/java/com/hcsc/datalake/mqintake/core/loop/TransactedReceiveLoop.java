package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradationPolicy;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.loop.recovery.BackoffPolicy;
import com.hcsc.datalake.mqintake.core.loop.session.ListenerSession;
import com.hcsc.datalake.mqintake.core.loop.recovery.SessionFaultPolicy;
import com.hcsc.datalake.mqintake.core.loop.recovery.SessionRecoveryCoordinator;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.poison.PoisonScreen;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hand-rolled transacted receive loop for MQ message consumption.
 *
 * <p>This is the core of the system. Key invariants:
 * <ul>
 *   <li>One transacted Session per listener thread (no sharing)</li>
 *   <li>MessageConsumer and MessageProducer created from the same Session</li>
 *   <li>Bounded receive(timeout) for predictable shutdown interruption</li>
 *   <li>Any failure before commit rolls back the entire batch</li>
 *   <li>No partial commit, no per-message acknowledgement</li>
 * </ul>
 *
 * <p>DO NOT use @JmsListener, DefaultMessageListenerContainer, or JmsTemplate here.
 * They impose per-message transaction boundaries which defeats the batching design.
 */
public class TransactedReceiveLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TransactedReceiveLoop.class);

    /**
     * Consecutive unrecognised JMS faults tolerated before the loop stops
     * trusting the fault policy's "not broken" verdict and forces a session
     * rebuild anyway. Guards the policy's documented blind spot: a
     * permanently broken session whose exception text matches neither the
     * BROKEN nor the FATAL matchers would otherwise fail-pause-retry forever
     * — zero throughput, health endpoint stale-healthy, supervisor blind
     * (the thread never dies), only the metrics gauge and log volume hinting.
     * Forcing recovery is safe in both directions: a genuinely broken
     * session gets rebuilt (bounded, alertable, budget-limited); a healthy
     * session pays one cheap close/reopen every N faults.
     */
    private static final int UNRECOGNISED_FAULTS_BEFORE_FORCED_RECOVERY = 10;

    private final BindingConfig config;
    private final Connection connection;
    private final BatchWriter batchWriter;
    private final TrackerMessageBuilder trackerMessageBuilder;
    private final PoisonScreen poisonMessageHandler;
    private final DegradationPolicy degradedModeManager;
    private final BindingHealthManager healthManager;
    private final AuditRecordEmitter auditRecordEmitter;
    private final BindingMetrics metrics;
    private final String instanceId;
    private final long receiveTimeoutMs;

    /** How to react to a JMS fault, and how long to wait before retrying. */
    private final SessionFaultPolicy faultPolicy;
    private final BackoffPolicy backoffPolicy;

    /**
     * Clock behind the FlushTrigger — and therefore behind the partition
     * window a batch is stamped with. Injectable so a test can cross a
     * quarter-hour boundary without waiting one.
     */
    private final Clock flushClock;

    private final AtomicBoolean running = new AtomicBoolean(false);

    /** This thread's JMS resources; see ListenerSession for the invariant. */
    private final ListenerSession listenerSession;

    /** Rebuilds the session after a fault; owns the retry budget and backoff. */
    private final SessionRecoveryCoordinator recoveryCoordinator;

    /** Translates what this listener observes into health and metric transitions. */
    private final LoopStateReporter reporter;

    /** One batch, from screening to commit. Owns the order the guarantee rests on. */
    private final BatchTransactionProcessor processor;
    private volatile Thread loopThread;

    /** Loop-thread-confined; see UNRECOGNISED_FAULTS_BEFORE_FORCED_RECOVERY. */
    private int consecutiveUnrecognisedFaults = 0;



    /**
     * Creates a receive loop for the given binding.
     *
     * @param config                the binding configuration
     * @param connection            shared JMS connection (thread-safe)
     * @param batchWriter           writer for HDFS batches
     * @param trackerMessageBuilder builder for tracker messages (null for LAND_ONLY)
     * @param poisonMessageHandler  handler for poison messages (null to disable)
     * @param degradedModeManager   manager for degraded batch mode (null to disable)
     * @param healthManager         health manager for binding health updates (null to disable)
     * @param auditRecordEmitter    emitter for audit records (null for a no-op emitter)
     * @param metrics               binding metrics (null for a no-op sink)
     * @param instanceId            instance identifier for audit records
     * @param receiveTimeoutMs      timeout for receive() calls
     */
    public TransactedReceiveLoop(BindingConfig config,
                                  Connection connection,
                                  BatchWriter batchWriter,
                                  TrackerMessageBuilder trackerMessageBuilder,
                                  PoisonScreen poisonMessageHandler,
                                  DegradationPolicy degradedModeManager,
                                  BindingHealthManager healthManager,
                                  AuditRecordEmitter auditRecordEmitter,
                                  BindingMetrics metrics,
                                  String instanceId,
                                  long receiveTimeoutMs) {
        this(config, connection, batchWriter, trackerMessageBuilder, poisonMessageHandler,
                degradedModeManager, healthManager, auditRecordEmitter, metrics,
                instanceId, receiveTimeoutMs, null, null, null, null);
    }

    /**
     * Visible for testing: lets a test inject a fault-injecting session, a
     * fault policy with known classifications, and a fast backoff — the only
     * way to drive the recovery state machine (RETRY→RECOVERED, budget
     * exhaustion, fatal-mid-recovery) without a real queue-manager outage.
     * The clock drives the FlushTrigger, which is the only way to cross a
     * quarter-hour partition boundary in a test without waiting for one.
     * Null means the production default.
     */
    TransactedReceiveLoop(BindingConfig config,
                          Connection connection,
                          BatchWriter batchWriter,
                          TrackerMessageBuilder trackerMessageBuilder,
                          PoisonScreen poisonMessageHandler,
                          DegradationPolicy degradedModeManager,
                          BindingHealthManager healthManager,
                          AuditRecordEmitter auditRecordEmitter,
                          BindingMetrics metrics,
                          String instanceId,
                          long receiveTimeoutMs,
                          ListenerSession listenerSession,
                          SessionFaultPolicy faultPolicy,
                          BackoffPolicy backoffPolicy,
                          Clock flushClock) {
        this.config = config;
        this.connection = connection;
        this.batchWriter = batchWriter;
        this.trackerMessageBuilder = trackerMessageBuilder;
        this.poisonMessageHandler = poisonMessageHandler;
        this.degradedModeManager = degradedModeManager;
        this.healthManager = healthManager;
        // Normalised to no-ops rather than null-checked at every recording
        // site: both are pure sinks with no behaviour to disable, unlike the
        // handler/manager collaborators above, whose absence changes what the
        // loop does and stays an explicit null check.
        this.auditRecordEmitter =
                auditRecordEmitter != null ? auditRecordEmitter : AuditRecordEmitter.noop();
        this.metrics = metrics != null ? metrics : BindingMetrics.noop();
        this.instanceId = instanceId;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.listenerSession = listenerSession != null
                ? listenerSession : new ListenerSession(connection, config);
        this.faultPolicy = faultPolicy != null
                ? faultPolicy : SessionFaultPolicy.defaultPolicy();
        this.backoffPolicy = backoffPolicy != null
                ? backoffPolicy : BackoffPolicy.exponentialWithJitter();
        this.flushClock = flushClock != null ? flushClock : Clock.systemUTC();
        this.reporter = new LoopStateReporter(config.getId(), healthManager, this.metrics);
        this.processor = new BatchTransactionProcessor(
                config, this.listenerSession, batchWriter, trackerMessageBuilder,
                poisonMessageHandler, degradedModeManager, this.auditRecordEmitter,
                this.metrics, this.reporter);
        this.recoveryCoordinator = new SessionRecoveryCoordinator(
                config.getId(), this.listenerSession, this.faultPolicy, this.backoffPolicy,
                healthManager, this.metrics, running::get);

        if (config.getMode() == BindingMode.TRACKED && trackerMessageBuilder == null) {
            throw new IllegalArgumentException(
                    "TRACKED binding '" + config.getId() + "' requires a TrackerMessageBuilder");
        }
    }

    @Override
    public void run() {
        loopThread = Thread.currentThread();
        String threadName = "recv-" + config.getId() + "-" + Thread.currentThread().getId();
        Thread.currentThread().setName(threadName);
        reporter.listenerStarted(threadName);

        log.info("Starting receive loop for binding '{}' on thread {}",
                config.getId(), threadName);

        try {
            listenerSession.open();
            running.set(true);
            runLoop();
        } catch (JMSException e) {
            log.error("Failed to initialize session for binding '{}': {}",
                    config.getId(), e.getMessage(), e);
        } finally {
            cleanup();
            log.info("Receive loop stopped for binding '{}'. Commits: {}, Rollbacks: {}, Messages: {}",
                    config.getId(), processor.getCommitCount(), processor.getRollbackCount(),
                    processor.getMessageCount());
        }
    }

    private void runLoop() {
        BatchAccumulator batch = new BatchAccumulator(
                config.getBatch().getSize(),
                config.getBatch().getBytes(),
                config.getBatch().getIntervalMs(),
                flushClock,
                metrics);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                pollOnce(batch);
                consecutiveUnrecognisedFaults = 0; // the session demonstrably works
            } catch (JMSException e) {
                if (!surviveFault(e, batch)) {
                    break;
                }
            }
        }

        drainOnShutdown(batch);
    }

    /**
     * One receive: accumulate the message, if any, and flush when a batch
     * boundary is hit.
     */
    private void pollOnce(BatchAccumulator batch) throws JMSException {
        int effectiveBatchSize = getEffectiveBatchSize();
        Message message = listenerSession.consumer().receive(receiveTimeoutMs);

        if (message != null) {
            batch.add(message);

            if (batch.size() >= effectiveBatchSize || batch.shouldFlush()) {
                flushBatch(batch);
            }
        } else if (!batch.isEmpty() && batch.shouldFlush()) {
            // Idle poll. shouldFlush() rather than isTimeoutExpired()
            // so the partition boundary is noticed here too — otherwise
            // a quiet batch would sit until the next message arrived,
            // and land in a later partition than the one it belongs to.
            flushBatch(batch);
        }
    }

    /**
     * Fault triage after a JMS failure: discard the in-flight batch (a
     * rollback puts its messages back on the queue for redelivery), then
     * decide whether the loop can keep running.
     *
     * @return true to keep looping; false to stop — a fatal fault, or
     *         recovery that failed after exhausting its budget
     */
    private boolean surviveFault(JMSException e, BatchAccumulator batch) {
        if (!running.get()) {
            // Shutdown-time: stop() flips running before interrupting,
            // so an exception raised by the interrupt lands here. It
            // used to vanish without a trace — if the same exception
            // had actually signalled a real broker fault coinciding
            // with shutdown, there was no forensic record at all.
            log.debug("JMS exception during shutdown for binding '{}' (expected if "
                    + "raised by the stop interrupt): {}", config.getId(), e.getMessage());
            return true; // the loop condition sees running=false and exits
        }

        log.error("JMS error in receive loop for binding '{}': {}",
                config.getId(), e.getMessage(), e);
        processor.handleFailure(e);
        processor.rollbackQuietly();
        // Resets the messages, the trigger and the in-flight gauge together.
        // Without the gauge reset it kept reporting the rolled-back batch's
        // size through the whole reconnect backoff — a phantom stuck batch on
        // the dashboard.
        batch.reset();

        if (faultPolicy.isFatal(e)) {
            // Reconnecting cannot fix bad credentials or denied
            // access; checking only mid-retry paid for one doomed
            // close + backoff + open before giving up.
            log.error("Fatal (non-recoverable) JMS fault for binding '{}', "
                    + "stopping loop: {}", config.getId(), e.getMessage());
            running.set(false);
            return false;
        }

        if (faultPolicy.requiresRecovery(e)) {
            consecutiveUnrecognisedFaults = 0;
            if (!recoveryCoordinator.recover()) {
                log.error("Session recovery failed for binding '{}', stopping loop",
                        config.getId());
                running.set(false);
                return false;
            }
            return true;
        }

        // The fault policy's own documented blind spot: a genuinely broken
        // session whose error text matches nothing. One-off faults get a
        // short pause and a retry; a RUN of them means the policy's verdict
        // is wrong, and the loop escalates to a forced session rebuild —
        // without this bound, an unmatched permanent fault stalled the
        // listener forever with the health endpoint still reporting healthy.
        consecutiveUnrecognisedFaults++;
        if (consecutiveUnrecognisedFaults >= UNRECOGNISED_FAULTS_BEFORE_FORCED_RECOVERY) {
            log.error("Binding '{}': {} consecutive unrecognised JMS faults — the session is "
                            + "presumed broken despite the fault policy; forcing recovery",
                    config.getId(), consecutiveUnrecognisedFaults);
            consecutiveUnrecognisedFaults = 0;
            if (!recoveryCoordinator.recover()) {
                log.error("Forced session recovery failed for binding '{}', stopping loop",
                        config.getId());
                running.set(false);
                return false;
            }
            return true;
        }
        pauseAfterUnrecognisedFault();
        return true;
    }

    /** Lands whatever is still accumulated when the loop stops. */
    private void drainOnShutdown(BatchAccumulator batch) {
        // stop() interrupts this thread to break the blocking receive(). The
        // drain below has to commit, and the IBM MQ client can fail in-flight
        // calls when the calling thread is still marked interrupted — which
        // would roll the final batch back on every clean shutdown instead of
        // landing it. Clear the flag before attempting the commit.
        Thread.interrupted();

        if (batch.isEmpty()) {
            return;
        }

        log.info("Draining {} messages on shutdown for binding '{}'",
                batch.size(), config.getId());
        try {
            processor.processBatch(batch.messages());
        } catch (Exception e) {
            log.warn("Failed to drain batch on shutdown for binding '{}': {}",
                    config.getId(), e.getMessage());
            processor.rollbackQuietly();
        }
    }

    /** Processes the accumulated batch and resets the accumulation state. */
    private void flushBatch(BatchAccumulator batch) {
        processor.processBatch(batch.messages());
        batch.reset();
    }

    /** Brief pause after a JMS fault the policy does not classify as broken. */
    private void pauseAfterUnrecognisedFault() {
        try {
            Thread.sleep(Math.min(500, Math.max(50, receiveTimeoutMs / 2)));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private int getEffectiveBatchSize() {
        if (degradedModeManager != null) {
            return degradedModeManager.getCurrentBatchSize();
        }
        return config.getBatch().getSize();
    }





















    public void stop() {
        log.info("Stopping receive loop for binding '{}'", config.getId());
        running.set(false);

        if (loopThread != null) {
            loopThread.interrupt();
        }
    }

    /**
     * Protected: a fault-injection test subclass overrides this, but a public
     * accessor handed the thread-confined transacted Session to any caller on
     * any thread — a standing-constraint violation waiting for a caller.
     */
    protected Session getSession() {
        return listenerSession.session();
    }

    private void cleanup() {
        running.set(false);
        listenerSession.close();
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getCommitCount() {
        return processor.getCommitCount();
    }

    public long getRollbackCount() {
        return processor.getRollbackCount();
    }

    public long getMessageCount() {
        return processor.getMessageCount();
    }

    public String getBindingId() {
        return config.getId();
    }

    public long getReconnectCount() {
        return recoveryCoordinator.getReconnectCount();
    }

    public int getCurrentReconnectAttempts() {
        return recoveryCoordinator.getCurrentAttempts();
    }
}
