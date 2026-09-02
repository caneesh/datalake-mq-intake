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
import java.util.concurrent.atomic.AtomicInteger;
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

    private static final int MAX_RECONNECT_ATTEMPTS = 10;

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
    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicLong reconnectCount = new AtomicLong(0);

    /** This thread's JMS resources; see ListenerSession for the invariant. */
    private final ListenerSession listenerSession;
    private volatile Thread loopThread;

    /** Loop-thread-confined; see UNRECOGNISED_FAULTS_BEFORE_FORCED_RECOVERY. */
    private int consecutiveUnrecognisedFaults = 0;

    /** Drives the log cadence in recordTrackerSuppressed; the metric is the signal. */
    private final AtomicLong suppressedTrackers = new AtomicLong(0);

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
                    config.getId(), commitCount.get(), rollbackCount.get(), messageCount.get());
        }
    }

    private void runLoop() {
        FlushTrigger flushTrigger = new FlushTrigger(
                config.getBatch().getSize(),
                config.getBatch().getBytes(),
                config.getBatch().getIntervalMs(),
                flushClock
        );
        List<Message> batch = new ArrayList<>(config.getBatch().getSize());

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                pollOnce(batch, flushTrigger);
                consecutiveUnrecognisedFaults = 0; // the session demonstrably works
            } catch (JMSException e) {
                if (!surviveFault(e, batch, flushTrigger)) {
                    break;
                }
            }
        }

        drainOnShutdown(batch, flushTrigger);
    }

    /**
     * One receive: accumulate the message, if any, and flush when a batch
     * boundary is hit.
     */
    private void pollOnce(List<Message> batch, FlushTrigger flushTrigger) throws JMSException {
        int effectiveBatchSize = getEffectiveBatchSize();
        Message message = listenerSession.consumer().receive(receiveTimeoutMs);

        if (message != null) {
            // A message arriving after the window turned belongs to the NEXT
            // partition, so the accumulated batch is flushed BEFORE it joins:
            // the flush is then stamped with its own window, and this message
            // opens a batch anchored to the new one. Adding first and flushing
            // afterwards would put both windows' messages in one file under
            // one of the two partitions. A no-op until the batch has opened,
            // so the first message of a batch never triggers it.
            if (flushTrigger.isPartitionBoundaryCrossed()) {
                flushBatch(batch, flushTrigger);
            }

            batch.add(message);
            flushTrigger.trackMessage(message);
            // In-flight batch depth. One atomic store per message,
            // negligible beside the JMS receive that produced it.
            metrics.setCurrentBatchSize(batch.size());

            if (batch.size() >= effectiveBatchSize || flushTrigger.shouldFlush()) {
                flushBatch(batch, flushTrigger);
            }
        } else if (!batch.isEmpty() && flushTrigger.shouldFlush()) {
            // Idle poll. shouldFlush() rather than isTimeoutExpired()
            // so the partition boundary is noticed here too — otherwise
            // a quiet batch would sit until the next message arrived,
            // and land in a later partition than the one it belongs to.
            flushBatch(batch, flushTrigger);
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
    private boolean surviveFault(JMSException e, List<Message> batch, FlushTrigger flushTrigger) {
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
        handleFailure(e);
        rollbackQuietly();
        batch.clear();
        flushTrigger.reset();
        // Without this the gauge kept reporting the rolled-back
        // batch's size through the whole reconnect backoff —
        // a phantom stuck batch on the dashboard.
        metrics.setCurrentBatchSize(0);

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
            if (!recoverSession()) {
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
            if (!recoverSession()) {
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
    private void drainOnShutdown(List<Message> batch, FlushTrigger flushTrigger) {
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
            processBatch(batch, flushTrigger.getBatchAnchor());
        } catch (Exception e) {
            log.warn("Failed to drain batch on shutdown for binding '{}': {}",
                    config.getId(), e.getMessage());
            rollbackQuietly();
        }
    }

    /** Processes the accumulated batch and resets the accumulation state. */
    private void flushBatch(List<Message> batch, FlushTrigger flushTrigger) {
        // Anchor read BEFORE reset(): reset re-anchors the trigger to now,
        // which for a partition-triggered flush is already the next window.
        processBatch(batch, flushTrigger.getBatchAnchor());
        batch.clear();
        flushTrigger.reset();
        metrics.setCurrentBatchSize(0);
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

    private void processBatch(List<Message> batch, java.time.Instant partitionInstant) {
        int batchSize = batch.size();
        log.debug("Processing batch of {} messages for binding '{}'",
                batchSize, config.getId());

        // Collect IDs BEFORE any send: routing a message to the BOQ via
        // producer.send() assigns it a NEW JMSMessageID, so IDs read afterward
        // would no longer match the suspect entries recorded at failure time
        List<String> batchMessageIds =
                degradedModeManager != null ? collectMessageIds(batch) : List.of();

        List<Message> cleanMessages = batch;
        boolean committed = false;
        int poisonCount = 0;
        try {
            if (poisonMessageHandler != null) {
                PoisonMessageHandler.BatchPoisonCheckResult poisonResult = screenForPoison(batch);
                poisonCount = poisonResult.getPoisonCount();
                cleanMessages = poisonResult.getCleanMessages();

                if (cleanMessages.isEmpty()) {
                    // Nothing landed, but messages were consumed. Without a
                    // record of its own this unit of work appears in no audit
                    // anywhere and the balance shows those messages as lost.
                    verifyAbcBalance(batchSize, 0, poisonCount);
                    emitBackoutOnlyAudit(batch, poisonCount, batchSize);
                    commitSession();
                    committed = true;
                    recordBackoutOnlyCommit(batchSize, batchMessageIds);
                    return;
                }
            }

            BatchWriter.BatchWriteResult writeResult =
                    writeBatchToHdfs(cleanMessages, partitionInstant);

            // ABC balance, before anything is sent or committed: every message
            // taken off the queue must be observed either in the file (the
            // writer's per-append count) or on the BOQ (the screen's routing
            // count). All three numbers are independent observations — none is
            // computed from the others. Scope honestly stated: the current
            // SequenceFileBatchWriter is all-or-nothing (any failed append
            // throws before a result exists), so with today's writer this
            // check guards loop/wiring bugs, a future writer that can return
            // partial counts, and screen implementations that break the
            // clean+routed=batch partition — not a partial write the current
            // writer already turns into a hard rollback.
            verifyAbcBalance(batchSize, writeResult.getRecordCount(), poisonCount);

            if (config.getMode() == BindingMode.TRACKED) {
                sendTrackerMessages(cleanMessages);
            }

            // Audit BEFORE commit. Written afterwards, a crash or an audit-store
            // outage in between leaves committed data with no record, which a
            // balancing control reads as loss — the wrong direction to fail in.
            // Written first, the same crash yields an audited file whose
            // messages are redelivered: a duplicate, which is detectable and
            // true. Under ABC the audit is a control, so this is also the point
            // at which an unwritable audit stops the batch rather than being
            // logged and forgotten.
            emitAuditRecord(cleanMessages, writeResult, poisonCount, batchSize);

            commitSession();
            committed = true;
            recordCommittedBatch(batchSize, cleanMessages, writeResult, batchMessageIds);

        } catch (Exception e) {
            // "Did the commit happen?" is the pivotal question of the whole
            // transaction design, so it stays visible here rather than inside
            // a handler. The committed flag is set in this method, directly
            // after each commitSession() call, for the same reason: set
            // anywhere else, a failure in between would send an
            // already-committed batch down the rollback path below.
            if (committed) {
                containPostCommitFailure(e, cleanMessages.size());
            } else {
                rollBackFailedBatch(e, batchSize, batchMessageIds);
            }
        }
    }

    /**
     * A failure after the commit. The unit of work is already durable and
     * acknowledged, so there is nothing to undo: this must not roll back and
     * must not mark the batch suspect — those IDs are off the queue, so they
     * can never be redelivered, clearSuspects() could never retire them, and
     * DegradedModeManager would refuse to restore normal batch size for the
     * life of the process.
     */
    private void containPostCommitFailure(Exception e, int committedCount) {
        log.error("Post-commit bookkeeping failed for binding '{}' after committing "
                        + "{} messages — delivery is unaffected: {}",
                config.getId(), committedCount, e.getMessage(), e);
    }

    /**
     * A failure before the commit. Rolling back puts every message of the
     * unit of work back on the queue for redelivery; nothing is lost.
     */
    private void rollBackFailedBatch(Exception e, int batchSize, List<String> batchMessageIds) {
        log.error("Batch processing failed for binding '{}', rolling back {} messages: {}",
                config.getId(), batchSize, e.getMessage(), e);
        handleFailure(e, batchMessageIds);
        rollbackQuietly();
        metrics.recordRollback();
    }

    /**
     * Routes poison messages to the backout queue and returns the screen
     * result. A failure to route MUST propagate: rolling the batch back is
     * what keeps the message on the queue instead of dropping it.
     */
    private PoisonMessageHandler.BatchPoisonCheckResult screenForPoison(List<Message> batch)
            throws PoisonMessageHandler.BackoutFailureException {
        PoisonMessageHandler.BatchPoisonCheckResult poisonResult;
        try {
            poisonResult = poisonMessageHandler.screen(listenerSession.session(), batch);
        } catch (PoisonMessageHandler.BackoutFailureException e) {
            log.error("Backout queue routing failed for binding '{}', rolling back: {}",
                    config.getId(), e.getMessage(), e);
            throw e; // Re-throw to trigger rollback in processBatch's catch
        }

        if (poisonResult.hasPoisonMessages()) {
            log.warn("Routed {} poison messages to backout queue for binding '{}'",
                    poisonResult.getPoisonCount(), config.getId());
            for (int i = 0; i < poisonResult.getPoisonCount(); i++) {
                metrics.recordPoisonMessageRouted();
            }
        }
        return poisonResult;
    }

    /**
     * The durable write: serialize, _tmp write, flush, close, rename into the
     * partition. Flush latency covers the whole of it — the part that
     * dominates batch time and the first thing to look at when throughput
     * drops.
     */
    private BatchWriter.BatchWriteResult writeBatchToHdfs(List<Message> cleanMessages,
                                                          java.time.Instant partitionInstant)
            throws BatchWriter.BatchWriteException {
        long flushStartNanos = System.nanoTime();
        BatchWriter.BatchWriteResult writeResult =
                batchWriter.write(config.getId(), cleanMessages, partitionInstant);
        metrics.recordFlushLatency(Duration.ofNanos(System.nanoTime() - flushStartNanos));
        return writeResult;
    }

    /**
     * The acknowledgement to MQ. Everything before this call can roll back;
     * nothing after it can.
     *
     * <p>Deliberately contains ONLY the commit: the caller flips its
     * {@code committed} flag on the very next statement, and any code that
     * ran here between the commit and the return would execute before that
     * flip — if it could throw, an already-committed batch would take the
     * rollback path and poison the suspect set.
     */
    private void commitSession() throws JMSException {
        listenerSession.session().commit();
    }

    /** Post-commit bookkeeping for a unit of work that landed nothing. */
    private void recordBackoutOnlyCommit(int batchSize, List<String> batchMessageIds) {
        commitCount.incrementAndGet();
        // The shared metrics must advance here too: this branch used to update
        // only the loop's internal counter, so dashboards undercounted commits
        // and consumption exactly when poison was churning.
        metrics.recordCommit();
        metrics.recordMessagesConsumed(batchSize);
        if (degradedModeManager != null) {
            degradedModeManager.clearSuspects(batchMessageIds);
        }
        handleSuccess();
    }

    /** Post-commit bookkeeping for a normally landed batch. */
    private void recordCommittedBatch(int batchSize, List<Message> cleanMessages,
                                      BatchWriter.BatchWriteResult writeResult,
                                      List<String> batchMessageIds) {
        commitCount.incrementAndGet();
        messageCount.addAndGet(cleanMessages.size());

        // Counted at commit, not at receive. A rolled-back batch is
        // redelivered, so counting on receive would tally the same message
        // repeatedly and overstate throughput during poison isolation.
        // batchSize rather than cleanMessages.size(): messages routed to
        // the BOQ in this unit of work were also consumed.
        metrics.recordMessagesConsumed(batchSize);

        // Committed messages (including any routed to BOQ in this unit of
        // work) are no longer suspects
        if (degradedModeManager != null) {
            degradedModeManager.clearSuspects(batchMessageIds);
        }

        handleSuccess();

        metrics.recordCommit();
        metrics.recordMessagesWritten(cleanMessages.size(), writeResult.getByteCount());

        log.debug("Committed batch of {} messages for binding '{}'",
                cleanMessages.size(), config.getId());
    }

    /** Failure with no batch context (e.g. a fault in receive() itself). */
    private void handleFailure(Throwable e) {
        handleFailure(e, null);
    }

    private void handleFailure(Throwable e, List<String> failedBatchMessageIds) {
        metrics.setHealthy(false);
        if (degradedModeManager == null) {
            return;
        }

        // Data failures mark the failed batch's message IDs as suspects so
        // the bisection coordinator can track them across redelivery to
        // any listener thread (§6.1). Marking is done inside recordFailure
        // so it cannot be separated from the degraded-mode transition — and
        // the entry edge is reported from inside that same transition, so
        // racing listener threads record it exactly once.
        DegradationPolicy.FailureResult result =
                degradedModeManager.recordFailure(e, failedBatchMessageIds);
        log.debug("Failure classified as {} for binding '{}'",
                result.getFailureClass(), config.getId());
        metrics.setSuspectCount(degradedModeManager.getSuspectCount());

        if (result.enteredDegradedMode() && healthManager != null) {
            healthManager.recordDegraded(config.getId(),
                    "Entered degraded mode due to " + result.getFailureClass() + " failure");
            metrics.recordDegradedModeEntry();
        }
    }

    /**
     * Collects JMS message IDs from a batch, skipping unreadable ones.
     */
    private List<String> collectMessageIds(List<Message> batch) {
        List<String> ids = new ArrayList<>(batch.size());
        for (Message message : batch) {
            try {
                String id = message.getJMSMessageID();
                if (id != null) {
                    ids.add(id);
                }
            } catch (JMSException e) {
                log.debug("Could not read JMSMessageID: {}", e.getMessage());
            }
        }
        return ids;
    }

    private void handleSuccess() {
        metrics.setHealthy(true);
        if (degradedModeManager == null) {
            return;
        }

        metrics.setSuspectCount(degradedModeManager.getSuspectCount());

        boolean exitedDegradedMode = degradedModeManager.recordSuccess();
        if (exitedDegradedMode && healthManager != null) {
            healthManager.recordHealthy(config.getId());
            metrics.recordDegradedModeExit();
        }
    }

    /**
     * The transaction-time ABC balance check: every message consumed must be
     * observed either written to HDFS or routed to the BOQ, or the batch does
     * not commit.
     *
     * <p>Gated per binding ({@code audit.balance-check-enabled}); RMS runs it,
     * Claims keeps its existing behaviour. Throwing here lands in
     * processBatch's catch before the commit, so the whole unit of work rolls
     * back and MQ redelivers — the standard at-least-once path, no bespoke
     * retry. The already-landed file (the write precedes this check) becomes a
     * design-permitted duplicate on redelivery, exactly as for any other
     * post-write pre-commit failure.
     *
     * @param mqConsumedCount  size of the batch as received from MQ
     * @param hdfsWrittenCount records the writer observed itself append
     * @param backoutCount     messages the screen observed itself route
     */
    private void verifyAbcBalance(int mqConsumedCount, int hdfsWrittenCount, int backoutCount)
            throws AbcBalanceException {
        if (!config.getAudit().isBalanceCheckEnabled()) {
            return;
        }

        int balanceDelta = mqConsumedCount - hdfsWrittenCount - backoutCount;
        if (balanceDelta == 0) {
            return;
        }

        metrics.recordBalanceCheckFailure();
        log.error("ABC balance check FAILED for binding '{}': consumed {} != written {} + "
                        + "backout {} (delta {}). Rolling back — an unbalanced batch must "
                        + "never be committed. The messages return to the queue for redelivery.",
                config.getId(), mqConsumedCount, hdfsWrittenCount, backoutCount, balanceDelta);
        throw new AbcBalanceException(String.format(
                "ABC balance violated for binding '%s': consumed=%d, hdfsWritten=%d, "
                        + "backout=%d, delta=%d",
                config.getId(), mqConsumedCount, hdfsWrittenCount, backoutCount, balanceDelta));
    }

    /** An unbalanced unit of work; deliberately not a data failure. */
    public static class AbcBalanceException extends Exception {
        public AbcBalanceException(String message) {
            super(message);
        }
    }

    /**
     * Writes the batch's audit record, before the commit.
     *
     * <p>Whether a failure here stops the batch is
     * {@code fail_batch_on_audit_error}, default true: under ABC the audit is
     * a control, and committing without one produces data no balance can
     * account for. Rolling back leaves the messages on the queue, so nothing
     * is lost — ingestion stalls until the audit path recovers.
     */
    private void emitAuditRecord(List<Message> messages, BatchWriter.BatchWriteResult writeResult,
                                 int backoutCount, int mqConsumedCount) throws Exception {
        try {
            auditRecordEmitter.emit(config.getId(), writeResult, messages, backoutCount,
                    mqConsumedCount);
        } catch (Exception e) {
            metrics.recordAuditFailure();
            if (config.getAudit().isFailBatchOnError()) {
                log.error("Audit record could not be written for binding '{}' — rolling back so "
                                + "no unaudited data is committed: {}",
                        config.getId(), e.getMessage(), e);
                throw e;
            }
            log.warn("Failed to emit audit record for binding '{}' — batch still commits, "
                            + "this landing is unaudited: {}",
                    config.getId(), e.getMessage());
        }
    }

    /** Audit for a unit of work that landed nothing because every message was poison. */
    private void emitBackoutOnlyAudit(List<Message> batch, int backoutCount, int mqConsumedCount)
            throws Exception {
        if (backoutCount == 0) {
            return;
        }

        try {
            auditRecordEmitter.emitBackoutOnly(config.getId(), batch, backoutCount,
                    mqConsumedCount);
        } catch (Exception e) {
            metrics.recordAuditFailure();
            if (config.getAudit().isFailBatchOnError()) {
                log.error("Backout-only audit record could not be written for binding '{}' — "
                        + "rolling back: {}", config.getId(), e.getMessage(), e);
                throw e;
            }
            log.warn("Failed to emit backout-only audit record for binding '{}': {}",
                    config.getId(), e.getMessage());
        }
    }

    /**
     * Sends one tracker message per source message.
     *
     * <p><strong>Two failure kinds, handled differently, because they have
     * different blast radii.</strong>
     *
     * <p><em>Infrastructure ({@code JMSException}) fails the batch.</em> The
     * provider refusing a put — tracker queue full, message too big for it,
     * producer broken — is not about this message; it will refuse the next one
     * too. Rolling back stalls the feed and pages, which is right: the
     * alternative is landing every message with its acknowledgement silently
     * dropped, for as long as the condition lasts. It also classifies as
     * MQ_INFRASTRUCTURE, which does NOT permit backout routing, so a stall
     * cannot divert healthy messages to the BOQ.
     *
     * <p><em>Content ({@code RuntimeException}) is skipped and counted.</em> A
     * malformed header that breaks the rewrite — {@code HeaderRewriter} runs
     * {@code replaceAll}, so tag content with regex metacharacters can throw —
     * affects exactly one message. Failing the batch for it would be actively
     * worse: such a failure classifies as UNKNOWN, which never triggers
     * degraded mode, so there is no bisection to isolate the culprit; the
     * batch would roll back at full size until delivery count pushed the
     * WHOLE batch past BOTHRESH and onto the backout queue. One bad header
     * would cost a thousand healthy messages a manual replay. Skipping loses
     * that one tracker notification instead, which is what the legacy MDB
     * does.
     *
     * <p>{@code fail_batch_on_tracker_error} escalates the content case to
     * match the infrastructure one. It no longer means "never fail the batch"
     * when false — infrastructure failures always do.
     *
     * <p>Unlike the MDB, every outcome is counted: sent, suppressed, and
     * failed. Matching its delivery behaviour is deliberate, inheriting its
     * blindness is not.
     */
    private void sendTrackerMessages(List<Message> batch) throws JMSException {
        for (Message sourceMessage : batch) {
            try {
                Optional<Message> trackerMessage =
                        trackerMessageBuilder.build(listenerSession.session(), sourceMessage);

                if (trackerMessage.isPresent()) {
                    listenerSession.trackerProducer().send(trackerMessage.get());
                    metrics.recordTrackerSent();
                } else {
                    recordTrackerSuppressed();
                }
            } catch (JMSException e) {
                metrics.recordTrackerFailure();
                log.error("Tracker put failed for binding '{}' — rolling back rather than "
                                + "landing messages whose acknowledgement was dropped. Check the "
                                + "tracker queue '{}' for depth, MAXDEPTH and MAXMSGL: {}",
                        config.getId(), config.getTracker().getQueue(), e.getMessage(), e);
                throw e;
            } catch (RuntimeException e) {
                if (config.getTracker().isFailBatchOnError()) {
                    throw e;
                }
                metrics.recordTrackerFailure();
                log.warn("Tracker message could not be built for binding '{}' — this one "
                                + "message still commits and its tracker notification is lost: {}",
                        config.getId(), e.getMessage());
            }
        }
    }

    /**
     * A source message the builder declined to track.
     *
     * <p>Legitimate per message (RMS suppresses anything without
     * MessageHeaderDetails, which is what keeps claims-shaped messages off the
     * tracker queue) and a serious condition in bulk: if upstream stops
     * setting that property, every message lands and none is acknowledged.
     * Logged first-and-every-thousandth so a systemic regression is visible
     * without a malformed flood becoming its own log problem; the counter
     * behind it is the thing to alert on.
     */
    private void recordTrackerSuppressed() {
        metrics.recordTrackerSuppressed();
        long suppressed = suppressedTrackers.incrementAndGet();
        if (suppressed == 1 || suppressed % 1000 == 0) {
            log.warn("Binding '{}': {} message(s) landed with NO tracker notification — the "
                            + "builder found no MessageHeaderDetails to rewrite. Isolated cases "
                            + "are expected; a climbing count means data is landing "
                            + "unacknowledged.",
                    config.getId(), suppressed);
        }
    }

    private void rollbackQuietly() {
        try {
            if (listenerSession.isOpen()) {
                listenerSession.session().rollback();
                rollbackCount.incrementAndGet();
            }
        } catch (JMSException e) {
            log.warn("Failed to rollback session for binding '{}': {}",
                    config.getId(), e.getMessage());
        }
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

    /** The outcome of one recovery attempt. */
    private enum RecoveryOutcome {
        /** The session is open again; the receive loop can resume. */
        RECOVERED,
        /** This attempt failed but the next one might not. */
        RETRY,
        /** Stop recovering: budget exhausted, fatal fault, or shutting down. */
        GIVE_UP
    }

    /**
     * Attempts to recover the session with bounded exponential backoff.
     *
     * <p>Iterative on purpose. The previous version retried by recursing,
     * which was safe only because the budget is a hardcoded 10 — each level's
     * stack frame stays live for the entire remaining backoff. The moment the
     * budget becomes configurable (a natural next step), recursion depth
     * scales with it. Same behaviour, loop instead of stack.
     *
     * @return true if recovery succeeded, false if recovery failed after max
     *         attempts, hit a fatal fault, or was interrupted
     */
    private boolean recoverSession() {
        RecoveryOutcome outcome;
        do {
            outcome = recoverOnce();
        } while (outcome == RecoveryOutcome.RETRY);
        return outcome == RecoveryOutcome.RECOVERED;
    }

    /** One recovery attempt: close, back off, reopen. */
    private RecoveryOutcome recoverOnce() {
        int attempts = reconnectAttempts.incrementAndGet();

        if (attempts > MAX_RECONNECT_ATTEMPTS) {
            log.error("Max reconnect attempts ({}) exceeded for binding '{}'",
                    MAX_RECONNECT_ATTEMPTS, config.getId());
            if (healthManager != null) {
                healthManager.recordUnhealthy(config.getId(),
                        new RuntimeException("Max reconnect attempts exceeded"));
            }
            metrics.recordReconnectFailure();
            return RecoveryOutcome.GIVE_UP;
        }

        log.warn("Attempting session recovery for binding '{}' (attempt {}/{})",
                config.getId(), attempts, MAX_RECONNECT_ATTEMPTS);

        // Update health to RECOVERING
        if (healthManager != null) {
            healthManager.recordRecovering(config.getId(),
                    String.format("Session reconnect attempt %d/%d", attempts, MAX_RECONNECT_ATTEMPTS));
        }

        // Close existing resources
        listenerSession.close();

        // Calculate backoff with jitter
        long backoffMs = backoffPolicy.backoffFor(attempts).toMillis();

        log.debug("Waiting {}ms before reconnect attempt {} for binding '{}'",
                backoffMs, attempts, config.getId());

        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("Reconnect wait interrupted for binding '{}'", config.getId());
            return RecoveryOutcome.GIVE_UP;
        }

        // Check if we should still be running
        if (!running.get() || Thread.currentThread().isInterrupted()) {
            log.info("Recovery aborted - loop stopping for binding '{}'", config.getId());
            return RecoveryOutcome.GIVE_UP;
        }

        try {
            listenerSession.open();
            // Fresh budget for the next incident. Plain state now — the
            // accessor and the next recovery read it — not the success signal
            // it once doubled as.
            reconnectAttempts.set(0);
            reconnectCount.incrementAndGet();
            log.info("Session recovered successfully for binding '{}' after {} attempt(s)",
                    config.getId(), attempts);

            metrics.recordReconnect();

            // Restore health to HEALTHY after successful recovery
            if (healthManager != null) {
                healthManager.recordHealthy(config.getId());
            }

            return RecoveryOutcome.RECOVERED;
        } catch (JMSException e) {
            log.error("Session recovery attempt {} failed for binding '{}': {}",
                    attempts, config.getId(), e.getMessage());

            if (faultPolicy.isFatal(e)) {
                log.error("Non-recoverable error detected for binding '{}', stopping recovery",
                        config.getId());
                return RecoveryOutcome.GIVE_UP;
            }

            return RecoveryOutcome.RETRY;
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getCommitCount() {
        return commitCount.get();
    }

    public long getRollbackCount() {
        return rollbackCount.get();
    }

    public long getMessageCount() {
        return messageCount.get();
    }

    public String getBindingId() {
        return config.getId();
    }

    public long getReconnectCount() {
        return reconnectCount.get();
    }

    public int getCurrentReconnectAttempts() {
        return reconnectAttempts.get();
    }
}
