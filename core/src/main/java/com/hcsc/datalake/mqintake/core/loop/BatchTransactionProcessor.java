package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradationPolicy;
import com.hcsc.datalake.mqintake.core.loop.session.ListenerSession;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.poison.PoisonScreen;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One batch, from screening to commit.
 *
 * <p>This is the unit of work, and its ORDER is the delivery guarantee:
 *
 * <pre>
 *   screen for poison -> write -> check the balance -> acknowledge downstream
 *   -> audit -> commit
 * </pre>
 *
 * <p>Extracted from the receive loop last and most carefully, because nothing
 * here is arbitrary. Each step's comment records why it sits where it does,
 * and those reasons moved across with the code unchanged. In particular:
 *
 * <ul>
 *   <li>Message identifiers are collected BEFORE anything is sent, because a
 *       send assigns a new one.</li>
 *   <li>The balance check runs before the acknowledgement and the audit, so a
 *       batch that does not balance has done nothing externally visible.</li>
 *   <li>The audit is written before the commit, so a crash between them yields
 *       a detectable duplicate rather than unaccounted data.</li>
 *   <li>{@code commitSession()} contains only the commit, and the caller flips
 *       its {@code committed} flag on the very next statement.</li>
 * </ul>
 *
 * <p>Those four are pinned by {@code LoopInvariantCharacterisationTest}, which
 * was written before this extraction precisely because the suite did not hold
 * them. Anything moved here should keep failing those tests when broken.
 *
 * <p>Confined to one listener thread, along with the session it commits.
 */
class BatchTransactionProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchTransactionProcessor.class);

    private final BindingConfig config;
    private final ListenerSession listenerSession;
    private final BatchWriter batchWriter;
    private final TrackerMessageBuilder trackerMessageBuilder;
    private final PoisonScreen poisonMessageHandler;
    private final DegradationPolicy degradedModeManager;
    private final AuditRecordEmitter auditRecordEmitter;
    private final BindingMetrics metrics;
    private final LoopStateReporter reporter;

    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong messageCount = new AtomicLong(0);

    /** Drives the log cadence in recordTrackerSuppressed; the metric is the signal. */
    private final AtomicLong suppressedTrackers = new AtomicLong(0);

    /**
     * No argument validation, deliberately. The loop never checked these, and
     * adding a null check here turns a construction that used to succeed into
     * a startup failure — a behavioural change smuggled in as tidiness. An
     * extraction has to accept exactly what the original accepted.
     */
    BatchTransactionProcessor(BindingConfig config,
                              ListenerSession listenerSession,
                              BatchWriter batchWriter,
                              TrackerMessageBuilder trackerMessageBuilder,
                              PoisonScreen poisonMessageHandler,
                              DegradationPolicy degradedModeManager,
                              AuditRecordEmitter auditRecordEmitter,
                              BindingMetrics metrics,
                              LoopStateReporter reporter) {
        this.config = config;
        this.listenerSession = listenerSession;
        this.batchWriter = batchWriter;
        this.trackerMessageBuilder = trackerMessageBuilder;
        this.poisonMessageHandler = poisonMessageHandler;
        this.degradedModeManager = degradedModeManager;
        this.auditRecordEmitter = auditRecordEmitter;
        this.metrics = metrics;
        this.reporter = reporter;
    }

    long getCommitCount() {
        return commitCount.get();
    }

    long getRollbackCount() {
        return rollbackCount.get();
    }

    long getMessageCount() {
        return messageCount.get();
    }

    void processBatch(List<Message> batch) {
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

            BatchWriter.BatchWriteResult writeResult = writeBatchToHdfs(cleanMessages);

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
        reporter.batchFailed(e);
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
    private BatchWriter.BatchWriteResult writeBatchToHdfs(List<Message> cleanMessages)
            throws BatchWriter.BatchWriteException {
        long flushStartNanos = System.nanoTime();
        BatchWriter.BatchWriteResult writeResult = batchWriter.write(config.getId(), cleanMessages);
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
    void handleFailure(Throwable e) {
        handleFailure(e, null);
    }
    private void handleFailure(Throwable e, List<String> failedBatchMessageIds) {
        reporter.unhealthy();
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
        reporter.suspects(degradedModeManager.getSuspectCount());

        if (result.enteredDegradedMode()) {
            reporter.enteredDegradedMode(result.getFailureClass());
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
        reporter.healthy();
        reporter.batchProgressed();
        if (degradedModeManager == null) {
            return;
        }

        reporter.suspects(degradedModeManager.getSuspectCount());

        if (degradedModeManager.recordSuccess()) {
            reporter.exitedDegradedMode();
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
    void rollbackQuietly() {
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
}
