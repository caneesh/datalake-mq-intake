package com.hcsc.datalake.mqintake.core.failure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages degraded batch mode for a binding.
 *
 * <p>From DESIGN.md §6.1: Degraded mode ensures the poison message is the
 * only message in its unit of work, so only it accumulates backout count.
 *
 * <p>Key behaviors:
 * <ul>
 *   <li>Only MESSAGE_DATA failures trigger degraded mode</li>
 *   <li>UNKNOWN failures NEVER trigger degraded mode</li>
 *   <li>Restore normal batch size after M consecutive successes AND once no
 *       suspect messages remain unresolved</li>
 * </ul>
 *
 * <p><strong>Suspect isolation (bisection coordinator):</strong> Classic
 * in-memory bisection of a failed batch is unsafe under MQ, because after
 * rollback the broker may redistribute the batch's messages to other listener
 * threads. Instead, this manager is binding-scoped (shared by all loops) and
 * tracks the JMS message IDs of failed batches as <em>suspects</em>:
 * <ul>
 *   <li>A data failure marks the whole failed batch suspect and shrinks the
 *       shared batch size (BISECT halves it)</li>
 *   <li>Any listener that later commits a batch clears those IDs from the
 *       suspect set — good subsets commit in batches, on any thread</li>
 *   <li>Batches containing the true poison keep failing and halving until the
 *       poison is alone in its unit of work; its backout count then breaches
 *       BOTHRESH and the PoisonMessageHandler routes it to the BOQ</li>
 *   <li>Normal batch size is restored only after the suspect set is empty and
 *       M consecutive successes have occurred</li>
 * </ul>
 * This is redistribution-safe: suspects are keyed by message ID, not by which
 * thread received them. Complexity for one poison in a batch of N is
 * O(log N) failing transactions, with clean subsets committing at the current
 * bisected size rather than one-by-one.
 */
public class DegradedModeManager implements DegradationPolicy {

    private static final Logger log = LoggerFactory.getLogger(DegradedModeManager.class);

    private final String bindingId;
    private final int normalBatchSize;
    private final DegradationStrategy strategy;
    private final int successesRequiredToRestore;
    private final FailureClassifier classifier;

    private final AtomicBoolean inDegradedMode = new AtomicBoolean(false);
    private final AtomicInteger currentBatchSize;
    private final AtomicInteger consecutiveSuccesses = new AtomicInteger(0);
    private final AtomicInteger degradationLevel = new AtomicInteger(0);

    /**
     * JMS message IDs from batches that failed with a data failure and have
     * not yet been part of a committed batch or routed to the BOQ.
     * Shared across all listener threads of the binding.
     */
    private final java.util.Set<String> suspectMessageIds =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Creates a degraded mode manager.
     *
     * @param bindingId                  the binding identifier
     * @param normalBatchSize            the normal (non-degraded) batch size
     * @param strategy                   the degradation strategy (BATCH_OF_ONE or BISECT)
     * @param successesRequiredToRestore consecutive successes needed to restore normal mode
     */
    public DegradedModeManager(String bindingId,
                                int normalBatchSize,
                                DegradationStrategy strategy,
                                int successesRequiredToRestore) {
        this(bindingId, normalBatchSize, strategy, successesRequiredToRestore,
                FailureClassifier.defaultClassifier());
    }

    /** Visible for testing: lets a concurrency test block inside classify(). */
    DegradedModeManager(String bindingId,
                        int normalBatchSize,
                        DegradationStrategy strategy,
                        int successesRequiredToRestore,
                        FailureClassifier classifier) {
        this.bindingId = bindingId;
        this.normalBatchSize = normalBatchSize;
        this.strategy = strategy;
        this.successesRequiredToRestore = successesRequiredToRestore;
        this.currentBatchSize = new AtomicInteger(normalBatchSize);
        this.classifier = classifier;
    }

    /**
     * Records a successful batch commit.
     * May restore normal batch size after enough consecutive successes,
     * but never while suspect messages remain unresolved.
     *
     * @return true when this call restored normal mode. Detected inside the
     *         lock, so exactly one of any racing callers observes the edge.
     */
    @Override
    // recordSuccess, recordFailure and clearSuspects are synchronized. Each
    // individual field is already thread-safe, but the TRANSITIONS were not:
    // recordSuccess's "suspects empty + enough successes -> restore" could
    // interleave with a concurrent recordFailure between its classify and its
    // markBatchSuspect, restoring full batch size in a window the design says
    // must not exist. Failures and commits are batch-cadence events, not
    // per-message, so an uncontended lock here costs nanoseconds.
    public synchronized boolean recordSuccess() {
        if (!inDegradedMode.get()) {
            return false;
        }

        int successes = consecutiveSuccesses.incrementAndGet();
        log.debug("Binding '{}': consecutive successes in degraded mode: {}/{} ({} suspects outstanding)",
                bindingId, successes, successesRequiredToRestore, suspectMessageIds.size());

        if (successes >= successesRequiredToRestore && suspectMessageIds.isEmpty()) {
            restore();
            return true;
        }
        return false;
    }

    /**
     * Marks the message IDs of a failed batch as suspects. Called by any
     * listener thread whose batch failed with a data-classified failure.
     * Redistribution-safe: another thread committing these messages later
     * clears them via {@link #clearSuspects}.
     */
    public void markBatchSuspect(java.util.Collection<String> messageIds) {
        for (String id : messageIds) {
            if (id != null) {
                suspectMessageIds.add(id);
            }
        }
        log.warn("Binding '{}': marked {} messages suspect ({} total outstanding)",
                bindingId, messageIds.size(), suspectMessageIds.size());
    }

    /**
     * Clears message IDs from the suspect set after they were part of a
     * committed batch (or routed to the BOQ). Called by whichever listener
     * thread the broker redelivered them to.
     */
    @Override
    public synchronized void clearSuspects(java.util.Collection<String> messageIds) {
        boolean removed = false;
        for (String id : messageIds) {
            if (id != null && suspectMessageIds.remove(id)) {
                removed = true;
            }
        }
        if (removed) {
            log.info("Binding '{}': cleared suspects, {} outstanding",
                    bindingId, suspectMessageIds.size());
        }
    }

    /**
     * Returns the number of unresolved suspect messages.
     */
    public int getSuspectCount() {
        return suspectMessageIds.size();
    }

    /**
     * Returns true if the given message ID is a known suspect.
     */
    public boolean isSuspect(String messageId) {
        return messageId != null && suspectMessageIds.contains(messageId);
    }

    /**
     * Records a failure and determines if degraded mode should be entered/deepened.
     *
     * @param throwable the failure
     * @return the classification, plus whether this call entered degraded mode
     */
    public FailureResult recordFailure(Throwable throwable) {
        return recordFailure(throwable, null);
    }

    /**
     * Records a failure and, when it is data-classified, marks the failed
     * batch's message IDs suspect as part of the same state change.
     *
     * <p>The two must happen together. Marking suspects in a separate call
     * after this one leaves a window in which another listener thread's
     * {@link #recordSuccess()} sees degraded mode, enough consecutive
     * successes, and an empty suspect set, and restores the normal batch size
     * while this failure's messages are still in flight — putting the poison
     * message straight back into a full-size batch.
     *
     * @param throwable         the failure
     * @param batchMessageIds   IDs of the failed batch, or null if unknown
     * @return the classification, plus whether this call entered degraded
     *         mode — decided under the lock, so racing failures report the
     *         entry edge exactly once
     */
    @Override
    public synchronized FailureResult recordFailure(Throwable throwable,
                                      java.util.Collection<String> batchMessageIds) {
        FailureClass failureClass = classifier.classify(throwable);
        boolean entered = false;

        if (failureClass.triggersDegradedMode()) {
            // Suspects first: once they are registered, no concurrent
            // recordSuccess() can satisfy the "no suspects outstanding"
            // half of the restore condition.
            if (batchMessageIds != null) {
                markBatchSuspect(batchMessageIds);
            }
            entered = enterOrDeepenDegradedMode();
        } else {
            // Deliberately does NOT reset consecutiveSuccesses: an unrelated
            // infrastructure blip while we are isolating a poison message
            // should not discard progress toward restoring the normal batch
            // size, or routine HDFS flakiness would pin the binding in
            // reduced-throughput mode indefinitely.
            log.debug("Binding '{}': {} failure does not trigger degraded mode",
                    bindingId, failureClass);
        }

        return new FailureResult(failureClass, entered);
    }

    /**
     * Enters degraded mode or deepens it (for BISECT strategy).
     *
     * @return true when this call ENTERED degraded mode; false when it
     *         deepened an already-degraded binding
     */
    private boolean enterOrDeepenDegradedMode() {
        // CAS rather than get/compute/set: two threads failing concurrently —
        // the normal case, since MQ redistributes a rolled-back batch across
        // listener threads — would otherwise both read the same size, compute
        // the same halved value, and both store it, losing one step of
        // bisection while degradationLevel still counted two.
        int oldBatchSize;
        int newBatchSize;
        do {
            oldBatchSize = currentBatchSize.get();
            newBatchSize = strategy.degrade(oldBatchSize, normalBatchSize);
        } while (!currentBatchSize.compareAndSet(oldBatchSize, newBatchSize));

        consecutiveSuccesses.set(0);

        // Publish the smaller batch size before announcing degraded mode, so a
        // concurrent getCurrentBatchSize() can never observe "degraded" while
        // still returning the old, larger size.
        boolean entered = !inDegradedMode.getAndSet(true);
        if (entered) {
            log.warn("Binding '{}': ENTERING degraded mode. Strategy={}, batch size {} -> {}",
                    bindingId, strategy, normalBatchSize, newBatchSize);
        } else {
            degradationLevel.incrementAndGet();
            log.warn("Binding '{}': DEEPENING degraded mode. Level={}, batch size {} -> {}",
                    bindingId, degradationLevel.get(), oldBatchSize, newBatchSize);
        }
        return entered;
    }

    /**
     * Restores normal batch size.
     */
    private void restore() {
        log.info("Binding '{}': EXITING degraded mode. Restoring batch size to {}",
                bindingId, normalBatchSize);

        inDegradedMode.set(false);
        currentBatchSize.set(normalBatchSize);
        consecutiveSuccesses.set(0);
        degradationLevel.set(0);
    }

    /**
     * Returns the current effective batch size.
     */
    @Override
    public int getCurrentBatchSize() {
        return currentBatchSize.get();
    }

    /**
     * Returns true if currently in degraded mode.
     */
    @Override
    public boolean isInDegradedMode() {
        return inDegradedMode.get();
    }

    /**
     * Returns the normal (non-degraded) batch size.
     */
    public int getNormalBatchSize() {
        return normalBatchSize;
    }

    /**
     * Returns the degradation strategy.
     */
    public DegradationStrategy getStrategy() {
        return strategy;
    }

    /**
     * Returns the current degradation level (for BISECT).
     */
    public int getDegradationLevel() {
        return degradationLevel.get();
    }

    /**
     * Returns the number of consecutive successes in degraded mode.
     */
    public int getConsecutiveSuccesses() {
        return consecutiveSuccesses.get();
    }
}
