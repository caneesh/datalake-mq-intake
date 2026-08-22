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
 *   <li>Restore normal batch size after M consecutive successes</li>
 * </ul>
 */
public class DegradedModeManager {

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
        this.bindingId = bindingId;
        this.normalBatchSize = normalBatchSize;
        this.strategy = strategy;
        this.successesRequiredToRestore = successesRequiredToRestore;
        this.currentBatchSize = new AtomicInteger(normalBatchSize);
        this.classifier = new FailureClassifier();
    }

    /**
     * Records a successful batch commit.
     * May restore normal batch size after enough consecutive successes.
     */
    public void recordSuccess() {
        if (!inDegradedMode.get()) {
            return;
        }

        int successes = consecutiveSuccesses.incrementAndGet();
        log.debug("Binding '{}': consecutive successes in degraded mode: {}/{}",
                bindingId, successes, successesRequiredToRestore);

        if (successes >= successesRequiredToRestore) {
            restore();
        }
    }

    /**
     * Records a failure and determines if degraded mode should be entered/deepened.
     *
     * @param throwable the failure
     * @return the classified failure
     */
    public FailureClass recordFailure(Throwable throwable) {
        consecutiveSuccesses.set(0);

        FailureClass failureClass = classifier.classify(throwable);

        if (failureClass.triggersDegradedMode()) {
            enterOrDeepenDegradedMode();
        } else {
            log.debug("Binding '{}': {} failure does not trigger degraded mode",
                    bindingId, failureClass);
        }

        return failureClass;
    }

    /**
     * Enters degraded mode or deepens it (for BISECT strategy).
     */
    private void enterOrDeepenDegradedMode() {
        int oldBatchSize = currentBatchSize.get();
        int newBatchSize = strategy.degrade(oldBatchSize, normalBatchSize);

        if (!inDegradedMode.getAndSet(true)) {
            log.warn("Binding '{}': ENTERING degraded mode. Strategy={}, batch size {} -> {}",
                    bindingId, strategy, normalBatchSize, newBatchSize);
        } else {
            degradationLevel.incrementAndGet();
            log.warn("Binding '{}': DEEPENING degraded mode. Level={}, batch size {} -> {}",
                    bindingId, degradationLevel.get(), oldBatchSize, newBatchSize);
        }

        currentBatchSize.set(newBatchSize);
        consecutiveSuccesses.set(0);
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
    public int getCurrentBatchSize() {
        return currentBatchSize.get();
    }

    /**
     * Returns true if currently in degraded mode.
     */
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
