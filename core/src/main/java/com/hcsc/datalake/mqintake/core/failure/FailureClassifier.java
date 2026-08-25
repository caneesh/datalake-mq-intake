package com.hcsc.datalake.mqintake.core.failure;

import com.hcsc.datalake.mqintake.core.failure.classification.RuleBasedFailureClassifier;

/**
 * Decides what kind of failure a throwable represents.
 *
 * <p>From DESIGN.md §6.2: misclassify an infrastructure failure as poison and
 * the binding crawls at batch-of-1 through an outage; misclassify a poison
 * message as infrastructure and it rolls back forever.
 *
 * <p>An interface so callers depend on the decision rather than on how it is
 * reached, and so a binding can be given a different policy without touching
 * the code that consumes the answer.
 *
 * <p><strong>Unknown failures must never trigger degraded mode.</strong>
 */
public interface FailureClassifier {

    FailureClass classify(Throwable throwable);

    /** The standard rule chain. */
    static FailureClassifier defaultClassifier() {
        return new RuleBasedFailureClassifier();
    }

    /**
     * Marker for message-level data exceptions. Implementations throw this for
     * payload-specific failures.
     */
    class MessageDataException extends RuntimeException {
        public MessageDataException(String message) {
            super(message);
        }

        public MessageDataException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
