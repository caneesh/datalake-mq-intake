package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;

import java.util.Optional;

/**
 * Recognises one category of failure.
 *
 * <p>One rule per {@link FailureClass}, so each has a single reason to change:
 * what counts as an HDFS problem is decided in exactly one place, and adding a
 * category means adding a class rather than another branch in a chain.
 *
 * <p>A rule that does not recognise a throwable returns empty and the next rule
 * is consulted — the classifier is a chain of responsibility, and the order of
 * that chain is part of the contract (see {@link RuleBasedFailureClassifier}).
 */
public interface FailureRule {

    /**
     * @return the class this rule assigns, or empty if it does not recognise
     *         the throwable
     */
    Optional<FailureClass> classify(Throwable throwable);

    /** The class this rule assigns, for diagnostics and ordering assertions. */
    FailureClass failureClass();

    /**
     * True when the rule can only reach its verdict by reading the message
     * text. Message matching is the weakest signal available and has already
     * caused one real misclassification, so the classifier asserts that no
     * text-based rule outranks a type-based one.
     */
    default boolean reliesOnMessageText() {
        return false;
    }
}
