package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;

import java.util.Objects;
import java.util.Optional;

/**
 * A {@link FailureRule} expressed as a {@link ThrowableMatcher}.
 *
 * <p>Every rule in the default chain has the same shape — match, then assign a
 * class — so the shape lives here once and each rule supplies only what makes
 * it different.
 */
public class MatcherFailureRule implements FailureRule {

    private final FailureClass failureClass;
    private final ThrowableMatcher matcher;
    private final boolean reliesOnMessageText;

    public MatcherFailureRule(FailureClass failureClass, ThrowableMatcher matcher) {
        this(failureClass, matcher, false);
    }

    public MatcherFailureRule(FailureClass failureClass, ThrowableMatcher matcher,
                              boolean reliesOnMessageText) {
        this.failureClass = Objects.requireNonNull(failureClass, "failureClass required");
        this.matcher = Objects.requireNonNull(matcher, "matcher required");
        this.reliesOnMessageText = reliesOnMessageText;
    }

    @Override
    public Optional<FailureClass> classify(Throwable throwable) {
        return matcher.matches(throwable) ? Optional.of(failureClass) : Optional.empty();
    }

    @Override
    public FailureClass failureClass() {
        return failureClass;
    }

    @Override
    public boolean reliesOnMessageText() {
        return reliesOnMessageText;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + failureClass + ")";
    }
}
