package com.hcsc.datalake.mqintake.core.failure.classification;

import java.util.Arrays;
import java.util.Objects;

/**
 * A composable test for recognising a kind of {@link Throwable}.
 *
 * <p>Failure classification is mostly the same three questions asked over and
 * over: is it one of these types, does its class name contain one of these
 * fragments, does its message contain one of these phrases. Written out
 * longhand that produced dozens of nested conditionals in which the actual
 * rule — and, more importantly, its priority relative to the other rules — was
 * invisible. Expressed as matchers, each rule reads as the statement it is.
 *
 * <p>Deliberately not {@code java.util.function.Predicate}: a named domain
 * type keeps {@code and}/{@code or} composition readable at the call site and
 * stops arbitrary predicates leaking into classification.
 */
@FunctionalInterface
public interface ThrowableMatcher {

    boolean matches(Throwable throwable);

    default ThrowableMatcher or(ThrowableMatcher other) {
        Objects.requireNonNull(other, "other required");
        return t -> matches(t) || other.matches(t);
    }

    default ThrowableMatcher and(ThrowableMatcher other) {
        Objects.requireNonNull(other, "other required");
        return t -> matches(t) && other.matches(t);
    }

    default ThrowableMatcher negate() {
        return t -> !matches(t);
    }

    /** Matches when the throwable is an instance of any listed type. */
    static ThrowableMatcher anyType(Class<?>... types) {
        Class<?>[] copy = Arrays.copyOf(types, types.length);
        return t -> {
            for (Class<?> type : copy) {
                if (type.isInstance(t)) {
                    return true;
                }
            }
            return false;
        };
    }

    /**
     * Matches on the throwable's class name.
     *
     * <p>Used where the type itself is not on our classpath — Hadoop and IBM MQ
     * exceptions reach us through interfaces that do not name them.
     */
    static ThrowableMatcher classNameContains(String... fragments) {
        String[] copy = Arrays.copyOf(fragments, fragments.length);
        return t -> {
            String className = t.getClass().getName();
            for (String fragment : copy) {
                if (className.contains(fragment)) {
                    return true;
                }
            }
            return false;
        };
    }

    /**
     * Matches on the throwable's message.
     *
     * <p>The weakest signal available, and the one that has already caused a
     * real misclassification: a payload field named {@code shutdownReason} once
     * made a poison message look like a clean shutdown. Rules that use this
     * must sit <em>below</em> type-based rules in
     * {@link RuleBasedFailureClassifier}'s ordering.
     */
    static ThrowableMatcher messageContains(String... fragments) {
        String[] copy = Arrays.copyOf(fragments, fragments.length);
        return t -> {
            String message = t.getMessage();
            if (message == null) {
                return false;
            }
            for (String fragment : copy) {
                if (message.contains(fragment)) {
                    return true;
                }
            }
            return false;
        };
    }

    /** Never matches. Useful as an identity element when composing. */
    static ThrowableMatcher never() {
        return t -> false;
    }
}
