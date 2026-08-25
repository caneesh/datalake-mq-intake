package com.hcsc.datalake.mqintake.core.loop.recovery;

import javax.jms.JMSException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

/**
 * A composable test over a {@link JMSException}.
 *
 * <p>Separate from the general {@code ThrowableMatcher} because JMS faults are
 * interrogated differently: the useful detail is usually in the <em>linked</em>
 * exception or the provider error code rather than the message, and the
 * comparisons here are case-insensitive. Folding those specifics into the
 * general matcher would make every caller pay for them.
 */
@FunctionalInterface
public interface JmsFaultMatcher {

    boolean matches(JMSException exception);

    default JmsFaultMatcher or(JmsFaultMatcher other) {
        Objects.requireNonNull(other, "other required");
        return e -> matches(e) || other.matches(e);
    }

    default JmsFaultMatcher and(JmsFaultMatcher other) {
        Objects.requireNonNull(other, "other required");
        return e -> matches(e) && other.matches(e);
    }

    default JmsFaultMatcher negate() {
        return e -> !matches(e);
    }

    /** Case-insensitive search of the exception's own message. */
    static JmsFaultMatcher messageContains(String... fragments) {
        String[] copy = lowercase(fragments);
        return e -> containsAny(e.getMessage(), copy);
    }

    /**
     * Case-insensitive search of the linked exception's message.
     *
     * <p>IBM MQ reports the real cause here — a JMSException often says only
     * that something failed, while the linked exception names the socket or
     * connection that went away.
     */
    static JmsFaultMatcher linkedMessageContains(String... fragments) {
        String[] copy = lowercase(fragments);
        return e -> {
            Exception linked = e.getLinkedException();
            return linked != null && containsAny(linked.getMessage(), copy);
        };
    }

    /** Matches when the provider error code starts with the given prefix. */
    static JmsFaultMatcher errorCodeStartsWith(String prefix) {
        Objects.requireNonNull(prefix, "prefix required");
        return e -> e.getErrorCode() != null && e.getErrorCode().startsWith(prefix);
    }

    /** Matches when the provider error code is exactly one of the given codes. */
    static JmsFaultMatcher errorCodeIn(String... codes) {
        String[] copy = Arrays.copyOf(codes, codes.length);
        return e -> {
            String code = e.getErrorCode();
            if (code == null) {
                return false;
            }
            for (String candidate : copy) {
                if (code.equals(candidate)) {
                    return true;
                }
            }
            return false;
        };
    }

    static JmsFaultMatcher never() {
        return e -> false;
    }

    private static String[] lowercase(String[] fragments) {
        String[] copy = new String[fragments.length];
        for (int i = 0; i < fragments.length; i++) {
            copy[i] = fragments[i].toLowerCase(Locale.ROOT);
        }
        return copy;
    }

    private static boolean containsAny(String text, String[] lowercaseFragments) {
        if (text == null) {
            return false;
        }
        String haystack = text.toLowerCase(Locale.ROOT);
        for (String fragment : lowercaseFragments) {
            if (haystack.contains(fragment)) {
                return true;
            }
        }
        return false;
    }
}
