package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.failure.FailureClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Classifies failures by consulting an ordered chain of {@link FailureRule}s.
 *
 * <p><strong>Order is the contract.</strong> Several rules can match the same
 * throwable — a JMSSecurityException satisfies both the security and the MQ
 * rule — so the first match wins and the sequence below is the decision, not an
 * implementation detail. Type-based rules precede text-based ones because
 * message matching has already produced one real misclassification: a
 * serialization failure whose payload mentioned "shutdown" was classified as a
 * clean shutdown, which meant the poison message was never isolated.
 *
 * <p>That ordering invariant is enforced in the constructor rather than left to
 * a comment, so a future reordering fails immediately instead of silently
 * disabling poison isolation.
 *
 * <p>Unknown failures deliberately fall through to {@link FailureClass#UNKNOWN},
 * which never triggers degraded mode.
 */
public class RuleBasedFailureClassifier implements FailureClassifier {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedFailureClassifier.class);

    private final List<FailureRule> rules;

    /** The default chain. The order is the classification policy. */
    public RuleBasedFailureClassifier() {
        this(List.of(
                new MessageDataRule(),        // type-based, and the only class that isolates poison
                new ShutdownRule(),           // expected during stop
                new SecurityConfigRule(),     // before MQ, which would swallow JMSSecurityException
                new HdfsInfrastructureRule(),
                new MqInfrastructureRule()    // broadest, so last
        ));
    }

    public RuleBasedFailureClassifier(List<FailureRule> rules) {
        Objects.requireNonNull(rules, "rules required");
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("At least one failure rule is required");
        }
        this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
        assertMessageDataIsNotOutrankedByTextMatching(this.rules);
    }

    /**
     * MESSAGE_DATA must not be outranked by a rule that matches on message text.
     *
     * <p>This is the specific ordering that has already broken once: ShutdownRule
     * falls back to searching for "shutdown" in the message, and while it ran
     * first, a serialization failure whose payload mentioned that word was
     * classified as a clean shutdown. SHUTDOWN never enters degraded mode, so
     * the poison message was never isolated and kept rolling back every clean
     * message batched with it.
     *
     * <p>Deliberately narrow. A blanket "no type-based rule below any
     * text-based rule" also rejects the legitimate placement of ShutdownRule
     * above the infrastructure rules, which is correct and always has been —
     * the invariant that matters is about poison isolation specifically.
     */
    private static void assertMessageDataIsNotOutrankedByTextMatching(List<FailureRule> rules) {
        for (FailureRule rule : rules) {
            if (rule.failureClass() == FailureClass.MESSAGE_DATA) {
                return;   // reached it before any text-matching rule
            }
            if (rule.reliesOnMessageText()) {
                throw new IllegalArgumentException(
                        "Rule ordering is unsafe: " + rule + " matches on message text and is "
                                + "placed above the MESSAGE_DATA rule. A payload containing an "
                                + "unlucky word would then be misclassified, and poison messages "
                                + "would stop being isolated — this has happened before.");
            }
        }
    }

    @Override
    public FailureClass classify(Throwable throwable) {
        if (throwable == null) {
            return FailureClass.UNKNOWN;
        }

        FailureClass direct = classifyOne(throwable);
        if (direct != FailureClass.UNKNOWN) {
            return direct;
        }

        // The useful signal is often wrapped: an IOException from HDFS arrives
        // inside a BatchWriteException.
        Throwable cause = throwable.getCause();
        while (cause != null && cause != throwable) {
            FailureClass fromCause = classifyOne(cause);
            if (fromCause != FailureClass.UNKNOWN) {
                return fromCause;
            }
            cause = cause.getCause();
        }

        log.warn("Unclassified exception: {} - {}",
                throwable.getClass().getName(), throwable.getMessage());
        return FailureClass.UNKNOWN;
    }

    private FailureClass classifyOne(Throwable throwable) {
        for (FailureRule rule : rules) {
            Optional<FailureClass> match = rule.classify(throwable);
            if (match.isPresent()) {
                return match.get();
            }
        }
        return FailureClass.UNKNOWN;
    }

    /** The chain in priority order, for diagnostics and tests. */
    public List<FailureRule> rules() {
        return rules;
    }
}
