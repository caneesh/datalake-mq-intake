package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.failure.FailureClassifier;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The chain itself: ordering, fall-through and the ordering guard.
 *
 * <p>{@code FailureClassifierTest} covers what each category recognises. This
 * covers the part that is easy to get wrong and expensive when wrong — which
 * rule wins when several match.
 */
class RuleBasedFailureClassifierTest {

    private final FailureClassifier classifier = FailureClassifier.defaultClassifier();

    @Test
    void rulesAreConsultedInPriorityOrder() {
        List<FailureClass> order = new RuleBasedFailureClassifier().rules().stream()
                .map(FailureRule::failureClass)
                .collect(java.util.stream.Collectors.toList());

        assertThat(order).containsExactly(
                FailureClass.MESSAGE_DATA,
                FailureClass.SHUTDOWN,
                FailureClass.SECURITY_CONFIG,
                FailureClass.HDFS_INFRASTRUCTURE,
                FailureClass.MQ_INFRASTRUCTURE);
    }

    @Test
    void securityIsCheckedBeforeMqSoJmsSecurityIsNotSwallowed() {
        // JMSSecurityException satisfies the MQ rule's JMSException test too.
        // If MQ ran first, a wrong password would look like a network blip and
        // be retried forever instead of failing loudly.
        assertThat(classifier.classify(new JMSSecurityException("not authorized")))
                .isEqualTo(FailureClass.SECURITY_CONFIG);

        assertThat(classifier.classify(new JMSException("connection broken")))
                .isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
    }

    @Test
    void dataFailuresOutrankTheShutdownTextMatch() {
        // The regression that started this: a payload mentioning "shutdown"
        // must still be recognised as a data failure, or the poison message
        // is never isolated.
        assertThat(classifier.classify(new RecordSerializer.SerializationException(
                "cannot parse <shutdownReason> in payload")))
                .isEqualTo(FailureClass.MESSAGE_DATA);
    }

    @Test
    void wrappedCausesAreClassified() {
        Throwable wrapped = new RuntimeException("batch failed",
                new IOException("NameNode is in safemode"));

        assertThat(classifier.classify(wrapped)).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
    }

    @Test
    void unrecognisedFailuresAreUnknownAndNeverTriggerDegradedMode() {
        FailureClass result = classifier.classify(new RuntimeException("something new"));

        assertThat(result).isEqualTo(FailureClass.UNKNOWN);
        assertThat(result.triggersDegradedMode())
                .as("an unknown failure must never shrink the batch")
                .isFalse();
    }

    @Test
    void nullIsUnknownRatherThanAnException() {
        assertThat(classifier.classify(null)).isEqualTo(FailureClass.UNKNOWN);
    }

    @Test
    void aNonRootCycleDeeperInTheChainDoesNotLoopForever() {
        // The old guard (`cause != throwable`) only rejected a cycle back to
        // the ROOT. A cycle between two deeper causes never equals the root,
        // so the walk spun forever — hanging the listener thread in a way
        // supervision cannot see, since the task never completes.
        RuntimeException root = new RuntimeException("root");
        RuntimeException b = new RuntimeException("b");
        RuntimeException c = new RuntimeException("c");
        b.initCause(c);
        c.initCause(b);
        root.initCause(b);

        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
                java.time.Duration.ofSeconds(5),
                () -> assertThat(classifier.classify(root)).isEqualTo(FailureClass.UNKNOWN));
    }

    @Test
    void aRecognisableCauseInsideACycleIsStillClassified() {
        // The guard must terminate the walk, not blind it: each node is still
        // visited once, so a real signal inside the cycle is found.
        RuntimeException root = new RuntimeException("root");
        RuntimeException b = new RuntimeException("b");
        java.io.IOException hdfs = new java.io.IOException("NameNode is in safemode");
        b.initCause(hdfs);
        hdfs.initCause(b);
        root.initCause(b);

        assertThat(classifier.classify(root)).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
    }

    @Test
    void cyclicCauseChainDoesNotLoopForever() {
        // Java forbids self-causation, but a two-node cycle is constructible
        // and would spin the cause walk forever without the guard.
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        first.initCause(second);
        second.initCause(first);

        assertThat(classifier.classify(first)).isEqualTo(FailureClass.UNKNOWN);
    }

    @Test
    void aCustomChainCanReplaceThePolicy() {
        // The point of the interface: a binding could classify differently
        // without touching anything that consumes the answer.
        FailureClassifier everythingIsData = new RuleBasedFailureClassifier(
                List.of(new MatcherFailureRule(FailureClass.MESSAGE_DATA, t -> true)));

        assertThat(everythingIsData.classify(new JMSException("broken")))
                .isEqualTo(FailureClass.MESSAGE_DATA);
    }

    @Test
    void placingATextMatchingRuleAboveMessageDataIsRejected() {
        // The guard exists because this exact ordering silently disabled
        // poison isolation once already.
        assertThatThrownBy(() -> new RuleBasedFailureClassifier(List.of(
                new ShutdownRule(),
                new MessageDataRule())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("poison messages would stop being isolated");
    }

    @Test
    void theDefaultChainSatisfiesItsOwnOrderingGuard() {
        // Guards against someone reordering the default chain into the unsafe
        // arrangement the constructor rejects.
        assertThat(new RuleBasedFailureClassifier().rules()).isNotEmpty();
    }

    @Test
    void anEmptyChainIsRejected() {
        assertThatThrownBy(() -> new RuleBasedFailureClassifier(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one failure rule");
    }

    @Test
    void domainProseInAnExceptionMessageDoesNotClassifyAsInfrastructure() {
        // The recurrence of the shutdownReason bug class the review flagged:
        // "channel", "not authorized" and "lease" are ordinary insurance
        // vocabulary. An exception echoing payload text must not classify as
        // infrastructure/security — those classes never degrade, so the
        // poison would never be isolated. UNKNOWN is the safe fall-through.
        assertThat(classifier.classify(new RuntimeException(
                "could not process: distribution channel unknown")))
                .isEqualTo(FailureClass.UNKNOWN);
        assertThat(classifier.classify(new RuntimeException(
                "provider not authorized for this procedure")))
                .isEqualTo(FailureClass.UNKNOWN);
        assertThat(classifier.classify(new RuntimeException(
                "invalid credential-type field in claim")))
                .isEqualTo(FailureClass.UNKNOWN);
    }

    @Test
    void mqVocabularyStillClassifiesAsMqInfrastructure() {
        // The pruning must not blind the rule to genuine MQ prose.
        assertThat(classifier.classify(new RuntimeException("MAXUMSGS limit exceeded")))
                .isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
        assertThat(classifier.classify(new RuntimeException("queue manager unavailable")))
                .isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
    }

    @Test
    void jdkFileSystemTypesNoLongerMatchTheHdfsRule() {
        // Bare classNameContains("FileSystem","RemoteException") also matched
        // java.nio.file and java.rmi types unrelated to HDFS.
        assertThat(classifier.classify(
                new java.nio.file.FileSystemNotFoundException("local disk")))
                .isEqualTo(FailureClass.UNKNOWN);
    }

    @Test
    void everyTextMatchingRuleSaysSo() {
        // The flag previously said false on three rules that DO match message
        // text — a misstatement that also made the ordering guard vacuous for
        // them. If one of these flips back to false, the guard stops
        // protecting anything again.
        assertThat(new ShutdownRule().reliesOnMessageText()).isTrue();
        assertThat(new SecurityConfigRule().reliesOnMessageText()).isTrue();
        assertThat(new HdfsInfrastructureRule().reliesOnMessageText()).isTrue();
        assertThat(new MqInfrastructureRule().reliesOnMessageText()).isTrue();
        assertThat(new MessageDataRule().reliesOnMessageText()).isFalse();
    }

    @Test
    void shutdownRuleIsMarkedAsRelyingOnMessageText() {
        // If this ever becomes false the ordering guard stops protecting
        // anything, so it is asserted rather than assumed.
        assertThat(new ShutdownRule().reliesOnMessageText()).isTrue();
        assertThat(new MessageDataRule().reliesOnMessageText()).isFalse();
    }
}
