package com.hcsc.datalake.mqintake.core.failure.classification;

import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import java.io.IOException;

import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.anyType;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.classNameContains;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.messageContains;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.never;
import static org.assertj.core.api.Assertions.assertThat;

class ThrowableMatcherTest {

    @Test
    void anyTypeMatchesSubclasses() {
        ThrowableMatcher matcher = anyType(IOException.class);

        assertThat(matcher.matches(new IOException("x"))).isTrue();
        assertThat(matcher.matches(new java.io.FileNotFoundException("x"))).isTrue();
        assertThat(matcher.matches(new RuntimeException("x"))).isFalse();
    }

    @Test
    void anyTypeMatchesAnyOfSeveralTypes() {
        ThrowableMatcher matcher = anyType(IOException.class, IllegalStateException.class);

        assertThat(matcher.matches(new IOException("x"))).isTrue();
        assertThat(matcher.matches(new IllegalStateException("x"))).isTrue();
        assertThat(matcher.matches(new RuntimeException("x"))).isFalse();
    }

    @Test
    void classNameContainsMatchesTypesWeCannotReference() {
        // Hadoop and MQ exceptions reach us without being on the compile path
        ThrowableMatcher matcher = classNameContains("IllegalState");

        assertThat(matcher.matches(new IllegalStateException("x"))).isTrue();
        assertThat(matcher.matches(new IOException("x"))).isFalse();
    }

    @Test
    void messageContainsHandlesANullMessage() {
        ThrowableMatcher matcher = messageContains("boom");

        assertThat(matcher.matches(new RuntimeException((String) null))).isFalse();
        assertThat(matcher.matches(new RuntimeException("boom happened"))).isTrue();
    }

    @Test
    void messageMatchingIsCaseSensitive() {
        // Documents existing behaviour: the original classifier used
        // String.contains, and loosening it now could reclassify live failures.
        assertThat(messageContains("Shutdown").matches(new RuntimeException("shutdown"))).isFalse();
        assertThat(messageContains("shutdown").matches(new RuntimeException("shutdown"))).isTrue();
    }

    @Test
    void orTakesEitherSide() {
        ThrowableMatcher matcher = anyType(IOException.class).or(messageContains("boom"));

        assertThat(matcher.matches(new IOException("quiet"))).isTrue();
        assertThat(matcher.matches(new RuntimeException("boom"))).isTrue();
        assertThat(matcher.matches(new RuntimeException("quiet"))).isFalse();
    }

    @Test
    void andRequiresBothSides() {
        ThrowableMatcher matcher = anyType(IOException.class).and(messageContains("NameNode"));

        assertThat(matcher.matches(new IOException("NameNode down"))).isTrue();
        assertThat(matcher.matches(new IOException("disk full"))).isFalse();
        assertThat(matcher.matches(new RuntimeException("NameNode down"))).isFalse();
    }

    @Test
    void negateInvertsTheResult() {
        // The composition MqInfrastructureRule depends on: JMS but not security
        ThrowableMatcher matcher = anyType(JMSException.class)
                .and(anyType(JMSSecurityException.class).negate());

        assertThat(matcher.matches(new JMSException("broken"))).isTrue();
        assertThat(matcher.matches(new JMSSecurityException("denied"))).isFalse();
    }

    @Test
    void neverMatchesNothing() {
        assertThat(never().matches(new RuntimeException("anything"))).isFalse();
    }

    @Test
    void emptyFragmentListsMatchNothing() {
        assertThat(anyType().matches(new RuntimeException("x"))).isFalse();
        assertThat(classNameContains().matches(new RuntimeException("x"))).isFalse();
        assertThat(messageContains().matches(new RuntimeException("x"))).isFalse();
    }
}
