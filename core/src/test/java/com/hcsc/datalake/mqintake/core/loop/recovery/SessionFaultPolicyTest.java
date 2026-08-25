package com.hcsc.datalake.mqintake.core.loop.recovery;

import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.SocketException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How the loop should react to a JMS fault.
 *
 * <p>These decisions used to live in two private methods on a 700-line class
 * and had no direct test — the only coverage was whatever a full loop run
 * happened to exercise. Extracting them made the policy addressable.
 */
class SessionFaultPolicyTest {

    private final SessionFaultPolicy policy = SessionFaultPolicy.defaultPolicy();

    @Test
    void connectionLossRequiresRecovery() {
        assertThat(policy.requiresRecovery(new JMSException("Connection broken"))).isTrue();
        assertThat(policy.requiresRecovery(new JMSException("session closed"))).isTrue();
        assertThat(policy.requiresRecovery(new JMSException("peer disconnect"))).isTrue();
        assertThat(policy.requiresRecovery(new JMSException("connection reset"))).isTrue();
    }

    @Test
    void matchingIsCaseInsensitive() {
        // The original lowercased the message before comparing; preserved.
        assertThat(policy.requiresRecovery(new JMSException("CONNECTION BROKEN"))).isTrue();
        assertThat(policy.requiresRecovery(new JMSException("Session Closed"))).isTrue();
    }

    @Test
    void theRealCauseIsOftenInTheLinkedException() {
        // IBM MQ reports a bland message and puts the detail in the link
        JMSException exception = new JMSException("MQJMS2002");
        exception.setLinkedException(new SocketException("Connection reset by peer"));

        assertThat(policy.requiresRecovery(exception)).isTrue();
    }

    @Test
    void anyMqReasonCodeRequiresRecovery() {
        JMSException exception = new JMSException("failed", "MQRC_CONNECTION_BROKEN");

        assertThat(policy.requiresRecovery(exception)).isTrue();
    }

    @Test
    void anUnrelatedFailureDoesNotTriggerRecovery() {
        assertThat(policy.requiresRecovery(new JMSException("message selector invalid")))
                .isFalse();
    }

    @Test
    void authorisationFailuresAreFatal() {
        // Reconnecting with the same rejected credentials just delays the real
        // error reaching an operator.
        assertThat(policy.isFatal(new JMSException("not authorized"))).isTrue();
        assertThat(policy.isFatal(new JMSException("authentication failed"))).isTrue();
        assertThat(policy.isFatal(new JMSException("bad password"))).isTrue();
        assertThat(policy.isFatal(new JMSException("credential rejected"))).isTrue();
    }

    @Test
    void knownFatalMqReasonCodesAreRecognisedByNameAndNumber() {
        assertThat(policy.isFatal(new JMSException("x", "MQRC_NOT_AUTHORIZED"))).isTrue();
        assertThat(policy.isFatal(new JMSException("x", "2035"))).isTrue();
        assertThat(policy.isFatal(new JMSException("x", "MQRC_SECURITY_ERROR"))).isTrue();
        assertThat(policy.isFatal(new JMSException("x", "2063"))).isTrue();
    }

    @Test
    void aTransientOutageIsNotFatal() {
        // The asymmetry that matters: broken is recognised broadly, fatal
        // narrowly. Calling a recoverable fault fatal stops a healthy binding
        // for good; the reverse only costs a bounded number of retries.
        JMSException outage = new JMSException("Connection broken", "MQRC_CONNECTION_BROKEN");

        assertThat(policy.requiresRecovery(outage)).isTrue();
        assertThat(policy.isFatal(outage)).isFalse();
    }

    @Test
    void nullMessagesAndCodesAreHandled() {
        JMSException blank = new JMSException(null);

        assertThat(policy.requiresRecovery(blank)).isFalse();
        assertThat(policy.isFatal(blank)).isFalse();
    }

    @Test
    void nullExceptionIsNeitherBrokenNorFatal() {
        assertThat(policy.requiresRecovery(null)).isFalse();
        assertThat(policy.isFatal(null)).isFalse();
    }

    @Test
    void aLinkedExceptionWithNoMessageDoesNotBlowUp() {
        JMSException exception = new JMSException("wrapper");
        exception.setLinkedException(new IOException());

        assertThat(policy.requiresRecovery(exception)).isFalse();
    }
}
