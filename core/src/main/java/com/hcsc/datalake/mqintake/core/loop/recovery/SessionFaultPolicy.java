package com.hcsc.datalake.mqintake.core.loop.recovery;

import javax.jms.JMSException;

/**
 * Decides how the receive loop should react to a JMS failure.
 *
 * <p>Two independent questions, which the loop previously answered with two
 * private string-matching methods:
 * <ul>
 *   <li>{@link #requiresRecovery} — is the session unusable, so that
 *       continuing to call receive() on it is pointless?</li>
 *   <li>{@link #isFatal} — is this something reconnecting can never fix, so
 *       that retrying only delays the real error reaching an operator?</li>
 * </ul>
 *
 * <p>An interface because the answers are provider-specific policy, not
 * mechanism. The loop should express "recover if the policy says the session
 * is broken", not carry a list of IBM MQ substrings.
 */
public interface SessionFaultPolicy {

    /** True when the session must be rebuilt before work can continue. */
    boolean requiresRecovery(JMSException exception);

    /** True when reconnecting cannot help — bad credentials, denied access. */
    boolean isFatal(JMSException exception);

    static SessionFaultPolicy defaultPolicy() {
        return new DefaultSessionFaultPolicy();
    }
}
