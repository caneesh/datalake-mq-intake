package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;

import javax.jms.JMSSecurityException;
import javax.security.auth.login.LoginException;
import java.security.AccessControlException;

import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.anyType;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.classNameContains;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.messageContains;

/**
 * Authentication, authorisation or credential configuration is wrong.
 *
 * <p>Retrying does not help, and it must not be mistaken for an MQ
 * infrastructure blip — which is why it is checked before the MQ rule, whose
 * JMSException test would otherwise swallow every JMSSecurityException.
 *
 * <p>Declares {@code reliesOnMessageText() == true}, because it does: the
 * message fallback is real, and the flag previously said false — a
 * misstatement that also made the classifier's ordering guard vacuous for
 * this rule.
 */
public final class SecurityConfigRule extends MatcherFailureRule {

    public SecurityConfigRule() {
        super(FailureClass.SECURITY_CONFIG,
                anyType(JMSSecurityException.class,
                        SecurityException.class,
                        AccessControlException.class,
                        LoginException.class)
                .or(classNameContains("KerberosException", "GSSException", "KrbException"))
                // "not authorized" and bare "credential" were removed: both
                // are plausible words in an insurance payload ("provider not
                // authorized for this procedure"), and an exception that
                // echoes payload text would have classified a data failure as
                // security — silently disabling poison isolation, the same
                // bug class the shutdownReason incident already exposed. The
                // remaining phrases are system-generated, not domain prose.
                .or(messageContains("Permission denied", "Access denied")),
                true);
    }
}
