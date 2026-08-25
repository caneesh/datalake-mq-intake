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
 */
public final class SecurityConfigRule extends MatcherFailureRule {

    public SecurityConfigRule() {
        super(FailureClass.SECURITY_CONFIG,
                anyType(JMSSecurityException.class,
                        SecurityException.class,
                        AccessControlException.class,
                        LoginException.class)
                .or(classNameContains("KerberosException", "GSSException", "KrbException"))
                .or(messageContains("Permission denied", "Access denied",
                        "not authorized", "credential")));
    }
}
