package com.hcsc.datalake.mqintake.core.loop.recovery;

import javax.jms.JMSException;

import static com.hcsc.datalake.mqintake.core.loop.recovery.JmsFaultMatcher.errorCodeIn;
import static com.hcsc.datalake.mqintake.core.loop.recovery.JmsFaultMatcher.errorCodeStartsWith;
import static com.hcsc.datalake.mqintake.core.loop.recovery.JmsFaultMatcher.linkedMessageContains;
import static com.hcsc.datalake.mqintake.core.loop.recovery.JmsFaultMatcher.messageContains;

/**
 * The IBM MQ policy: recognise a broken session broadly, and a fatal one
 * narrowly.
 *
 * <p>The asymmetry is deliberate. Treating a recoverable fault as fatal stops a
 * healthy binding permanently; treating a fatal fault as recoverable only costs
 * a bounded number of retries before the attempt budget runs out. So
 * {@link #requiresRecovery} errs towards yes and {@link #isFatal} towards no.
 *
 * <p>This remains substring matching, which is a known weakness — a
 * connection loss whose text matches none of these falls outside the recovery
 * path entirely. Isolating it here is what makes that weakness visible,
 * testable, and replaceable without touching the loop.
 */
public class DefaultSessionFaultPolicy implements SessionFaultPolicy {

    private static final JmsFaultMatcher BROKEN =
            messageContains("connection", "session", "closed", "disconnect", "broken", "reset")
                    .or(linkedMessageContains("connection", "socket"))
                    // Any MQ reason code: the provider is reporting a fault it
                    // has a name for, which is reason enough to rebuild.
                    .or(errorCodeStartsWith("MQRC"));

    private static final JmsFaultMatcher FATAL =
            messageContains("authentication", "authorization", "security",
                    "not authorized", "password", "credential")
                    // 2035 MQRC_NOT_AUTHORIZED, 2063 MQRC_SECURITY_ERROR
                    .or(errorCodeIn("MQRC_NOT_AUTHORIZED", "2035",
                            "MQRC_SECURITY_ERROR", "2063"));

    @Override
    public boolean requiresRecovery(JMSException exception) {
        return exception != null && BROKEN.matches(exception);
    }

    @Override
    public boolean isFatal(JMSException exception) {
        return exception != null && FATAL.matches(exception);
    }
}
