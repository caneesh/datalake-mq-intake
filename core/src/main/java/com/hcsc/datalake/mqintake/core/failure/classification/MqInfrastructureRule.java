package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.anyType;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.classNameContains;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.messageContains;

/**
 * The queue manager or the network to it is the problem.
 *
 * <p>Last in the chain: its JMSException test is broad enough to claim
 * security failures and some data failures if it ran earlier.
 */
public final class MqInfrastructureRule extends MatcherFailureRule {

    public MqInfrastructureRule() {
        super(FailureClass.MQ_INFRASTRUCTURE,
                anyType(JMSException.class).and(anyType(JMSSecurityException.class).negate())
                .or(classNameContains("MQ", "mq.jms"))
                .or(anyType(ConnectException.class, SocketException.class,
                        SocketTimeoutException.class))
                .or(messageContains("MAXUMSGS", "syncpoint", "queue manager", "channel")));
    }
}
