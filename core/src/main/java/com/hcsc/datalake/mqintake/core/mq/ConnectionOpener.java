package com.hcsc.datalake.mqintake.core.mq;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * Produces a JMS {@link Connection}, or fails trying.
 *
 * <p>The one seam in the connect path, and it exists for a measured reason:
 * with the IBM factory construction inlined, nothing could drive a connection
 * failure without a real queue manager, so the retry budget, the backoff and
 * the fault classification that decides whether to retry at all were all
 * untested in any build without a broker.
 *
 * <p>Production always uses {@link IbmMqConnectionOpener}, and the real-MQ
 * tests still run through it, so the seam is not a path that only tests take.
 */
@FunctionalInterface
interface ConnectionOpener {

    /** @return a new, unstarted JMS connection */
    Connection open() throws JMSException;
}
