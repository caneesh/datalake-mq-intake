package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.loop.recovery.BackoffPolicy;
import com.hcsc.datalake.mqintake.core.loop.recovery.JmsFaultMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * One queue manager's shared JMS {@link Connection}, opened with bounded
 * retries and held for the life of the manager.
 *
 * <p>Extracted from {@code MqConnectionManager}, where it was a private nested
 * class and therefore unreachable from any test that did not have a live queue
 * manager. That mattered more than it looked: mutating the retry budget, the
 * fault classification, the linked-exception matcher, the factory
 * configuration and the connection sharing itself each left the default build
 * green. Five for five, on the code that opens every MQ connection both
 * applications make.
 *
 * <p>The seam that fixes it is {@link ConnectionOpener}. An earlier round of
 * work deliberately avoided one, on the grounds that this class was reachable
 * through the manager's public API against a real queue manager — which it is,
 * and those eleven tests still run unchanged and still exercise the production
 * opener. What they cannot do is run when nobody has a broker, which is every
 * ordinary build.
 *
 * <p><strong>One Connection per queue manager, shared; Sessions are never
 * shared.</strong> {@link #getConnection()} is synchronized and returns the
 * same instance to every caller, and each listener thread creates its own
 * Session from it. That is the second standing constraint of this project, and
 * until now nothing tested it.
 */
class ManagedConnection {

    private static final Logger log = LoggerFactory.getLogger(ManagedConnection.class);

    private final MqConnectionConfig config;
    private final ConnectionOpener opener;
    private final BackoffPolicy backoffPolicy;
    private volatile Connection connection;

    ManagedConnection(MqConnectionConfig config, ConnectionOpener opener,
                      BackoffPolicy backoffPolicy) {
        this.config = config;
        this.opener = opener;
        this.backoffPolicy = backoffPolicy;
    }

    synchronized Connection getConnection() throws MqConnectionManager.MqConnectionException {
        if (connection != null) {
            return connection;
        }

        return connect();
    }

    private Connection connect() throws MqConnectionManager.MqConnectionException {
        int attempts = 0;
        int maxAttempts = config.getReconnectAttempts();
        JMSException lastException = null;

        while (attempts < maxAttempts) {
            attempts++;
            try {
                if (Thread.currentThread().isInterrupted()) {
                    throw new MqConnectionManager.MqConnectionException("Connection attempt interrupted");
                }

                // Published only once it is started. Assigning the field
                // first — which is what this did — left a non-null,
                // never-started Connection behind whenever start() threw. The
                // caller that triggered it still got an exception once the
                // budget ran out, but the NEXT caller found the field non-null
                // and was handed the dead connection with no error at all.
                // Reachable with two bindings sharing one queue manager: the
                // first fails startup, the second asks for the same connection
                // id and consumes nothing, silently.
                Connection opened = opener.open();
                opened.start();
                connection = opened;

                log.info("Connected to MQ: id={}, host={}, queueManager={}",
                        config.getId(), config.getHost(), config.getQueueManager());

                return connection;

            } catch (JMSException e) {
                lastException = e;
                log.warn("Connection attempt {} of {} failed for {}: {}",
                        attempts, maxAttempts, config.getId(), e.getMessage());

                if (isConfigurationError(e)) {
                    throw new MqConnectionManager.MqConnectionException(
                            "Configuration error connecting to " + config.getId() + ": " + e.getMessage(), e);
                }

                if (attempts < maxAttempts) {
                    try {
                        Thread.sleep(backoffPolicy.backoffFor(attempts).toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new MqConnectionManager.MqConnectionException("Connection attempt interrupted", ie);
                    }
                }
            }
        }

        throw new MqConnectionManager.MqConnectionException(
                "Failed to connect to " + config.getId() + " after " + maxAttempts + " attempts",
                lastException);
    }

    /**
     * Reasons no amount of retrying will fix.
     *
     * <p>{@code MQRC_UNKNOWN_CHANNEL_NAME} rather than
     * {@code MQRC_CHANNEL_NOT_FOUND}: the latter was in this list and is
     * not a reason code IBM MQ emits, so it never matched anything. A
     * channel missing from the queue manager reports 2540
     * MQRC_UNKNOWN_CHANNEL_NAME.
     *
     * <p>Deliberately absent: {@code MQRC_HOST_NOT_AVAILABLE} (2538) and
     * {@code MQRC_CHANNEL_NOT_AVAILABLE} (2537). Both are transient — a
     * listener not up yet, every channel instance busy — and both must
     * keep retrying. Adding either would turn a queue manager restart into
     * a startup failure.
     */
    private static final String[] NOT_WORTH_RETRYING = {
            "MQRC_UNKNOWN_OBJECT_NAME",
            "MQRC_NOT_AUTHORIZED",
            "MQRC_SECURITY_ERROR",
            "MQRC_Q_MGR_NAME_ERROR",
            "MQRC_UNKNOWN_CHANNEL_NAME",
    };

    /**
     * Searches the exception's own message AND its linked exception.
     *
     * <p>The linked half is the half that works. IBM MQ reports a failed
     * connect as {@code JMSWMQ0018: Failed to connect to queue manager
     * 'X'...} with error code {@code JMSWMQ0018} — identical for a wrong
     * queue-manager name, a wrong channel and an unreachable listener. The
     * reason code that distinguishes them lives only in the linked
     * {@code MQException}: {@code ... reason '2058'
     * ('MQRC_Q_MGR_NAME_ERROR')}. Matching on the top-level message alone,
     * as this did, therefore never recognised any of the conditions listed
     * above, and every misconfiguration was retried to exhaustion.
     *
     * <p>The error code is not matched on for the same reason: JMSWMQ0018
     * covers all three cases and would make transient failures look like
     * configuration ones.
     */
    private static final JmsFaultMatcher CONFIGURATION_FAULT =
            JmsFaultMatcher.messageContains(NOT_WORTH_RETRYING)
                    .or(JmsFaultMatcher.linkedMessageContains(NOT_WORTH_RETRYING));

    private boolean isConfigurationError(JMSException e) {
        // A credential that will not resolve is a configuration problem,
        // not a transient one. Retrying cannot fix it and would only delay
        // the real error reaching the operator.
        if (e instanceof MqConnectionManager.MqCredentialException) {
            return true;
        }
        return CONFIGURATION_FAULT.matches(e);
    }

    synchronized void close() {
        if (connection != null) {
            try {
                connection.close();
                log.info("Closed MQ connection: {}", config.getId());
            } catch (JMSException e) {
                log.warn("Error closing MQ connection {}: {}", config.getId(), e.getMessage());
            }
            connection = null;
        }
    }
}
