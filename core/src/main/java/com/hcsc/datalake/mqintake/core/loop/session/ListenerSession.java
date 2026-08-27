package com.hcsc.datalake.mqintake.core.loop.session;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Objects;

/**
 * The JMS resources belonging to one listener thread.
 *
 * <p>This class exists to hold a standing constraint in one place: <em>one
 * transacted Session per listener thread, with its consumer and producer
 * created from that same Session, never shared across threads.</em> That rule
 * is what makes the landing write, the tracker send and the source acknowledge
 * a single unit of work. Previously it was upheld by convention, spread across
 * two private methods and three mutable fields on a 700-line class, where it
 * could be broken by an edit that looked harmless.
 *
 * <p>Everything here is created together, replaced together and closed
 * together, so a half-open state cannot outlive a failure. Instances are
 * confined to their owning thread — that confinement is the invariant, so this
 * class deliberately offers no synchronisation that might suggest otherwise.
 *
 * <p>The tracker producer exists only for TRACKED bindings. Asking a LAND_ONLY
 * binding for one is a programming error rather than a runtime condition, and
 * is reported as such.
 */
public class ListenerSession implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ListenerSession.class);

    private final Connection connection;
    private final BindingConfig config;

    private Session session;
    private MessageConsumer consumer;
    private MessageProducer trackerProducer;

    public ListenerSession(Connection connection, BindingConfig config) {
        this.connection = Objects.requireNonNull(connection, "connection required");
        this.config = Objects.requireNonNull(config, "config required");
    }

    /**
     * Creates the session, the consumer, and — for TRACKED bindings — the
     * tracker producer.
     *
     * <p>Reuses the existing {@link Connection}. Recovery after an outage
     * depends on that: the IBM MQ client re-establishes lazily when a new
     * session is created, which is why a broken session can be replaced
     * without rebuilding the connection.
     */
    public void open() throws JMSException {
        session = connection.createSession(true, Session.SESSION_TRANSACTED);

        Queue sourceQueue = session.createQueue(config.getSourceQueue());
        consumer = session.createConsumer(sourceQueue);

        if (config.getMode() == BindingMode.TRACKED) {
            Queue trackerQueue = session.createQueue(config.getTracker().getQueue());
            trackerProducer = session.createProducer(trackerQueue);
            log.debug("Created tracker producer for binding '{}' on queue '{}'",
                    config.getId(), config.getTracker().getQueue());
        }

        log.info("Initialized session for binding '{}': source='{}', mode={}",
                config.getId(), config.getSourceQueue(), config.getMode());
    }

    /**
     * Closes everything, in the reverse of creation order, and leaves the
     * object reusable by a subsequent {@link #open()}.
     *
     * <p>Close failures are logged and swallowed by design: this runs on the
     * recovery and shutdown paths, where the resources are already suspect and
     * the useful error is the one that got us here, not a complaint about
     * closing a socket that has already gone.
     *
     * <p>Closing an open transacted session rolls back its transaction per the
     * JMS specification, so uncommitted messages return to the queue rather
     * than being silently acknowledged.
     */
    @Override
    public void close() {
        trackerProducer = closeQuietly(trackerProducer, "tracker producer");
        consumer = closeQuietly(consumer, "consumer");
        session = closeQuietly(session, "session");
    }

    private <T extends AutoCloseable> T closeQuietly(T resource, String description) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                log.debug("Error closing {}: {}", description, e.getMessage());
            }
        }
        return null;
    }

    /** True once {@link #open()} has succeeded and {@link #close()} has not run. */
    public boolean isOpen() {
        return session != null;
    }

    public Session session() {
        return session;
    }

    public MessageConsumer consumer() {
        return consumer;
    }

    /**
     * @throws IllegalStateException if the binding is not TRACKED — a
     *         LAND_ONLY binding has no tracker queue, so asking for a producer
     *         is a coding error, not a runtime state to handle
     */
    public MessageProducer trackerProducer() {
        if (trackerProducer == null) {
            throw new IllegalStateException(
                    "Binding '" + config.getId() + "' has no tracker producer (mode="
                            + config.getMode() + "). Only TRACKED bindings send trackers.");
        }
        return trackerProducer;
    }

    public boolean hasTrackerProducer() {
        return trackerProducer != null;
    }
}
