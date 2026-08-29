package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionProvider;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Probes the MQ side of each binding, one fact at a time.
 *
 * <p>Nothing here consumes or produces a message. Opening a queue is what
 * proves it: IBM MQ resolves the destination when the consumer or producer is
 * created, so a queue that does not exist — or that the service account
 * cannot open — fails here rather than at the first batch.
 *
 * <p>The queues are opened on <strong>one session of the connection the
 * application will actually use</strong>, which is the point. The tracker and
 * backout producers live on the listener's own transacted session, so a queue
 * defined on a sibling queue manager is unreachable however healthy it looks
 * in a monitoring console. That failure mode — first poison message rolls
 * back forever, binding stalls — is invisible until it happens in production,
 * and this is what makes it visible in a minute.
 */
public final class MqChecks {

    private MqChecks() {
    }

    public static List<PreflightCheck> forAllBindings(IntakeProperties properties,
                                                      MqConnectionProvider connections) {
        List<PreflightCheck> checks = new ArrayList<>();
        for (BindingConfig binding : properties.getBindings()) {
            String connectionId = binding.getMqConnection();
            checks.add(connectivity(binding, connectionId, connections));
            checks.add(queueAccess(binding, connectionId, connections,
                    "source-queue.input", binding.getSourceQueue(), Access.INPUT,
                    "the listener can open the source queue for input"));
            checks.add(depthReadable(binding, connectionId, connections));
            checks.add(queueAccess(binding, connectionId, connections,
                    "tracker-queue.output",
                    binding.getMode() == BindingMode.TRACKED ? binding.getTracker().getQueue() : null,
                    Access.OUTPUT,
                    "tracker messages can be put, on the same queue manager as the source"));
            checks.add(queueAccess(binding, connectionId, connections,
                    "backout-queue.output", binding.getBackout().getQueue(), Access.OUTPUT,
                    "poison messages can be routed, on the same queue manager as the source"));
        }
        return checks;
    }

    private enum Access { INPUT, OUTPUT }

    private static PreflightCheck connectivity(BindingConfig binding, String connectionId,
                                               MqConnectionProvider connections) {
        return new AbstractCheck("mq", binding.getId() + ".connection",
                "the queue manager accepts the configured host, channel and credentials") {
            @Override
            public CheckOutcome run() {
                try {
                    Connection connection = connections.getConnection(connectionId);
                    try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
                        String provider = connection.getMetaData().getJMSProviderName()
                                + " " + connection.getMetaData().getProviderVersion();
                        return CheckOutcome.pass("connected via '" + connectionId
                                + "', transacted session created — " + provider);
                    }
                } catch (JMSException | RuntimeException e) {
                    return CheckOutcome.fail("could not connect via '" + connectionId + "'", e,
                            "Check MQ_HOST / MQ_PORT / MQ_QUEUE_MANAGER / MQ_CHANNEL and the "
                                    + "credential behind MQ_CREDENTIAL_REF. MQRC 2035 means the "
                                    + "service account is not authorised on the channel.");
                }
            }
        };
    }

    private static PreflightCheck queueAccess(BindingConfig binding, String connectionId,
                                              MqConnectionProvider connections,
                                              String suffix, String queueName, Access access,
                                              String describes) {
        return new AbstractCheck("mq", binding.getId() + "." + suffix, describes) {
            @Override
            public CheckOutcome run() {
                if (queueName == null || queueName.isBlank()) {
                    return CheckOutcome.skip("not configured for this binding");
                }
                try {
                    Connection connection = connections.getConnection(connectionId);
                    try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
                        Queue queue = session.createQueue(queueName);
                        // Opening is the whole probe: resolution happens here,
                        // and neither a consumer that never receives nor a
                        // producer that never sends moves a message.
                        if (access == Access.INPUT) {
                            try (MessageConsumer consumer = session.createConsumer(queue)) {
                                return CheckOutcome.pass(
                                        "'" + queueName + "' opened for input (nothing consumed)");
                            }
                        }
                        try (MessageProducer producer = session.createProducer(queue)) {
                            return CheckOutcome.pass(
                                    "'" + queueName + "' opened for output (nothing sent)");
                        }
                    }
                } catch (JMSException | RuntimeException e) {
                    return CheckOutcome.fail("cannot open '" + queueName + "' for "
                            + access.name().toLowerCase(java.util.Locale.ROOT), e,
                            "MQRC 2085 (unknown object name) here usually means the queue exists "
                                    + "on a DIFFERENT queue manager than the one this connection "
                                    + "reached. The tracker and backout producers run on the "
                                    + "listener's own session, so every queue must be defined on "
                                    + "the connected queue manager. MQRC 2035 means the account "
                                    + "lacks the open option this probe used.");
                }
            }
        };
    }

    private static PreflightCheck depthReadable(BindingConfig binding, String connectionId,
                                                MqConnectionProvider connections) {
        return new AbstractCheck("mq", binding.getId() + ".backout-queue.browse",
                "the backout-depth monitor can browse the backout queue") {
            @Override
            public CheckOutcome run() {
                String queueName = binding.getBackout().getQueue();
                if (queueName == null || queueName.isBlank()) {
                    return CheckOutcome.skip("no backout queue configured");
                }
                if (binding.getBackout().getDepthPollIntervalMs() <= 0) {
                    return CheckOutcome.skip("depth monitoring disabled for this binding");
                }
                try {
                    Connection connection = connections.getConnection(connectionId);
                    try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                        Queue queue = session.createQueue(queueName);
                        try (QueueBrowser browser = session.createBrowser(queue)) {
                            int depth = 0;
                            Enumeration<?> messages = browser.getEnumeration();
                            // Bounded: a deep queue must not turn a preflight
                            // into a full scan.
                            while (messages.hasMoreElements() && depth < 1000) {
                                messages.nextElement();
                                depth++;
                            }
                            String reading = depth >= 1000 ? "1000+" : String.valueOf(depth);
                            return CheckOutcome.pass("browsable, current depth " + reading
                                    + (depth > 0 ? " — NOTE: non-empty backout queue" : ""));
                        }
                    }
                } catch (JMSException | RuntimeException e) {
                    return CheckOutcome.fail("cannot browse '" + queueName + "'", e,
                            "The depth gauge needs browse authority (MQOO_BROWSE). Without it "
                                    + "the backout-depth alert — the design's nominated pager "
                                    + "condition — runs blind.");
                }
            }
        };
    }

    /** Boilerplate for the anonymous checks above. */
    abstract static class AbstractCheck implements PreflightCheck {
        private final String group;
        private final String name;
        private final String describes;

        AbstractCheck(String group, String name, String describes) {
            this.group = group;
            this.name = name;
            this.describes = describes;
        }

        @Override
        public String group() {
            return group;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String describes() {
            return describes;
        }
    }
}
