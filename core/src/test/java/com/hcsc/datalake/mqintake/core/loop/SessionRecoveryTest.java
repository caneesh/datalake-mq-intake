package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.loop.recovery.BackoffPolicy;
import com.hcsc.datalake.mqintake.core.loop.session.ListenerSession;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The recovery state machine, driven without a queue-manager outage.
 *
 * <p>The review found this machine was exercised only by the Docker-gated
 * real-MQ failure tests: the embedded-suite "recovery" tests asserted
 * {@code reconnectCount == 0} on a healthy run, which passes whether
 * {@code recoverSession()} works, is broken, or is deleted. A regression in
 * the give-up budget (retrying forever) or the fatal short-circuit (retrying
 * bad credentials forever) was undetectable without the container. These
 * tests inject a fault-wrapped {@link ListenerSession} and a 1ms backoff
 * through the loop's testing constructor to pin all three outcomes:
 * RETRY→RECOVERED, budget-exhausted GIVE_UP, and fatal GIVE_UP.
 */
class SessionRecoveryTest {

    private static final long RECEIVE_TIMEOUT_MS = 50;

    private Connection connection;
    private Session producerSession;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        try {
            producerSession.close();
            connection.close();
        } catch (Exception ignored) {
        }
    }

    @Test
    void aBrokenSessionIsRecoveredAndConsumptionResumes() throws Exception {
        BindingConfig config = config("REC.RESUME", 3);
        BreakOnceSession session = new BreakOnceSession(connection, config);
        session.breakNextReceive();
        CountingWriter writer = new CountingWriter();

        TransactedReceiveLoop loop = loop(config, writer, session);
        Future<?> future = executor.submit(loop);

        // The armed fault fires on the first receive; recovery reopens against
        // the live broker; the messages sent afterwards must still land.
        send(config.getSourceQueue(), "m1", "m2", "m3");
        awaitTrue(5_000, () -> loop.getCommitCount() >= 1);

        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(loop.getReconnectCount())
                .as("exactly one recovery cycle must have completed").isEqualTo(1);
        assertThat(loop.getCurrentReconnectAttempts()).isZero();
        assertThat(writer.written.get()).isEqualTo(3);
        assertThat(loop.getCommitCount()).isEqualTo(1);
    }

    @Test
    void anUnrecoverableSessionExhaustsTheBudgetAndStopsTheLoop() throws Exception {
        BindingConfig config = config("REC.EXHAUST", 3);
        DeadOnReopenSession session = new DeadOnReopenSession(
                connection, config, "connection refused, broker gone");

        TransactedReceiveLoop loop = loop(config, new CountingWriter(), session);
        Future<?> future = executor.submit(loop);
        awaitTrue(2_000, loop::isRunning);

        session.breakPermanently();

        // 10 failed reopen attempts at 1ms backoff, then GIVE_UP stops the loop.
        future.get(10, TimeUnit.SECONDS);

        assertThat(loop.isRunning()).isFalse();
        assertThat(session.openAttempts.get())
                .as("recovery must stop at the budget, not retry forever")
                .isEqualTo(10);
        assertThat(loop.getReconnectCount()).isZero();
    }

    @Test
    void aFatalFaultDuringRecoveryStopsImmediatelyInsteadOfBurningTheBudget() throws Exception {
        BindingConfig config = config("REC.FATAL", 3);
        DeadOnReopenSession session = new DeadOnReopenSession(
                connection, config, "MQ reported: not authorized for channel");

        TransactedReceiveLoop loop = loop(config, new CountingWriter(), session);
        Future<?> future = executor.submit(loop);
        awaitTrue(2_000, loop::isRunning);

        session.breakPermanently();

        future.get(10, TimeUnit.SECONDS);

        assertThat(loop.isRunning()).isFalse();
        assertThat(session.openAttempts.get())
                .as("reconnecting cannot fix bad credentials — one attempt, then stop")
                .isEqualTo(1);
    }

    @Test
    void aRunOfUnrecognisedFaultsForcesRecoveryInsteadOfStallingForever() throws Exception {
        // The fault policy's documented blind spot: an exception whose text
        // matches neither BROKEN nor FATAL used to fail-pause-retry forever —
        // zero throughput, actuator health stale, supervisor blind (the
        // thread never dies). After a run of unrecognised faults the loop now
        // presumes the session broken and forces a rebuild.
        BindingConfig config = config("REC.UNRECOG", 3);
        UnrecognisedFaultSession session = new UnrecognisedFaultSession(connection, config);
        CountingWriter writer = new CountingWriter();

        TransactedReceiveLoop loop = loop(config, writer, session);
        Future<?> future = executor.submit(loop);
        awaitTrue(2_000, loop::isRunning);

        session.breakUnrecognisably();
        send(config.getSourceQueue(), "m1", "m2", "m3");

        // 10 unmatched faults, then forced recovery; the reopen heals the
        // session and consumption resumes.
        awaitTrue(15_000, () -> loop.getCommitCount() >= 1);

        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(loop.getReconnectCount())
                .as("the escape hatch is a real recovery cycle").isEqualTo(1);
        assertThat(writer.written.get()).isEqualTo(3);
        assertThat(session.faultsThrown.get())
                .as("escalation happens after the tolerated run, not immediately")
                .isGreaterThanOrEqualTo(10);
    }

    // --- harness ---

    private TransactedReceiveLoop loop(BindingConfig config, BatchWriter writer,
                                       ListenerSession session) {
        return new TransactedReceiveLoop(
                config, connection, writer, null, null, null, null, null, null,
                "recovery-test", RECEIVE_TIMEOUT_MS,
                session, null /* default fault policy */, BackoffPolicy.fixed(Duration.ofMillis(1)),
                null /* system clock */);
    }

    private BindingConfig config(String sourceQueue, int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId("recovery-test");
        config.setMode(BindingMode.LAND_ONLY);
        config.setSourceQueue(sourceQueue);
        config.getBatch().setSize(batchSize);
        config.getBatch().setBytes(1024 * 1024);
        config.getBatch().setIntervalMs(0);
        return config;
    }

    private void send(String queue, String... bodies) throws Exception {
        MessageProducer producer =
                producerSession.createProducer(producerSession.createQueue(queue));
        for (String body : bodies) {
            producer.send(producerSession.createTextMessage(body));
        }
        producer.close();
    }

    private void awaitTrue(long timeoutMs, Check check) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (check.satisfied()) {
                return;
            }
            Thread.sleep(20);
        }
    }

    private interface Check {
        boolean satisfied();
    }

    private static final class CountingWriter implements BatchWriter {
        final AtomicInteger written = new AtomicInteger();

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages,
                                     java.time.Instant partitionInstant) {
            written.addAndGet(messages.size());
            return new BatchWriteResult("/fake/file.seq", messages.size(), 1_000);
        }
    }

    /**
     * Throws one broken-classified receive when armed. "session"/"broken"
     * match the default policy's requiresRecovery matcher and none of its
     * fatal matchers, so recovery runs and the reopen (against the live
     * embedded broker) succeeds.
     */
    private static final class BreakOnceSession extends ListenerSession {
        private final AtomicBoolean breakNext = new AtomicBoolean(false);

        BreakOnceSession(Connection connection, BindingConfig config) {
            super(connection, config);
        }

        void breakNextReceive() {
            breakNext.set(true);
        }

        @Override
        public MessageConsumer consumer() {
            return new FaultingConsumer(super.consumer(),
                    () -> breakNext.compareAndSet(true, false),
                    "simulated broken session");
        }
    }

    /**
     * Once broken: every receive throws broken-classified, and every reopen
     * throws the configured text — so the outcome is decided entirely by how
     * the fault policy classifies that text (retryable vs fatal).
     */
    private static final class DeadOnReopenSession extends ListenerSession {
        private final AtomicBoolean broken = new AtomicBoolean(false);
        private final String reopenFailureText;
        final AtomicInteger openAttempts = new AtomicInteger();

        DeadOnReopenSession(Connection connection, BindingConfig config,
                            String reopenFailureText) {
            super(connection, config);
            this.reopenFailureText = reopenFailureText;
        }

        void breakPermanently() {
            broken.set(true);
        }

        @Override
        public void open() throws JMSException {
            if (broken.get()) {
                openAttempts.incrementAndGet();
                throw new JMSException(reopenFailureText);
            }
            super.open();
        }

        @Override
        public MessageConsumer consumer() {
            return new FaultingConsumer(super.consumer(), broken::get,
                    "simulated broken session");
        }
    }

    /**
     * While armed, every receive throws a JMSException whose text matches
     * NONE of the default policy's matchers ("gremlins…" — no MQ reason code,
     * no connection/session/broken keywords). The forced-recovery reopen
     * heals it, proving the escalation path ends in a working session.
     */
    private static final class UnrecognisedFaultSession extends ListenerSession {
        private final AtomicBoolean faulting = new AtomicBoolean(false);
        private final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger faultsThrown = new AtomicInteger();

        UnrecognisedFaultSession(Connection connection, BindingConfig config) {
            super(connection, config);
        }

        void breakUnrecognisably() {
            faulting.set(true);
        }

        @Override
        public void open() throws JMSException {
            super.open();
            if (opens.incrementAndGet() > 1) {
                faulting.set(false); // the recovery reopen fixes the session
            }
        }

        @Override
        public MessageConsumer consumer() {
            return new FaultingConsumer(super.consumer(), () -> {
                if (faulting.get()) {
                    faultsThrown.incrementAndGet();
                    return true;
                }
                return false;
            }, "gremlins ate the reply");
        }
    }

    /** Delegates to the real consumer unless the fault condition says throw. */
    private static final class FaultingConsumer implements MessageConsumer {
        private final MessageConsumer real;
        private final Check shouldThrow;
        private final String faultText;

        FaultingConsumer(MessageConsumer real, Check shouldThrow, String faultText) {
            this.real = real;
            this.shouldThrow = shouldThrow;
            this.faultText = faultText;
        }

        @Override
        public Message receive(long timeout) throws JMSException {
            if (shouldThrow.satisfied()) {
                throw new JMSException(faultText);
            }
            return real.receive(timeout);
        }

        @Override
        public Message receive() throws JMSException {
            return real.receive();
        }

        @Override
        public Message receiveNoWait() throws JMSException {
            return real.receiveNoWait();
        }

        @Override
        public String getMessageSelector() throws JMSException {
            return real.getMessageSelector();
        }

        @Override
        public MessageListener getMessageListener() throws JMSException {
            return real.getMessageListener();
        }

        @Override
        public void setMessageListener(MessageListener listener) throws JMSException {
            real.setMessageListener(listener);
        }

        @Override
        public void close() throws JMSException {
            real.close();
        }
    }
}
