package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.loop.recovery.BackoffPolicy;
import com.ibm.mq.jms.MQConnectionFactory;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * The connection path, without a queue manager.
 *
 * <p>This class exists because five mutations of {@code ManagedConnection}
 * left the default build green — the retry budget, the fault classification,
 * the linked-exception half of the matcher, the channel on the IBM factory,
 * and the shared-connection rule itself. Coverage was not absent, it was
 * gated: {@code ManagedConnectionAgainstRealMqTest} covers all of it and skips
 * unless {@code MQ_USER} is set, which is every ordinary build.
 *
 * <p>Nothing here reaches a broker. The IBM opener stays on the far side of
 * {@link ConnectionOpener} and keeps its real-MQ tests; what is driven here is
 * everything that decides whether to open again, which never needed one.
 */
class ManagedConnectionTest {

    private static final String ID = "primary";

    // --- the retry budget ---

    @Test
    void aTransientFailureIsRetriedUpToTheConfiguredBudgetAndThenReported() {
        CountingOpener opener = new CountingOpener(new JMSException("listener not up"));
        ManagedConnection managed = managed(config(4), opener);

        assertThatThrownBy(managed::getConnection)
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageContaining("after 4 attempts");

        assertThat(opener.attempts.get())
                .as("every attempt in the budget is spent before giving up").isEqualTo(4);
    }

    @Test
    void aBudgetOfOneMeansOneAttempt() {
        CountingOpener opener = new CountingOpener(new JMSException("down"));

        assertThatThrownBy(() -> managed(config(1), opener).getConnection())
                .isInstanceOf(MqConnectionManager.MqConnectionException.class);

        assertThat(opener.attempts.get()).isEqualTo(1);
    }

    @Test
    void aConnectionThatSucceedsOnARetryIsReturnedWithoutError() throws Exception {
        Connection connection = mock(Connection.class);
        CountingOpener opener = new CountingOpener(new JMSException("blip"), connection, 3);

        assertThat(managed(config(5), opener).getConnection()).isSameAs(connection);
        assertThat(opener.attempts.get()).isEqualTo(3);
    }

    @Test
    void theBackoffPolicyIsConsultedOncePerRetryAndNotAfterTheLastAttempt() {
        RecordingBackoff backoff = new RecordingBackoff();
        CountingOpener opener = new CountingOpener(new JMSException("down"));

        assertThatThrownBy(() -> new ManagedConnection(config(3), opener, backoff).getConnection())
                .isInstanceOf(MqConnectionManager.MqConnectionException.class);

        // Three attempts, two gaps between them. Waiting after the final
        // attempt would delay the failure the caller is already going to get.
        assertThat(backoff.attempts).containsExactly(1, 2);
    }

    // --- fault classification: what must NOT be retried ---

    @Test
    void aConfigurationFaultFailsOnTheFirstAttemptInsteadOfBurningTheBudget() {
        JMSException wrongQueueManager = new JMSException(
                "JMSWMQ0018: Failed to connect to queue manager 'QM1'");
        wrongQueueManager.setLinkedException(
                new Exception("... reason '2058' ('MQRC_Q_MGR_NAME_ERROR')"));
        CountingOpener opener = new CountingOpener(wrongQueueManager);

        assertThatThrownBy(() -> managed(config(10), opener).getConnection())
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageContaining("Configuration error");

        assertThat(opener.attempts.get())
                .as("retrying a misconfiguration only delays the real error").isEqualTo(1);
    }

    @Test
    void theReasonCodeIsFoundInTheLinkedExceptionNotTheTopLevelMessage() {
        // The half that works. IBM MQ reports every failed connect as
        // JMSWMQ0018 — wrong queue manager, wrong channel and an unreachable
        // listener are indistinguishable at the top level. Matching only there
        // retried every misconfiguration to exhaustion.
        JMSException topLevelOnly = new JMSException(
                "JMSWMQ0018: Failed to connect to queue manager 'QM1'");
        CountingOpener opener = new CountingOpener(topLevelOnly);

        assertThatThrownBy(() -> managed(config(3), opener).getConnection())
                .isInstanceOf(MqConnectionManager.MqConnectionException.class);

        assertThat(opener.attempts.get())
                .as("indistinguishable from a listener that is not up yet, so it retries")
                .isEqualTo(3);
    }

    @Test
    void anUnresolvableCredentialIsAConfigurationFaultAndIsNeverRetried() {
        CountingOpener opener = new CountingOpener(
                new MqConnectionManager.MqCredentialException("no such secret"));

        assertThatThrownBy(() -> managed(config(10), opener).getConnection())
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageContaining("Configuration error");

        assertThat(opener.attempts.get()).isEqualTo(1);
    }

    @Test
    void atransientHostFaultKeepsRetrying() {
        // MQRC_HOST_NOT_AVAILABLE and MQRC_CHANNEL_NOT_AVAILABLE are absent
        // from the do-not-retry list on purpose: a listener not up yet and
        // every channel instance busy are both temporary. Treating either as
        // fatal turns a queue manager restart into a startup failure.
        for (String reason : List.of("MQRC_HOST_NOT_AVAILABLE", "MQRC_CHANNEL_NOT_AVAILABLE")) {
            JMSException transientFault = new JMSException("JMSWMQ0018: Failed to connect");
            transientFault.setLinkedException(new Exception("reason '2538' ('" + reason + "')"));
            CountingOpener opener = new CountingOpener(transientFault);

            assertThatThrownBy(() -> managed(config(3), opener).getConnection())
                    .isInstanceOf(MqConnectionManager.MqConnectionException.class);

            assertThat(opener.attempts.get()).as("%s must keep retrying", reason).isEqualTo(3);
        }
    }

    // --- the sharing rule (standing constraint #2) ---

    @Test
    void everyCallerGetsTheSameConnectionAndItIsOpenedOnlyOnce() throws Exception {
        Connection connection = mock(Connection.class);
        CountingOpener opener = new CountingOpener(null, connection, 1);
        ManagedConnection managed = managed(config(3), opener);

        Connection first = managed.getConnection();
        Connection second = managed.getConnection();

        assertThat(second).as("one Connection per queue manager, shared").isSameAs(first);
        assertThat(opener.attempts.get())
                .as("a second caller must not open a second connection").isEqualTo(1);
    }

    @Test
    void closingReleasesTheConnectionAndTheNextCallerOpensAFreshOne() throws Exception {
        Connection first = mock(Connection.class);
        Connection second = mock(Connection.class);
        SequenceOpener opener = new SequenceOpener(first, second);
        ManagedConnection managed = managed(config(3), opener);

        assertThat(managed.getConnection()).isSameAs(first);
        managed.close();
        assertThat(managed.getConnection()).isSameAs(second);
    }

    @Test
    void closingTwiceIsHarmlessAndClosingBeforeOpeningDoesNothing() {
        ManagedConnection managed = managed(config(3), new SequenceOpener(mock(Connection.class)));

        managed.close();
        managed.close();
    }

    @Test
    void aConnectionThatFailedToStartIsNotHandedToTheNextCaller() throws Exception {
        // Found by reading the loop after extracting it: the field is assigned
        // before start(), so a start() that throws leaves a non-null,
        // never-started Connection behind. Once the budget is exhausted the
        // caller gets an exception — but the NEXT caller finds the field
        // non-null and is handed the dead connection with no error at all.
        //
        // Reachable with two bindings sharing one queue manager: the first
        // fails startup, the second asks for the same connection id and gets a
        // connection nothing ever started. Consuming from it silently returns
        // nothing.
        Connection neverStarts = mock(Connection.class);
        org.mockito.Mockito.doThrow(new JMSException("broker refused start"))
                .when(neverStarts).start();
        // Opening always works; starting never does.
        ManagedConnection managed = managed(config(2), new CountingOpener(null, neverStarts, 1));

        assertThatThrownBy(managed::getConnection)
                .isInstanceOf(MqConnectionManager.MqConnectionException.class);

        assertThatThrownBy(managed::getConnection)
                .as("the second caller must not receive the connection that failed to start")
                .isInstanceOf(MqConnectionManager.MqConnectionException.class);
    }

    @Test
    void aConnectionThatFailsToStartIsClosedRatherThanAbandoned() throws Exception {
        // An abandoned connection holds an MQ channel instance until the
        // client object is collected. Channel instances are a server-side
        // resource with a MAXINST limit, so a binding retrying its way through
        // the budget could deny channels to every other application on that
        // queue manager — reported to them as MQRC_CHANNEL_NOT_AVAILABLE,
        // which looks like their problem.
        Connection neverStarts = mock(Connection.class);
        org.mockito.Mockito.doThrow(new JMSException("broker refused start"))
                .when(neverStarts).start();

        assertThatThrownBy(() ->
                managed(config(3), new CountingOpener(null, neverStarts, 1)).getConnection())
                .isInstanceOf(MqConnectionManager.MqConnectionException.class);

        org.mockito.Mockito.verify(neverStarts, org.mockito.Mockito.times(3)).close();
    }

    @Test
    void aCleanupThatAlsoFailsDoesNotReplaceTheFailureThatMattered() throws Exception {
        // The operator needs to know why the connect failed. Letting a
        // secondary close error propagate would replace the diagnosis with a
        // symptom.
        Connection broken = mock(Connection.class);
        org.mockito.Mockito.doThrow(new JMSException("broker refused start"))
                .when(broken).start();
        org.mockito.Mockito.doThrow(new JMSException("and the close failed too"))
                .when(broken).close();

        assertThatThrownBy(() ->
                managed(config(2), new CountingOpener(null, broken, 1)).getConnection())
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageContaining("after 2 attempts")
                .cause()
                .hasMessageContaining("broker refused start");
    }

    // --- interruption ---

    @Test
    void anInterruptDuringBackoffIsReportedAndLeavesTheFlagSet() {
        // Swallowing the interrupt is how a shutdown stalls: the thread that
        // asked to stop never learns it was interrupted, and the listener it
        // belongs to keeps going until something else notices. Nothing held
        // this — deleting the interrupt() call changed no test result.
        BackoffPolicy interruptsWhileWaiting = attempt -> {
            Thread.currentThread().interrupt();
            return Duration.ofSeconds(10);   // so the sleep throws at once
        };
        CountingOpener opener = new CountingOpener(new JMSException("down"));

        try {
            assertThatThrownBy(() ->
                    new ManagedConnection(config(3), opener, interruptsWhileWaiting)
                            .getConnection())
                    .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                    .hasMessageContaining("interrupted");

            assertThat(Thread.currentThread().isInterrupted())
                    .as("the caller must still see the interrupt").isTrue();
            assertThat(opener.attempts.get())
                    .as("and the budget is abandoned, not spent").isEqualTo(1);
        } finally {
            // Clear it, or the flag leaks into whatever runs next on this thread.
            Thread.interrupted();
        }
    }

    @Test
    void anAlreadyInterruptedThreadOpensNothing() {
        CountingOpener opener = new CountingOpener(new JMSException("down"));
        ManagedConnection managed = managed(config(3), opener);

        try {
            Thread.currentThread().interrupt();

            assertThatThrownBy(managed::getConnection)
                    .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                    .hasMessageContaining("interrupted");

            assertThat(opener.attempts.get())
                    .as("a connect that was asked to stop before it began opens nothing")
                    .isZero();
        } finally {
            Thread.interrupted();
        }
    }

    // --- the IBM factory, which needs no broker to check ---

    @Test
    void everyConfiguredFieldReachesTheIbmConnectionFactory() throws Exception {
        MqConnectionConfig config = config(3);
        config.setHost("mq.example.test");
        config.setPort(1415);
        config.setQueueManager("QMX");
        config.setChannel("APP.SVRCONN");

        MQConnectionFactory factory = IbmMqConnectionOpener.buildConnectionFactory(config);

        assertThat(factory.getHostName()).isEqualTo("mq.example.test");
        assertThat(factory.getPort()).isEqualTo(1415);
        assertThat(factory.getQueueManager()).isEqualTo("QMX");
        assertThat(factory.getChannel())
                .as("a channel that never reaches the factory connects to the wrong one")
                .isEqualTo("APP.SVRCONN");
        assertThat(factory.getTransportType())
                .isEqualTo(MqTransportType.CLIENT.wmqConstant());
    }

    // --- harness ---

    private ManagedConnection managed(MqConnectionConfig config, ConnectionOpener opener) {
        // Zero backoff: the delay is the policy's business, tested above.
        return new ManagedConnection(config, opener, BackoffPolicy.fixed(Duration.ZERO));
    }

    private MqConnectionConfig config(int reconnectAttempts) {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId(ID);
        config.setHost("localhost");
        config.setQueueManager("QM1");
        config.setChannel("TEST.SVRCONN");
        config.setReconnectAttempts(reconnectAttempts);
        config.setReconnectDelayMs(0);
        return config;
    }

    /** Fails with the given exception until the nominated attempt succeeds. */
    private static class CountingOpener implements ConnectionOpener {
        final AtomicInteger attempts = new AtomicInteger();
        private final JMSException failure;
        private final Connection success;
        private final int succeedsOnAttempt;

        CountingOpener(JMSException failure) {
            this(failure, null, Integer.MAX_VALUE);
        }

        CountingOpener(JMSException failure, Connection success, int succeedsOnAttempt) {
            this.failure = failure;
            this.success = success;
            this.succeedsOnAttempt = succeedsOnAttempt;
        }

        @Override
        public Connection open() throws JMSException {
            int attempt = attempts.incrementAndGet();
            if (attempt >= succeedsOnAttempt) {
                return success;
            }
            throw failure;
        }
    }

    /** Hands out the given connections in order, one per call. */
    private static class SequenceOpener implements ConnectionOpener {
        private final List<Connection> remaining;

        SequenceOpener(Connection... connections) {
            this.remaining = new ArrayList<>(List.of(connections));
        }

        @Override
        public Connection open() {
            return remaining.remove(0);
        }
    }

    /** Records which attempt numbers were asked to wait. */
    private static class RecordingBackoff implements BackoffPolicy {
        final List<Integer> attempts = new ArrayList<>();

        @Override
        public Duration backoffFor(int attempt) {
            attempts.add(attempt);
            return Duration.ZERO;
        }
    }
}
