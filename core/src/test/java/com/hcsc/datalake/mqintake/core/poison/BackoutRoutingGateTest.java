package com.hcsc.datalake.mqintake.core.poison;

import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The backout routing gate.
 *
 * <p>Delivery count rises on every rollback, for any reason. After a
 * landing-path outage lasting a few retry cycles every message in flight is
 * over the threshold, and routing on count alone — the legacy MDB's rule —
 * diverts thousands of healthy messages onto a queue sized for poison. The
 * first version of this gate asked "was the last failure infrastructure?",
 * which is in-memory and open at start: the restart that follows an outage
 * routed the whole backlog on the first screen.
 *
 * <p>The gate now routes a message only once it has been CONFIRMED: it
 * failed with a data failure while alone in its unit of work. That is the
 * only observation that pins a failure on one message rather than on a batch
 * or on the infrastructure under it.
 */
class BackoutRoutingGateTest {

    private static final String BACKOUT_QUEUE = "TEST.GATE.BOQ";

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (session != null) session.close();
        if (connection != null) connection.close();
    }

    private int nextId = 1;

    /**
     * A message over the threshold, with a JMS message id assigned directly:
     * sending it through the broker to obtain one would reset the delivery
     * count, which is the very property under test.
     */
    private TextMessage overThreshold(String body) throws Exception {
        TextMessage message = session.createTextMessage(body);
        message.setJMSMessageID("ID:gate-test:1:1:" + (nextId++));
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 9);
        return message;
    }

    private DegradedModeManager rmsManager() {
        return new DegradedModeManager("rms", 1000, DegradationStrategy.BATCH_OF_ONE, 10);
    }

    @Test
    void unconfirmedMessagesAreRetriedNotDivertedHoweverHighTheirCount() throws Exception {
        DegradedModeManager manager = rmsManager();
        PoisonMessageHandler handler =
                new PoisonMessageHandler(5, BACKOUT_QUEUE, manager::isConfirmedPoison);

        PoisonMessageHandler.BatchPoisonCheckResult result = handler.screen(session,
                List.of(overThreshold("good-1"), overThreshold("good-2")));

        assertThat(result.getPoisonCount())
                .as("nothing may be diverted on delivery count alone").isZero();
        assertThat(result.getCleanMessages())
                .as("the whole batch is handed back for the normal path").hasSize(2);
    }

    @Test
    void aMessageThatFailedAloneWithADataFailureIsRoutedAndItsNeighboursAreNot() throws Exception {
        DegradedModeManager manager = rmsManager();
        TextMessage poison = overThreshold("poison");
        TextMessage healthy = overThreshold("healthy-but-rolled-back");

        // Batch of two fails on data: both suspect, neither confirmed.
        manager.recordFailure(new RecordSerializer.SerializationException("bad payload"),
                List.of(poison.getJMSMessageID(), healthy.getJMSMessageID()));
        assertThat(manager.isConfirmedPoison(poison.getJMSMessageID())).isFalse();

        // Isolated retry of the bad one fails again: now confirmed.
        manager.recordFailure(new RecordSerializer.SerializationException("bad payload"),
                List.of(poison.getJMSMessageID()));
        assertThat(manager.isConfirmedPoison(poison.getJMSMessageID())).isTrue();
        assertThat(manager.isConfirmedPoison(healthy.getJMSMessageID())).isFalse();

        // The broker re-ids a message on send, so capture before routing.
        String poisonId = poison.getJMSMessageID();
        PoisonMessageHandler handler =
                new PoisonMessageHandler(5, BACKOUT_QUEUE, manager::isConfirmedPoison);
        PoisonMessageHandler.BatchPoisonCheckResult result =
                handler.screen(session, List.of(poison, healthy));
        session.commit();

        assertThat(result.getRoutedMessages())
                .extracting(PoisonMessageHandler.BackoutResult::getMessageId)
                .containsExactly(poisonId);
        assertThat(result.getCleanMessages()).containsExactly(healthy);
    }

    @Test
    void anInfrastructureFailureWhileAloneDoesNotConfirmAnything() throws Exception {
        DegradedModeManager manager = rmsManager();
        TextMessage message = overThreshold("healthy");

        manager.recordFailure(new IOException("HDFS NameNode unavailable"),
                List.of(message.getJMSMessageID()));

        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(manager.isConfirmedPoison(message.getJMSMessageID()))
                .as("an outage must not divert healthy messages").isFalse();
    }

    @Test
    void anUnknownFailureWhileAloneDoesNotConfirmEither() throws Exception {
        // UNKNOWN never enters degraded mode, so it cannot confirm; a payload
        // that provokes a RuntimeException is wrapped by the serializers into
        // a SerializationException precisely so it classifies as data.
        DegradedModeManager manager = rmsManager();
        TextMessage message = overThreshold("odd");

        manager.recordFailure(new RuntimeException("totally unexpected"),
                List.of(message.getJMSMessageID()));

        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.UNKNOWN);
        assertThat(manager.isConfirmedPoison(message.getJMSMessageID())).isFalse();
    }

    @Test
    void confirmationIsClearedWhenTheMessageIsCommittedOrRouted() throws Exception {
        DegradedModeManager manager = rmsManager();
        TextMessage message = overThreshold("poison");
        manager.recordFailure(new RecordSerializer.SerializationException("bad"),
                List.of(message.getJMSMessageID()));
        assertThat(manager.isConfirmedPoison(message.getJMSMessageID())).isTrue();

        manager.clearSuspects(List.of(message.getJMSMessageID()));

        assertThat(manager.isConfirmedPoison(message.getJMSMessageID())).isFalse();
    }

    @Test
    void theDefaultConstructorKeepsLegacyCountOnlyBehaviour() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(5, BACKOUT_QUEUE);

        PoisonMessageHandler.BatchPoisonCheckResult result =
                handler.screen(session, List.of(overThreshold("poison")));
        session.commit();

        assertThat(result.getPoisonCount()).isEqualTo(1);
    }

    // --- classification the gate depends on ---

    @Test
    void aBareWritePathFailureIsInfrastructureNotUnknown() {
        // A rename returning false throws BatchWriteException with no cause —
        // nothing for the classifier's cause-walk to find. It used to land in
        // UNKNOWN.
        DegradedModeManager manager = rmsManager();

        manager.recordFailure(new com.hcsc.datalake.mqintake.core.batch.BatchWriter
                .BatchWriteException("Failed to rename temp file to partition: /_tmp/x -> /y"));

        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(manager.isInDegradedMode()).isFalse();
    }

    @Test
    void aSerializerFailureWrappedInAWritePathExceptionIsStillMessageData() {
        // The fallback above must never shadow the real cause: the production
        // writer wraps a SerializationException in BatchWriteException, and
        // misreading that as infrastructure would disable poison isolation.
        DegradedModeManager manager = rmsManager();

        manager.recordFailure(new com.hcsc.datalake.mqintake.core.batch.BatchWriter
                .BatchWriteException("Failed to serialize message: bad payload",
                        new RecordSerializer.SerializationException("bad payload")));

        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(manager.isInDegradedMode())
                .as("poison isolation must still engage").isTrue();
    }
}
