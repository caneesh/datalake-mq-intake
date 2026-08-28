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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The backout routing gate.
 *
 * <p>Routing is delivery-count based, exactly like the legacy MDB, which
 * cannot distinguish a malformed message from a healthy one that sat in
 * several batches which rolled back for an unrelated reason. With a large
 * batch and a backout queue sized for poison rather than whole batches, a
 * landing-path outage lasting a few retry cycles would otherwise divert
 * thousands of healthy messages onto the BOQ — safe, but requiring manual
 * replay, and capable of filling the queue and wedging ingestion.
 *
 * <p>The gate must close under infrastructure failures and open for
 * message-data ones — and, critically, also open for UNKNOWN, or a genuine
 * poison message whose failure resists classification would redeliver forever
 * with no escape.
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

    private TextMessage overThreshold(String body) throws Exception {
        TextMessage message = session.createTextMessage(body);
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 9);
        return message;
    }

    @Test
    void aClosedGateRetriesInsteadOfDiverting() throws Exception {
        AtomicBoolean open = new AtomicBoolean(false);
        PoisonMessageHandler handler =
                new PoisonMessageHandler(5, BACKOUT_QUEUE, open::get);

        PoisonMessageHandler.BatchPoisonCheckResult result = handler.screen(session,
                List.of(overThreshold("good-1"), overThreshold("good-2")));

        assertThat(result.getPoisonCount())
                .as("nothing may be diverted while the gate is closed").isZero();
        assertThat(result.getCleanMessages())
                .as("the whole batch is handed back for the normal path").hasSize(2);
    }

    @Test
    void anOpenGateRoutesExactlyAsBefore() throws Exception {
        PoisonMessageHandler handler =
                new PoisonMessageHandler(5, BACKOUT_QUEUE, () -> true);

        PoisonMessageHandler.BatchPoisonCheckResult result = handler.screen(session,
                List.of(overThreshold("poison-1"), session.createTextMessage("fresh")));

        assertThat(result.getPoisonCount()).isEqualTo(1);
        assertThat(result.getCleanMessages()).hasSize(1);
    }

    @Test
    void theDefaultConstructorKeepsLegacyUngatedBehaviour() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(5, BACKOUT_QUEUE);

        assertThat(handler.screen(session, List.of(overThreshold("poison")))
                .getPoisonCount()).isEqualTo(1);
    }

    // --- what the factory wires the gate to ---

    @Test
    void infrastructureFailuresCloseTheGateAndDataFailuresOpenIt() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 1000, DegradationStrategy.BATCH_OF_ONE, 10);

        // Before any failure the gate is open: a message already over the
        // threshold when the process starts must still be routable.
        assertThat(gateFor(manager).getAsBoolean()).isTrue();

        manager.recordFailure(new IOException("HDFS NameNode unavailable"));
        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(gateFor(manager).getAsBoolean())
                .as("an outage must not divert healthy messages").isFalse();

        manager.recordFailure(new RecordSerializer.SerializationException("bad payload"));
        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(gateFor(manager).getAsBoolean())
                .as("a data failure means the message itself is suspect").isTrue();
    }

    @Test
    void unknownFailuresKeepTheGateOpenSoPoisonAlwaysHasAnEscape() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 1000, DegradationStrategy.BATCH_OF_ONE, 10);

        manager.recordFailure(new RuntimeException("totally unexpected"));

        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.UNKNOWN);
        assertThat(gateFor(manager).getAsBoolean())
                .as("suppressing UNKNOWN would strand an unclassifiable poison message "
                        + "in an endless redelivery loop")
                .isTrue();
    }

    @Test
    void aBareWritePathFailureIsInfrastructureNotUnknown() {
        // A rename returning false throws BatchWriteException with no cause —
        // nothing for the classifier's cause-walk to find. It used to land in
        // UNKNOWN, which permits routing, so a landing-path permissions fault
        // could still divert healthy messages.
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 1000, DegradationStrategy.BATCH_OF_ONE, 10);

        manager.recordFailure(new com.hcsc.datalake.mqintake.core.batch.BatchWriter
                .BatchWriteException("Failed to rename temp file to partition: /_tmp/x -> /y"));

        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(gateFor(manager).getAsBoolean()).isFalse();
    }

    @Test
    void aSerializerFailureWrappedInAWritePathExceptionIsStillMessageData() {
        // The fallback above must never shadow the real cause: the production
        // writer wraps a SerializationException in BatchWriteException, and
        // misreading that as infrastructure would disable poison isolation.
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 1000, DegradationStrategy.BATCH_OF_ONE, 10);

        manager.recordFailure(new com.hcsc.datalake.mqintake.core.batch.BatchWriter
                .BatchWriteException("Failed to serialize message: bad payload",
                        new RecordSerializer.SerializationException("bad payload")));

        assertThat(manager.getLastFailureClass()).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(manager.isInDegradedMode())
                .as("poison isolation must still engage").isTrue();
        assertThat(gateFor(manager).getAsBoolean()).isTrue();
    }

    @Test
    void everyFailureClassHasADeliberateRoutingStance() {
        // Pinned so a new FailureClass cannot silently inherit "route",
        // which is the direction that diverts healthy messages.
        assertThat(FailureClass.MESSAGE_DATA.permitsBackoutRouting()).isTrue();
        assertThat(FailureClass.UNKNOWN.permitsBackoutRouting()).isTrue();
        assertThat(FailureClass.HDFS_INFRASTRUCTURE.permitsBackoutRouting()).isFalse();
        assertThat(FailureClass.MQ_INFRASTRUCTURE.permitsBackoutRouting()).isFalse();
        assertThat(FailureClass.SECURITY_CONFIG.permitsBackoutRouting()).isFalse();
        assertThat(FailureClass.SHUTDOWN.permitsBackoutRouting()).isFalse();
    }

    /** Mirrors the supplier BindingRuntimeFactory builds for a gated binding. */
    private java.util.function.BooleanSupplier gateFor(DegradedModeManager manager) {
        return () -> {
            FailureClass last = manager.getLastFailureClass();
            return last == null || last.permitsBackoutRouting();
        };
    }
}
