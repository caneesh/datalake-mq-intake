package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.batch.CountingBatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradationPolicy;
import com.hcsc.datalake.mqintake.core.failure.FailureClass;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.poison.PoisonScreen;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the receive loop's fine-grained invariants — the ones its comments
 * argue for at length and no test held.
 *
 * <p>Written because a decomposition of {@code TransactedReceiveLoop} was
 * proposed and the safety net was measured before starting. Mutating the loop
 * showed the suite caught the coarse orderings (the commit flag, tracker
 * before audit) and missed the fine ones: message identifiers could stop being
 * collected entirely, the shutdown drain could commit with the interrupt flag
 * still set, and 67 tests stayed green. Those are exactly the seams a
 * decomposition cuts, so a green build after refactoring would not have been
 * evidence of anything.
 *
 * <p>Every test here was verified to FAIL against the specific mutation it
 * describes. A characterisation test that passes against the bug it names is
 * worse than no test, because it is read as proof.
 *
 * <p>These pin BEHAVIOUR, not structure. Wherever these responsibilities end
 * up living, the assertions should still hold — that is what makes them useful
 * to refactor against.
 */
class LoopInvariantCharacterisationTest {

    private static final String SOURCE_QUEUE = "CHAR.SOURCE";
    private static final String BACKOUT_QUEUE = "CHAR.BACKOUT";
    private static final String TRACKER_QUEUE = "CHAR.TRACKER";
    private static final long RECEIVE_TIMEOUT_MS = 100;

    private Connection connection;
    private Session producerSession;
    private MessageProducer producer;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = producerSession.createProducer(producerSession.createQueue(SOURCE_QUEUE));
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (producer != null) producer.close();
        if (producerSession != null) producerSession.close();
        if (connection != null) connection.close();
    }

    // ---------------------------------------------------------------------
    // 1. Message identifiers are collected from the batch BEFORE anything is
    //    sent, because a send reassigns them.
    // ---------------------------------------------------------------------

    @Test
    void aCommittedBatchClearsTheIdentifiersItActuallyConsumed() throws Exception {
        // Mutation this must fail against: collecting no identifiers at all
        // (`List.of()`), which silently disables suspect tracking. Sixty-seven
        // tests passed with that in place.
        RecordingPolicy policy = new RecordingPolicy(10);
        CountingBatchWriter writer = new CountingBatchWriter();
        List<String> sentIds = send(3);

        runUntilCommitted(config(3, false), writer, null, policy, 1);

        assertThat(policy.cleared)
                .as("the identifiers of every message in the committed batch")
                .containsExactlyInAnyOrderElementsOf(sentIds);
    }

    @Test
    void aFailedBatchMarksTheIdentifiersItActuallyConsumed() throws Exception {
        RecordingPolicy policy = new RecordingPolicy(10);
        CountingBatchWriter writer = new CountingBatchWriter();
        writer.failNextWrites(1, "Failed to serialize message: malformed payload");
        List<String> sentIds = send(3);

        runUntilRolledBack(config(3, false), writer, null, policy, 1);

        assertThat(policy.markedOnFailure)
                .as("a data failure marks the batch's own identifiers suspect")
                .containsExactlyInAnyOrderElementsOf(sentIds);
    }

    @Test
    void identifiersSurviveARoutedPoisonMessageInTheSameBatch() throws Exception {
        // The reason the collection happens first, stated in the loop: routing
        // a message to the backout queue sends it, and a send assigns a NEW
        // identifier. Collected afterwards, the routed message's recorded
        // identifier would be one the source queue never had.
        RecordingPolicy policy = new RecordingPolicy(10);
        CountingBatchWriter writer = new CountingBatchWriter();

        List<String> sentIds = new ArrayList<>(send(2));
        sentIds.addAll(sendWithDeliveryCount(1, 9));   // over the threshold

        PoisonScreen screen = new PoisonMessageHandler(5, BACKOUT_QUEUE);

        runUntilCommitted(config(3, true), writer, screen, policy, 1);

        assertThat(policy.cleared)
                .as("including the routed message, by the identifier it had on the source queue")
                .containsExactlyInAnyOrderElementsOf(sentIds);
    }

    // ---------------------------------------------------------------------
    // 2. The shutdown drain must not attempt its commit while the thread is
    //    still marked interrupted.
    // ---------------------------------------------------------------------

    @Test
    void theShutdownDrainCommitsWithTheInterruptFlagCleared() throws Exception {
        // stop() interrupts the loop to break its blocking receive, and the
        // drain then has to commit. A messaging client may fail an in-flight
        // call on a thread that is still marked interrupted, which would roll
        // the final batch back on EVERY clean shutdown instead of landing it.
        //
        // The embedded broker tolerates the interrupt, which is why no test
        // caught the flag being left set. This writer refuses to work on an
        // interrupted thread, standing in for a client that does the same.
        InterruptSensitiveWriter writer = new InterruptSensitiveWriter();
        BindingConfig config = config(100, false);   // never fills; only the drain flushes

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, null, null, null, null, null,
                "characterisation", RECEIVE_TIMEOUT_MS);
        Future<?> future = executor.submit(loop);

        send(3);
        awaitQueueEmpty(3000);        // all three are in the in-flight batch

        loop.stop();                  // sets running=false, then interrupts
        future.get(5, TimeUnit.SECONDS);

        assertThat(writer.sawInterruptedThread)
                .as("the drain must clear the interrupt before doing its work")
                .isFalse();
        assertThat(loop.getCommitCount())
                .as("the final batch lands rather than rolling back")
                .isEqualTo(1);
        assertThat(countOnQueue(SOURCE_QUEUE)).isZero();
    }

    // ---------------------------------------------------------------------
    // 3. A failure AFTER the commit is contained: never rolled back, never
    //    marked suspect.
    // ---------------------------------------------------------------------

    @Test
    void aFailureAfterTheCommitIsNeverMarkedSuspect() throws Exception {
        // Those identifiers are off the queue and can never be redelivered, so
        // nothing could ever retire them: marking them would hold the binding
        // at a reduced batch size for the life of the process.
        RecordingPolicy policy = new RecordingPolicy(10);
        policy.throwOnSuccess = true;   // fails during post-commit bookkeeping
        CountingBatchWriter writer = new CountingBatchWriter();
        send(2);

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config(2, false), connection, writer, null, null, policy, null, null,
                null, "characterisation", RECEIVE_TIMEOUT_MS);
        Future<?> future = executor.submit(loop);
        awaitQueueEmpty(3000);
        Thread.sleep(300);
        loop.stop();
        future.get(5, TimeUnit.SECONDS);

        assertThat(loop.getCommitCount()).as("the commit stands").isEqualTo(1);
        assertThat(policy.markedOnFailure)
                .as("post-commit bookkeeping failures must not create suspects")
                .isEmpty();
        assertThat(countOnQueue(SOURCE_QUEUE))
                .as("nothing is redelivered — the messages were acknowledged")
                .isZero();
    }

    // ---------------------------------------------------------------------
    // 4. The balance check runs BEFORE anything leaves the unit of work.
    // ---------------------------------------------------------------------

    @Test
    void anUnbalancedBatchSendsNoTrackerMessageAndWritesNoAudit() throws Exception {
        // Every message consumed must be observed either written or diverted.
        // When that does not hold the batch must roll back having done nothing
        // externally visible — so the check has to run before the tracker send
        // and before the audit, not after them.
        //
        // Ordering it later would put acknowledgements on the tracker queue and
        // an audit record on storage for a batch that then rolls back: the
        // tracker consumer would see an acknowledgement for data that was never
        // committed, and the audit would account for a file the balance says is
        // wrong. Neither can be withdrawn.
        RecordingAuditEmitter audit = new RecordingAuditEmitter();
        BindingConfig config = config(2, false);
        config.setMode(BindingMode.TRACKED);
        config.getTracker().setQueue(TRACKER_QUEUE);
        config.getAudit().setBalanceCheckEnabled(true);

        send(2);

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, new UnderReportingWriter(),
                (session, source) -> java.util.Optional.of(
                        session.createTextMessage("ack")),
                null, null, null, audit, null, "characterisation", RECEIVE_TIMEOUT_MS);
        Future<?> future = executor.submit(loop);

        long deadline = System.currentTimeMillis() + 5000;
        while (loop.getRollbackCount() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        loop.stop();
        future.get(5, TimeUnit.SECONDS);

        assertThat(loop.getRollbackCount())
                .as("an unbalanced batch must never commit").isGreaterThanOrEqualTo(1);
        assertThat(loop.getCommitCount()).isZero();
        assertThat(countOnQueue(TRACKER_QUEUE))
                .as("no acknowledgement for a batch that did not commit").isZero();
        assertThat(audit.emitted)
                .as("no audit record for a batch the balance check refused").isZero();
    }

    // ---------------------------------------------------------------------
    // 5. A flush leaves no accumulator state behind.
    // ---------------------------------------------------------------------

    @Test
    void eachFlushStartsFromAnEmptyBatch() throws Exception {
        // Whatever holds the batch after a decomposition must reset every part
        // of its state together — messages, byte accounting and flush state.
        // A partial reset shows up as batches that grow or shrink.
        CountingBatchWriter writer = new CountingBatchWriter();
        send(6);

        runUntilCommitted(config(2, false), writer, null, null, 3);

        assertThat(writer.getBatchSizes())
                .as("three flushes of exactly two, not a batch carrying remnants")
                .containsExactly(2, 2, 2);
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    private void runUntilCommitted(BindingConfig config, BatchWriter writer, PoisonScreen screen,
                                   DegradationPolicy policy, int commits) throws Exception {
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, screen, policy, null, null, null,
                "characterisation", RECEIVE_TIMEOUT_MS);
        Future<?> future = executor.submit(loop);
        long deadline = System.currentTimeMillis() + 5000;
        while (loop.getCommitCount() < commits && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        loop.stop();
        future.get(5, TimeUnit.SECONDS);
        assertThat(loop.getCommitCount()).isGreaterThanOrEqualTo(commits);
    }

    private void runUntilRolledBack(BindingConfig config, BatchWriter writer, PoisonScreen screen,
                                    DegradationPolicy policy, int rollbacks) throws Exception {
        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, screen, policy, null, null, null,
                "characterisation", RECEIVE_TIMEOUT_MS);
        Future<?> future = executor.submit(loop);
        long deadline = System.currentTimeMillis() + 5000;
        while (loop.getRollbackCount() < rollbacks && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        loop.stop();
        future.get(5, TimeUnit.SECONDS);
        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(rollbacks);
    }

    private BindingConfig config(int batchSize, boolean withBackout) {
        BindingConfig config = new BindingConfig();
        config.setId("characterisation");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.getHdfs().setBasePath("/tmp/characterisation");
        config.getBatch().setSize(batchSize);
        config.getBatch().setBytes(64L * 1024 * 1024);
        config.getBatch().setIntervalMs(0);
        config.setListenerThreads(1);
        if (withBackout) {
            config.getBackout().setQueue(BACKOUT_QUEUE);
            config.getBackout().setThreshold(5);
        }
        return config;
    }

    /** Sends messages and returns the identifiers the queue assigned them. */
    private List<String> send(int count) throws JMSException {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TextMessage message = producerSession.createTextMessage("body-" + i);
            producer.send(message);
            ids.add(message.getJMSMessageID());
        }
        return ids;
    }

    /** Sends messages that already look redelivered, so the screen routes them. */
    private List<String> sendWithDeliveryCount(int count, int deliveryCount) throws JMSException {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TextMessage message = producerSession.createTextMessage("poison-" + i);
            message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, deliveryCount);
            producer.send(message);
            ids.add(message.getJMSMessageID());
        }
        return ids;
    }

    private int countOnQueue(String queueName) throws JMSException {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(queueName);
            QueueBrowser browser = session.createBrowser(queue);
            int count = 0;
            var messages = browser.getEnumeration();
            while (messages.hasMoreElements()) {
                messages.nextElement();
                count++;
            }
            browser.close();
            return count;
        }
    }

    private void awaitQueueEmpty(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (countOnQueue(SOURCE_QUEUE) == 0) {
                return;
            }
            Thread.sleep(25);
        }
    }

    /** Reports one fewer record than it was given, breaking the balance. */
    private static class UnderReportingWriter implements BatchWriter {
        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages) {
            return new BatchWriteResult(
                    "/tmp/characterisation/f.seq", messages.size() - 1, 100L);
        }
    }

    /** Counts audit records so their absence can be asserted. */
    private static class RecordingAuditEmitter
            implements com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter {
        volatile int emitted = 0;

        @Override
        public void emit(com.hcsc.datalake.mqintake.core.audit.AuditRecord record) {
            emitted++;
        }

        @Override
        public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                         List<Message> messages) {
            emitted++;
        }

        @Override
        public void emitBackoutOnly(String bindingId, List<Message> messages, int backoutCount) {
            emitted++;
        }
    }

    /** Refuses to work on an interrupted thread, as a real client may. */
    private static class InterruptSensitiveWriter implements BatchWriter {
        volatile boolean sawInterruptedThread = false;

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages)
                throws BatchWriteException {
            if (Thread.currentThread().isInterrupted()) {
                sawInterruptedThread = true;
                throw new BatchWriteException("interrupted thread — call refused");
            }
            return new BatchWriteResult("/tmp/characterisation/f.seq", messages.size(), 100L);
        }
    }

    /** Records exactly which identifiers reach the degradation policy. */
    private static class RecordingPolicy implements DegradationPolicy {
        final List<String> cleared = Collections.synchronizedList(new ArrayList<>());
        final List<String> markedOnFailure = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger successes = new AtomicInteger();
        final AtomicBoolean degraded = new AtomicBoolean(false);
        volatile boolean throwOnSuccess = false;

        private final int batchSize;

        RecordingPolicy(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public int getCurrentBatchSize() {
            return batchSize;
        }

        @Override
        public boolean isInDegradedMode() {
            return degraded.get();
        }

        @Override
        public boolean recordSuccess() {
            successes.incrementAndGet();
            if (throwOnSuccess) {
                // Stands in for any post-commit bookkeeping that can fail.
                throw new IllegalStateException("post-commit bookkeeping failed");
            }
            return false;
        }

        @Override
        public FailureResult recordFailure(Throwable throwable, Collection<String> ids) {
            if (ids != null) {
                markedOnFailure.addAll(ids);
            }
            return new FailureResult(FailureClass.MESSAGE_DATA, false);
        }

        @Override
        public void clearSuspects(Collection<String> messageIds) {
            cleared.addAll(messageIds);
        }
    }
}
