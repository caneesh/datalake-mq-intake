package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.audit.AuditRecord;
import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
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
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The transaction-time ABC balance control (RMS posture):
 * {@code mqConsumed == hdfsWritten + backout} must hold before the MQ commit,
 * with all three numbers independently observed — batch size at the receive
 * loop, written count from the writer's per-append counter, backout count
 * from the poison screen's routing result.
 *
 * <p>The previously persisted {@code consumed_count} was
 * {@code recordCount + backoutCount}: a derived value that balances by
 * construction and is therefore mathematically incapable of exposing a
 * dropped message. These tests use writers whose REPORTED count is decoupled
 * from the messages handed to them, which is exactly the disagreement a
 * derived count could never show.
 */
class AbcBalanceCheckTest {

    private static final long RECEIVE_TIMEOUT_MS = 50;

    private Connection connection;
    private Session producerSession;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        factory.getRedeliveryPolicy().setMaximumRedeliveries(-1);
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

    // --- Test 1: normal balanced batch ---

    @Test
    void balancedBatchCommits() throws Exception {
        String source = "ABC.BALANCED.SOURCE";
        send(source, bodies("CLEAN", 10));

        BindingConfig config = config(source, 10, true);
        ReportingWriter writer = ReportingWriter.honest();
        CapturingEmitter emitter = new CapturingEmitter();
        BindingMetrics metrics = new BindingMetrics("rms");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, null, null, null, emitter, metrics,
                "abc-test", RECEIVE_TIMEOUT_MS);
        executor.submit(loop);
        awaitTrue(5_000, () -> loop.getCommitCount() >= 1);
        loop.stop();

        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(loop.getRollbackCount()).isZero();
        assertThat(metrics.getBalanceCheckFailures()).isZero();
        assertThat(countOnQueue(source)).isZero();

        AuditRecord audit = emitter.records.get(0);
        assertThat(audit.getConsumedCount()).isEqualTo(10);
        assertThat(audit.getRecordCount()).isEqualTo(10);
        assertThat(audit.getBackoutCount()).isZero();
        assertThat(audit.getBalanceDelta()).isZero();
        assertThat(audit.isBalanced()).isTrue();
    }

    // --- Test 2: poison messages accounted for ---

    @Test
    void poisonRoutedToBackoutStillBalances() throws Exception {
        String source = "ABC.POISON.SOURCE";
        String boq = "ABC.POISON.BOQ";
        List<String> bodies = bodies("CLEAN", 8);
        bodies.add("POISON-1");
        bodies.add("POISON-2");
        send(source, bodies);

        BindingConfig config = config(source, 10, true);
        ReportingWriter writer = ReportingWriter.honest();
        CapturingEmitter emitter = new CapturingEmitter();
        BindingMetrics metrics = new BindingMetrics("rms");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, new BodyPoisonScreen(boq), null, null,
                emitter, metrics, "abc-test", RECEIVE_TIMEOUT_MS);
        executor.submit(loop);
        awaitTrue(5_000, () -> loop.getCommitCount() >= 1);
        loop.stop();

        assertThat(loop.getCommitCount()).isEqualTo(1);
        assertThat(metrics.getBalanceCheckFailures()).isZero();
        assertThat(countOnQueue(boq)).isEqualTo(2);
        assertThat(countOnQueue(source)).isZero();

        AuditRecord audit = emitter.records.get(0);
        assertThat(audit.getConsumedCount()).isEqualTo(10);
        assertThat(audit.getRecordCount()).isEqualTo(8);
        assertThat(audit.getBackoutCount()).isEqualTo(2);
        assertThat(audit.getBalanceDelta()).isZero();
        assertThat(audit.isBalanced()).isTrue();
    }

    // --- Test 3: unaccounted message must never commit ---

    @Test
    void unaccountedMessageRollsBackInsteadOfCommitting() throws Exception {
        String source = "ABC.UNBALANCED.SOURCE";
        send(source, bodies("CLEAN", 10));

        BindingConfig config = config(source, 10, true);
        // The writer observed itself append only 9 of the 10 it was handed —
        // one message consumed, neither landed nor routed.
        ReportingWriter writer = ReportingWriter.shortBy(1);
        CapturingEmitter emitter = new CapturingEmitter();
        BindingMetrics metrics = new BindingMetrics("rms");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, null, null, null, emitter, metrics,
                "abc-test", RECEIVE_TIMEOUT_MS);
        executor.submit(loop);
        awaitTrue(5_000, () -> loop.getRollbackCount() >= 1);
        loop.stop();

        assertThat(loop.getCommitCount())
                .as("an unbalanced batch must never be committed").isZero();
        assertThat(loop.getRollbackCount()).isGreaterThanOrEqualTo(1);
        assertThat(metrics.getBalanceCheckFailures()).isGreaterThanOrEqualTo(1);
        assertThat(emitter.records)
                .as("no audit record for a batch that did not commit").isEmpty();
        assertThat(countOnQueue(source))
                .as("rolled-back messages stay on the queue for redelivery").isEqualTo(10);
    }

    // --- Test 4: persisted consumed_count is the true MQ count, not derived ---

    @Test
    void auditConsumedCountComesFromTheBatchNotFromWrittenPlusBackout() throws Exception {
        String source = "ABC.INDEPENDENT.SOURCE";
        send(source, bodies("CLEAN", 10));

        // Check DISABLED (the Claims posture, and the proof of independence):
        // the writer reports 7 of 10, the batch still commits, and the audit
        // must say consumed=10 with delta=3 — a derived consumed_count would
        // have said 7 and balanced perfectly.
        BindingConfig config = config(source, 10, false);
        ReportingWriter writer = ReportingWriter.shortBy(3);
        CapturingEmitter emitter = new CapturingEmitter();

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, null, null, null, emitter, null,
                "abc-test", RECEIVE_TIMEOUT_MS);
        executor.submit(loop);
        awaitTrue(5_000, () -> loop.getCommitCount() >= 1);
        loop.stop();

        assertThat(loop.getCommitCount())
                .as("with the check disabled, existing commit behaviour is preserved")
                .isEqualTo(1);

        AuditRecord audit = emitter.records.get(0);
        assertThat(audit.getConsumedCount())
                .as("consumed_count must be the MQ batch size").isEqualTo(10);
        assertThat(audit.getRecordCount()).isEqualTo(7);
        assertThat(audit.getBalanceDelta()).isEqualTo(3);
        assertThat(audit.isBalanced()).isFalse();
    }

    // --- The backout-only branch is checked too ---

    @Test
    void allPoisonBatchBalancesThroughTheBackoutOnlyPath() throws Exception {
        String source = "ABC.ALLPOISON.SOURCE";
        String boq = "ABC.ALLPOISON.BOQ";
        send(source, List.of("POISON-1", "POISON-2"));

        BindingConfig config = config(source, 2, true);
        CapturingEmitter emitter = new CapturingEmitter();
        BindingMetrics metrics = new BindingMetrics("rms");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, ReportingWriter.honest(), null,
                new BodyPoisonScreen(boq), null, null, emitter, metrics,
                "abc-test", RECEIVE_TIMEOUT_MS);
        executor.submit(loop);
        awaitTrue(5_000, () -> loop.getCommitCount() >= 1);
        loop.stop();

        assertThat(metrics.getBalanceCheckFailures()).isZero();
        assertThat(countOnQueue(boq)).isEqualTo(2);

        AuditRecord audit = emitter.records.get(0);
        assertThat(audit.getConsumedCount()).isEqualTo(2);
        assertThat(audit.getRecordCount()).isZero();
        assertThat(audit.getBackoutCount()).isEqualTo(2);
        assertThat(audit.isBalanced()).isTrue();
    }

    // --- harness ---

    private BindingConfig config(String sourceQueue, int batchSize, boolean balanceCheck) {
        BindingConfig config = new BindingConfig();
        config.setId("rms");
        config.setMode(BindingMode.LAND_ONLY);
        config.setSourceQueue(sourceQueue);
        config.getBatch().setSize(batchSize);
        config.getBatch().setBytes(64 * 1024 * 1024);
        config.getBatch().setIntervalMs(0);
        config.getAudit().setBalanceCheckEnabled(balanceCheck);
        return config;
    }

    private List<String> bodies(String prefix, int count) {
        List<String> bodies = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            bodies.add(prefix + "-" + i);
        }
        return bodies;
    }

    private void send(String queue, List<String> bodies) throws Exception {
        MessageProducer producer =
                producerSession.createProducer(producerSession.createQueue(queue));
        for (String body : bodies) {
            producer.send(producerSession.createTextMessage(body));
        }
        producer.close();
    }

    private int countOnQueue(String queueName) throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            QueueBrowser browser = s.createBrowser(s.createQueue(queueName));
            int count = 0;
            Enumeration<?> e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                e.nextElement();
                count++;
            }
            return count;
        }
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

    /**
     * A writer whose REPORTED count is decoupled from the input size — the
     * stand-in for a writer that observed fewer successful appends than
     * messages handed to it. (The real writer's count comes from its
     * per-append index entries; here the shortfall is injected.)
     */
    private static final class ReportingWriter implements BatchWriter {
        private final int shortfall;

        private ReportingWriter(int shortfall) {
            this.shortfall = shortfall;
        }

        static ReportingWriter honest() {
            return new ReportingWriter(0);
        }

        static ReportingWriter shortBy(int shortfall) {
            return new ReportingWriter(shortfall);
        }

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages) {
            int observed = messages.size() - shortfall;
            return new BatchWriteResult("/fake/year=2026/rms_abc_1.seq", observed, 1_000);
        }
    }

    /**
     * Routes messages whose body starts with POISON to the backout queue on
     * the SAME transacted session — true to the real handler's semantics, so
     * the BOQ puts commit and roll back with the unit of work.
     */
    private static final class BodyPoisonScreen implements PoisonScreen {
        private final String backoutQueue;

        BodyPoisonScreen(String backoutQueue) {
            this.backoutQueue = backoutQueue;
        }

        @Override
        public PoisonMessageHandler.BatchPoisonCheckResult screen(
                Session session, List<Message> messages)
                throws PoisonMessageHandler.BackoutFailureException {
            List<Message> clean = new ArrayList<>();
            List<PoisonMessageHandler.BackoutResult> routed = new ArrayList<>();
            try {
                for (Message message : messages) {
                    String body = ((TextMessage) message).getText();
                    if (body.startsWith("POISON")) {
                        MessageProducer producer =
                                session.createProducer(session.createQueue(backoutQueue));
                        producer.send(message);
                        producer.close();
                        routed.add(PoisonMessageHandler.BackoutResult.success(
                                message.getJMSMessageID(), backoutQueue, 1));
                    } else {
                        clean.add(message);
                    }
                }
            } catch (JMSException e) {
                throw new PoisonMessageHandler.BackoutFailureException(
                        "test routing failed: " + e.getMessage(), e);
            }
            return new PoisonMessageHandler.BatchPoisonCheckResult(clean, routed);
        }
    }

    /** Captures every audit record instead of writing it anywhere. */
    private static final class CapturingEmitter implements AuditRecordEmitter {
        final List<AuditRecord> records = new CopyOnWriteArrayList<>();

        @Override
        public void emit(AuditRecord record) {
            records.add(record);
        }

        @Override
        public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                         List<Message> messages) {
            emit(bindingId, writeResult, messages, 0);
        }

        @Override
        public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                         List<Message> messages, int backoutCount) {
            emit(bindingId, writeResult, messages, backoutCount, -1);
        }

        @Override
        public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                         List<Message> messages, int backoutCount, int consumedCount) {
            AuditRecord.Builder builder = AuditRecord.builder()
                    .bindingId(bindingId)
                    .partitionPath("/fake/year=2026")
                    .filename("rms_abc_1.seq")
                    .recordCount(writeResult.getRecordCount())
                    .byteCount(writeResult.getByteCount())
                    .backoutCount(backoutCount)
                    .instanceId("abc-test")
                    .commitTimestamp(java.time.Instant.EPOCH);
            if (consumedCount >= 0) {
                builder.consumedCount(consumedCount);
            }
            records.add(builder.build());
        }

        @Override
        public void emitBackoutOnly(String bindingId, List<Message> messages, int backoutCount) {
            emitBackoutOnly(bindingId, messages, backoutCount, -1);
        }

        @Override
        public void emitBackoutOnly(String bindingId, List<Message> messages, int backoutCount,
                                    int consumedCount) {
            AuditRecord.Builder builder = AuditRecord.builder()
                    .bindingId(bindingId)
                    .partitionPath("")
                    .filename("backout-only-test")
                    .recordCount(0)
                    .byteCount(0)
                    .backoutCount(backoutCount)
                    .instanceId("abc-test")
                    .commitTimestamp(java.time.Instant.EPOCH);
            if (consumedCount >= 0) {
                builder.consumedCount(consumedCount);
            }
            records.add(builder.build());
        }
    }
}
