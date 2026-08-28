package com.hcsc.datalake.mqintake.core.loop;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for true claims batch bisection (round 2 prompt 3).
 *
 * <p>Algorithm under test (redistribution-safe suspect isolation):
 * <ol>
 *   <li>Data failure → whole failed batch marked suspect (by JMS message ID)
 *       in the binding-scoped DegradedModeManager; batch size halves (BISECT);
 *       JMS transaction rolls back</li>
 *   <li>MQ redelivers — possibly to a different listener; whichever listener
 *       commits a subset clears those IDs from the suspect set</li>
 *   <li>Subsets containing the poison keep failing and halving until the
 *       poison's backout count breaches BOTHRESH, at which point the
 *       PoisonMessageHandler routes it to the BOQ on the same transaction</li>
 *   <li>Only after the suspect set is empty can normal batch size restore</li>
 * </ol>
 *
 * <p>Transaction boundaries: every attempt is one transacted unit
 * (N gets [+ BOQ put] then commit, or rollback). No partial commit exists.
 */
class ClaimsBisectionIntegrationTest {

    private static final String SOURCE_QUEUE = "TEST.CLAIMS.SOURCE";
    private static final String BACKOUT_QUEUE = "TEST.CLAIMS.BOQ";
    private static final long RECEIVE_TIMEOUT_MS = 50;

    private Connection connection;
    private Session producerSession;
    private MessageProducer producer;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        // Let our own poison handling decide; don't let ActiveMQ DLQ messages first
        factory.getRedeliveryPolicy().setMaximumRedeliveries(-1);
        connection = factory.createConnection();
        connection.start();

        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = producerSession.createProducer(producerSession.createQueue(SOURCE_QUEUE));
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        try {
            producer.close();
            producerSession.close();
            connection.close();
        } catch (Exception ignored) {
        }
    }

    @Test
    void batchOf16WithOnePoisonIsolatesItWithoutOneByOneProcessing() throws Exception {
        // Given: 15 clean messages + 1 deterministic poison in a batch of 16
        for (int i = 0; i < 15; i++) {
            producer.send(producerSession.createTextMessage("CLEAN-" + i));
        }
        producer.send(producerSession.createTextMessage("POISON-A"));

        BindingConfig config = claimsConfig(16);
        PoisonSensitiveBatchWriter writer = new PoisonSensitiveBatchWriter();
        DegradedModeManager shared = new DegradedModeManager(
                "claims", 16, DegradationStrategy.BISECT, 2);
        // BOTHRESH must be >= ceil(log2(16)) + 1 = 5 so clean messages that
        // shared failing batches with the poison are never misrouted
        PoisonMessageHandler poisonHandler = new PoisonMessageHandler(5, BACKOUT_QUEUE);

        // Two listener loops sharing the binding-scoped manager — redelivery
        // after rollback may land on either thread
        TransactedReceiveLoop loop1 = newLoop(config, writer, shared, poisonHandler);
        TransactedReceiveLoop loop2 = newLoop(config, writer, shared, poisonHandler);

        executor.submit(loop1);
        executor.submit(loop2);

        // When: run until the poison is on the BOQ and all clean messages landed
        awaitCondition(30_000, () ->
                writer.writtenBodies.size() == 15 && countOnQueue(BACKOUT_QUEUE) == 1);

        loop1.stop();
        loop2.stop();

        long commits = loop1.getCommitCount() + loop2.getCommitCount();
        long rollbacks = loop1.getRollbackCount() + loop2.getRollbackCount();

        // Then: zero loss — every clean message landed exactly (bodies distinct)
        assertThat(writer.writtenBodies)
                .hasSize(15)
                .allSatisfy(body -> assertThat(body).startsWith("CLEAN-"));

        // Only the actual poison reached the BOQ
        List<String> boqBodies = drainQueue(BACKOUT_QUEUE);
        assertThat(boqBodies).containsExactly("POISON-A");

        // Source queue fully drained
        assertThat(countOnQueue(SOURCE_QUEUE)).isZero();

        // Fewer than 16 one-by-one transactions: good subsets committed in
        // batches, poison isolated in O(log N) failing attempts
        assertThat(commits + rollbacks)
                .as("total transactions (commits=%d, rollbacks=%d)", commits, rollbacks)
                .isLessThan(16);
        assertThat(rollbacks).isLessThanOrEqualTo(7);

        // All suspects resolved
        assertThat(shared.getSuspectCount()).isZero();
    }

    @Test
    void multiplePoisonMessagesAreAllIsolatedSafely() throws Exception {
        for (int i = 0; i < 14; i++) {
            producer.send(producerSession.createTextMessage("CLEAN-" + i));
        }
        producer.send(producerSession.createTextMessage("POISON-A"));
        producer.send(producerSession.createTextMessage("POISON-B"));

        BindingConfig config = claimsConfig(16);
        PoisonSensitiveBatchWriter writer = new PoisonSensitiveBatchWriter();
        DegradedModeManager shared = new DegradedModeManager(
                "claims", 16, DegradationStrategy.BISECT, 2);
        PoisonMessageHandler poisonHandler = new PoisonMessageHandler(5, BACKOUT_QUEUE);

        TransactedReceiveLoop loop = newLoop(config, writer, shared, poisonHandler);
        executor.submit(loop);

        awaitCondition(30_000, () ->
                writer.writtenBodies.size() == 14 && countOnQueue(BACKOUT_QUEUE) == 2);

        loop.stop();

        assertThat(writer.writtenBodies).hasSize(14);
        assertThat(drainQueue(BACKOUT_QUEUE))
                .containsExactlyInAnyOrder("POISON-A", "POISON-B");
        assertThat(countOnQueue(SOURCE_QUEUE)).isZero();
        assertThat(shared.getSuspectCount()).isZero();
    }

    @Test
    void anAllPoisonBatchAdvancesTheSharedCommitAndConsumptionMetrics() throws Exception {
        // The backout-only commit path once updated only the loop's internal
        // counter, so dashboards undercounted commits and consumption exactly
        // while poison was churning. The fix's own comment records that — but
        // until now no test asserted it: the path was driven end-to-end with
        // null metrics, so the regression could return with zero failures.
        producer.send(producerSession.createTextMessage("POISON-ONLY"));

        BindingConfig config = claimsConfig(1);
        // Threshold semantics mirror MQ BOTHRESH: poison once deliveryCount
        // EXCEEDS the threshold. With 1, the first delivery fails in the
        // writer and rolls back; the redelivery (count 2) screens to the BOQ.
        config.getBackout().setThreshold(1);
        DegradedModeManager shared = new DegradedModeManager(
                "claims", 1, DegradationStrategy.BISECT, 2);
        PoisonMessageHandler poisonHandler = new PoisonMessageHandler(1, BACKOUT_QUEUE);
        com.hcsc.datalake.mqintake.core.metrics.BindingMetrics metrics =
                new com.hcsc.datalake.mqintake.core.metrics.BindingMetrics("claims");
        PoisonSensitiveBatchWriter writer = new PoisonSensitiveBatchWriter();

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null,
                poisonHandler, shared, null, null, metrics,
                "test-instance", RECEIVE_TIMEOUT_MS);
        executor.submit(loop);

        awaitCondition(10_000, () -> metrics.getCommitCount() >= 1);
        loop.stop();

        // The committed unit of work is backout-only: consumed 1, committed 1,
        // routed 1, landed 0. The failed first delivery shows as exactly one
        // rollback — and rolled-back consumption is deliberately NOT counted,
        // so consumed stays 1 despite two deliveries.
        assertThat(metrics.getCommitCount()).isEqualTo(1);
        assertThat(metrics.getMessagesConsumed()).isEqualTo(1);
        assertThat(metrics.getPoisonMessagesRouted()).isEqualTo(1);
        assertThat(metrics.getMessagesWritten()).isZero();
        assertThat(metrics.getRollbackCount()).isEqualTo(1);

        assertThat(drainQueue(BACKOUT_QUEUE)).containsExactly("POISON-ONLY");
        assertThat(countOnQueue(SOURCE_QUEUE)).isZero();
        assertThat(shared.getSuspectCount()).isZero();
        assertThat(writer.writtenBodies).isEmpty();
    }

    // --- Suspect-gated restore ---

    @Test
    void restoreIsBlockedWhileSuspectsOutstanding() {
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 16, DegradationStrategy.BISECT, 1);

        manager.recordFailure(new BatchWriter.BatchWriteException("malformed payload"));
        assertThat(manager.isInDegradedMode()).isTrue();

        manager.markBatchSuspect(List.of("ID:1", "ID:2"));

        // Enough successes, but suspects outstanding → no restore
        manager.recordSuccess();
        manager.recordSuccess();
        assertThat(manager.isInDegradedMode()).isTrue();

        // Clearing suspects allows the next success to restore
        manager.clearSuspects(List.of("ID:1", "ID:2"));
        manager.recordSuccess();
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(16);
    }

    @Test
    void suspectTrackingIsIdBased() {
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 16, DegradationStrategy.BISECT, 1);

        manager.markBatchSuspect(List.of("ID:a", "ID:b"));
        assertThat(manager.isSuspect("ID:a")).isTrue();
        assertThat(manager.isSuspect("ID:zzz")).isFalse();
        assertThat(manager.getSuspectCount()).isEqualTo(2);

        manager.clearSuspects(List.of("ID:a"));
        assertThat(manager.isSuspect("ID:a")).isFalse();
        assertThat(manager.getSuspectCount()).isEqualTo(1);
    }

    // --- helpers ---

    private BindingConfig claimsConfig(int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId("claims");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.getHdfs().setBasePath("/data/raw/claims");
        config.getBatch().setSize(batchSize);
        config.getBatch().setBytes(128 * 1024 * 1024);
        config.getBatch().setIntervalMs(400);
        config.setListenerThreads(1);
        config.getBackout().setQueue(BACKOUT_QUEUE);
        config.getBackout().setThreshold(5);
        config.getDegradation().setStrategy(DegradationStrategy.BISECT);
        return config;
    }

    private TransactedReceiveLoop newLoop(BindingConfig config,
                                          BatchWriter writer,
                                          DegradedModeManager shared,
                                          PoisonMessageHandler poisonHandler) {
        return new TransactedReceiveLoop(
                config, connection, writer, null,
                poisonHandler, shared, null, null, null,
                "test-instance", RECEIVE_TIMEOUT_MS);
    }

    private void awaitCondition(long timeoutMs, ConditionCheck check) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (check.satisfied()) {
                return;
            }
            Thread.sleep(100);
        }
    }

    @FunctionalInterface
    private interface ConditionCheck {
        boolean satisfied() throws Exception;
    }

    private int countOnQueue(String queueName) throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            QueueBrowser browser = s.createBrowser(s.createQueue(queueName));
            int count = 0;
            var e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                e.nextElement();
                count++;
            }
            return count;
        }
    }

    private List<String> drainQueue(String queueName) throws Exception {
        List<String> bodies = new ArrayList<>();
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageConsumer consumer = s.createConsumer(s.createQueue(queueName))) {
            Message m;
            while ((m = consumer.receive(300)) != null) {
                bodies.add(((TextMessage) m).getText());
            }
        }
        return bodies;
    }

    /**
     * Batch writer that fails the whole batch with a data-classified failure
     * when any message in it is poison, otherwise records the distinct bodies
     * it wrote. Mirrors a deterministic serialization failure.
     */
    private static class PoisonSensitiveBatchWriter implements BatchWriter {
        final Set<String> writtenBodies = ConcurrentHashMap.newKeySet();

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages)
                throws BatchWriteException {
            try {
                for (Message m : messages) {
                    if (((TextMessage) m).getText().startsWith("POISON")) {
                        throw new BatchWriteException("malformed payload in batch");
                    }
                }
                long bytes = 0;
                for (Message m : messages) {
                    String body = ((TextMessage) m).getText();
                    writtenBodies.add(body);
                    bytes += body.length();
                }
                return new BatchWriteResult("/test/claims.seq", messages.size(), bytes);
            } catch (JMSException e) {
                throw new BatchWriteException("cannot read message", e);
            }
        }
    }
}
