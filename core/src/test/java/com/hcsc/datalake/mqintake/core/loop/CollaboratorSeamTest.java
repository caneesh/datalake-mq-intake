package com.hcsc.datalake.mqintake.core.loop;

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
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The receive loop's collaborators are genuinely replaceable.
 *
 * <p>An interface that only ever has one implementation proves nothing — it
 * compiles whether or not the abstraction is real. These tests substitute
 * both {@link PoisonScreen} and {@link DegradationPolicy} with alternatives
 * and drive the actual loop, which is what shows the seam holds.
 */
class CollaboratorSeamTest {

    private static final String SOURCE_QUEUE = "SEAM.SOURCE";

    private Connection connection;
    private Session producerSession;
    private MessageProducer producer;
    private CountingBatchWriter batchWriter;
    private ExecutorService executor;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://seam?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = producerSession.createQueue(SOURCE_QUEUE);
        producer = producerSession.createProducer(queue);
        batchWriter = new CountingBatchWriter();
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        producer.close();
        producerSession.close();
        connection.close();
    }

    @Test
    void aDifferentPoisonScreenChangesWhatGetsLanded() throws Exception {
        // Screens out every message whose body starts with "SKIP" — nothing to
        // do with delivery counts, which is the point: the loop depends on the
        // behaviour, not on IBM MQ's mechanism.
        RecordingScreen screen = new RecordingScreen();

        send("KEEP-1", "SKIP-1", "KEEP-2");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config(3), connection, batchWriter, null, screen, null, null, null,
                null, "seam", 100);

        Future<?> future = executor.submit(loop);
        awaitQueueEmpty(3000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(screen.invocations.get()).isPositive();
        assertThat(batchWriter.getTotalMessageCount())
                .as("only the unscreened messages are landed")
                .isEqualTo(2);
    }

    @Test
    void aDifferentDegradationPolicyChangesTheEffectiveBatchSize() throws Exception {
        // Always reports a batch size of 1, so the loop commits one message at
        // a time even though the binding is configured for 10.
        FixedBatchSizePolicy policy = new FixedBatchSizePolicy(1);

        send("A", "B", "C");

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config(10), connection, batchWriter, null, null, policy, null, null,
                null, "seam", 100);

        Future<?> future = executor.submit(loop);
        awaitQueueEmpty(3000);
        loop.stop();
        future.get(2, TimeUnit.SECONDS);

        assertThat(batchWriter.getTotalMessageCount()).isEqualTo(3);
        assertThat(batchWriter.getBatchCount())
                .as("one batch per message, driven entirely by the substituted policy")
                .isEqualTo(3);
        assertThat(policy.successes.get()).isEqualTo(3);
    }

    @Test
    void theRealImplementationsStillSatisfyTheirInterfaces() {
        // Guards against an interface drifting away from its production
        // implementation.
        assertThat(new PoisonMessageHandler(5, "BOQ")).isInstanceOf(PoisonScreen.class);
        assertThat(new com.hcsc.datalake.mqintake.core.failure.DegradedModeManager(
                "b", 10, com.hcsc.datalake.mqintake.core.failure.DegradationStrategy.BATCH_OF_ONE, 3))
                .isInstanceOf(DegradationPolicy.class);
    }

    // --- substitutes ---

    /** Screens by payload content rather than delivery count. */
    private static class RecordingScreen implements PoisonScreen {
        final AtomicInteger invocations = new AtomicInteger();

        @Override
        public PoisonMessageHandler.BatchPoisonCheckResult screen(
                Session session, List<Message> messages) {
            invocations.incrementAndGet();
            List<Message> clean = new ArrayList<>();
            List<PoisonMessageHandler.BackoutResult> screened = new ArrayList<>();
            for (Message message : messages) {
                if (bodyOf(message).startsWith("SKIP")) {
                    screened.add(PoisonMessageHandler.BackoutResult.success(
                            "screened", "TEST.BOQ", 1));
                } else {
                    clean.add(message);
                }
            }
            return new PoisonMessageHandler.BatchPoisonCheckResult(clean, screened);
        }

        private String bodyOf(Message message) {
            try {
                return ((javax.jms.TextMessage) message).getText();
            } catch (Exception e) {
                return "";
            }
        }
    }

    /** Reports a constant batch size, ignoring failures entirely. */
    private static class FixedBatchSizePolicy implements DegradationPolicy {
        private final int batchSize;
        final AtomicInteger successes = new AtomicInteger();
        final List<String> cleared = new CopyOnWriteArrayList<>();

        FixedBatchSizePolicy(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public int getCurrentBatchSize() {
            return batchSize;
        }

        @Override
        public boolean isInDegradedMode() {
            return false;
        }

        @Override
        public void recordSuccess() {
            successes.incrementAndGet();
        }

        @Override
        public FailureClass recordFailure(Throwable throwable, Collection<String> ids) {
            return FailureClass.UNKNOWN;
        }

        @Override
        public void clearSuspects(Collection<String> messageIds) {
            cleared.addAll(messageIds);
        }
    }

    // --- helpers ---

    private BindingConfig config(int batchSize) {
        BindingConfig config = new BindingConfig();
        config.setId("seam");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath("/tmp/seam");
        config.setBatchSize(batchSize);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(200);
        config.setListenerThreads(1);
        return config;
    }

    private void send(String... bodies) throws Exception {
        for (String body : bodies) {
            producer.send(producerSession.createTextMessage(body));
        }
    }

    private void awaitQueueEmpty(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                javax.jms.QueueBrowser browser = s.createBrowser(s.createQueue(SOURCE_QUEUE));
                if (!browser.getEnumeration().hasMoreElements()) {
                    Thread.sleep(300);   // let the final commit land
                    return;
                }
            }
            Thread.sleep(50);
        }
    }
}
