package com.hcsc.datalake.mqintake.core.metrics;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the backout-queue depth monitor.
 *
 * <p>Uses embedded ActiveMQ so the browse is a real one — the point of the
 * class is that it reads a queue without consuming from it, which a mock
 * could not demonstrate.
 */
class BackoutQueueDepthMonitorTest {

    private static final String BACKOUT_QUEUE = "TEST.BACKOUT";

    private Connection connection;
    private Session producerSession;
    private MessageProducer producer;
    private BindingMetrics metrics;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://boqdepth?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();

        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = producerSession.createQueue(BACKOUT_QUEUE);
        producer = producerSession.createProducer(queue);

        metrics = new BindingMetrics("test");
    }

    @AfterEach
    void tearDown() throws Exception {
        drainQueue();
        if (producer != null) producer.close();
        if (producerSession != null) producerSession.close();
        if (connection != null) connection.close();
    }

    @Test
    void emptyBackoutQueueReportsZero() throws Exception {
        BackoutQueueDepthMonitor monitor = newMonitor(30_000);

        monitor.pollOnce();

        assertThat(metrics.getBackoutQueueDepth()).isZero();
        assertThat(monitor.isDepthAvailable()).isTrue();
        assertThat(monitor.getLastSuccessfulPollMs()).isPositive();
    }

    @Test
    void depthReflectsMessagesOnTheBackoutQueue() throws Exception {
        sendMessages(3);
        BackoutQueueDepthMonitor monitor = newMonitor(30_000);

        monitor.pollOnce();

        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(3);
    }

    @Test
    void samplingDoesNotConsumeTheMessagesItCounts() throws Exception {
        // The whole design rests on the browse being read-only. If sampling
        // consumed, monitoring the backout queue would destroy the very
        // messages an operator is being paged to look at.
        sendMessages(4);
        BackoutQueueDepthMonitor monitor = newMonitor(30_000);

        monitor.pollOnce();
        monitor.pollOnce();
        monitor.pollOnce();

        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(4);
        assertThat(countByBrowsing()).isEqualTo(4);
    }

    @Test
    void depthReturnsToZeroAfterTheQueueIsCleared() throws Exception {
        sendMessages(2);
        BackoutQueueDepthMonitor monitor = newMonitor(30_000);
        monitor.pollOnce();
        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(2);

        drainQueue();
        monitor.pollOnce();

        assertThat(metrics.getBackoutQueueDepth()).isZero();
    }

    @Test
    void countIsCappedSoADeepQueueCannotCostAnUnboundedEnumeration() throws Exception {
        sendMessages(10);
        BackoutQueueDepthMonitor monitor = new BackoutQueueDepthMonitor(
                "test", BACKOUT_QUEUE, connection, metrics, 30_000, 4);

        monitor.pollOnce();

        // Capped at 4, not 10 — still non-zero, which is all the alert needs
        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(4);
    }

    @Test
    void aFailedSampleLeavesTheGaugeAloneRatherThanZeroingIt() throws Exception {
        // Zeroing on error would suppress a page: the queue would look empty
        // precisely when we had lost the ability to see it.
        sendMessages(5);
        BackoutQueueDepthMonitor monitor = newMonitor(30_000);
        monitor.pollOnce();
        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(5);

        Connection broken = mock(Connection.class);
        when(broken.createSession(false, Session.AUTO_ACKNOWLEDGE))
                .thenThrow(new javax.jms.JMSException("connection broken"));
        BackoutQueueDepthMonitor failing = new BackoutQueueDepthMonitor(
                "test", BACKOUT_QUEUE, broken, metrics, 30_000);

        assertThatThrownBy(failing::pollOnce).isInstanceOf(javax.jms.JMSException.class);

        // The last good reading stands, so the alert keeps firing
        assertThat(metrics.getBackoutQueueDepth()).isEqualTo(5);
        assertThat(failing.isDepthAvailable()).isFalse();
    }

    @Test
    void scheduledMonitorPopulatesTheGaugeWithoutBeingDrivenByHand() throws Exception {
        sendMessages(2);
        BackoutQueueDepthMonitor monitor = newMonitor(50);

        monitor.start();
        try {
            long deadline = System.currentTimeMillis() + 5_000;
            while (metrics.getBackoutQueueDepth() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
            assertThat(metrics.getBackoutQueueDepth()).isEqualTo(2);
            assertThat(monitor.isRunning()).isTrue();
        } finally {
            monitor.stop();
        }
        assertThat(monitor.isRunning()).isFalse();
    }

    @Test
    void nonPositivePollIntervalDisablesTheMonitor() {
        BackoutQueueDepthMonitor monitor = newMonitor(0);

        monitor.start();

        assertThat(monitor.isRunning()).isFalse();
        assertThat(monitor.isDepthAvailable()).isFalse();
    }

    @Test
    void maxBrowseMustBeAtLeastOne() {
        assertThatThrownBy(() -> new BackoutQueueDepthMonitor(
                "test", BACKOUT_QUEUE, connection, metrics, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBrowse");
    }

    // --- helpers ---

    private BackoutQueueDepthMonitor newMonitor(long pollIntervalMs) {
        return new BackoutQueueDepthMonitor(
                "test", BACKOUT_QUEUE, connection, metrics, pollIntervalMs);
    }

    private void sendMessages(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            producer.send(producerSession.createTextMessage("poison-" + i));
        }
    }

    private int countByBrowsing() throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.QueueBrowser browser = s.createBrowser(s.createQueue(BACKOUT_QUEUE));
            int count = 0;
            var e = browser.getEnumeration();
            while (e.hasMoreElements()) {
                e.nextElement();
                count++;
            }
            return count;
        }
    }

    private void drainQueue() throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             javax.jms.MessageConsumer c = s.createConsumer(s.createQueue(BACKOUT_QUEUE))) {
            while (c.receive(200) != null) {
                // drain
            }
        }
    }
}
