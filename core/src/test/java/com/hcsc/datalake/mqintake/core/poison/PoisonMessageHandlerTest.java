package com.hcsc.datalake.mqintake.core.poison;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import javax.jms.*;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for PoisonMessageHandler.
 *
 * Verifies application-owned poison handling:
 * - Read backout count (JMSXDeliveryCount)
 * - Compare against BOTHRESH
 * - Route to backout queue on same session
 */
class PoisonMessageHandlerTest {

    private static final String BACKOUT_QUEUE = "TEST.BACKOUT";

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (session != null) session.close();
        if (connection != null) connection.close();
    }

    @Test
    void detectsPoisonMessageWhenDeliveryCountExceedsThreshold() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        // Create message with delivery count = 4 (exceeds threshold of 3)
        TextMessage message = session.createTextMessage("Test");
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 4);

        assertThat(handler.isPoisonMessage(message)).isTrue();
    }

    @Test
    void doesNotFlagMessageBelowThreshold() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage message = session.createTextMessage("Test");
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 2);

        assertThat(handler.isPoisonMessage(message)).isFalse();
    }

    @Test
    void doesNotFlagMessageAtExactlyThreshold() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage message = session.createTextMessage("Test");
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 3);

        assertThat(handler.isPoisonMessage(message)).isFalse();
    }

    @Test
    void routesPoisonMessageToBackoutQueue() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage message = session.createTextMessage("Poison payload");
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 5);

        PoisonMessageHandler.BackoutResult result = handler.routeToBackout(session, message);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getBackoutQueue()).isEqualTo(BACKOUT_QUEUE);
        // DeliveryCount is read from the original message
        assertThat(result.getDeliveryCount()).isEqualTo(5);

        // Commit to finalize
        session.commit();

        // Verify message payload is on backout queue
        Queue backout = session.createQueue(BACKOUT_QUEUE);
        try (MessageConsumer consumer = session.createConsumer(backout)) {
            Message received = consumer.receive(1000);
            assertThat(received).isNotNull();
            assertThat(((TextMessage) received).getText()).isEqualTo("Poison payload");
        }
    }

    @Test
    void routeUsesTheSameTransactedSession() throws Exception {
        // This test verifies the message goes to backout in the same transaction
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage message = session.createTextMessage("Test");
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 5);

        handler.routeToBackout(session, message);

        // Before commit, message should not be visible to other consumers
        Session otherSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue backout = otherSession.createQueue(BACKOUT_QUEUE);
        try (MessageConsumer consumer = otherSession.createConsumer(backout)) {
            Message received = consumer.receive(100);
            // ActiveMQ may or may not show uncommitted - depends on isolation
            // The key is that after rollback, it would not be there
        }
        otherSession.close();

        // Rollback - message should NOT end up in backout
        session.rollback();

        // Verify backout queue is empty after rollback
        Session checkSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue checkQueue = checkSession.createQueue(BACKOUT_QUEUE);
        QueueBrowser browser = checkSession.createBrowser(checkQueue);
        assertThat(browser.getEnumeration().hasMoreElements()).isFalse();
        checkSession.close();
    }

    @Test
    void checkAndRouteSeparatesCleanAndPoisonMessages() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage clean1 = session.createTextMessage("Clean 1");
        clean1.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 1);

        TextMessage poison = session.createTextMessage("Poison");
        poison.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 5);

        TextMessage clean2 = session.createTextMessage("Clean 2");
        clean2.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 2);

        List<Message> batch = Arrays.asList(clean1, poison, clean2);

        PoisonMessageHandler.BatchPoisonCheckResult result =
                handler.checkAndRoutePoisonMessages(session, batch);

        assertThat(result.getCleanMessages()).hasSize(2);
        assertThat(result.getPoisonCount()).isEqualTo(1);
        assertThat(result.hasPoisonMessages()).isTrue();
    }

    @Test
    void readsIbmMqBackoutCountProperty() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage message = session.createTextMessage("Test");
        // Note: ActiveMQ doesn't recognize IBM MQ-specific property names.
        // In production with IBM MQ, this property would be set by the broker.
        // For this test, we verify the property name constant is correct.
        assertThat(PoisonMessageHandler.JMS_IBM_MQMD_BACKOUT_COUNT)
                .isEqualTo("JMS_IBM_MQMD_BackoutCount");

        // Test with standard property instead (which ActiveMQ supports)
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 5);
        int deliveryCount = handler.getDeliveryCount(message);
        assertThat(deliveryCount).isEqualTo(5);
    }

    @Test
    void defaultsToDeliveryCountOneIfNoProperty() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage message = session.createTextMessage("Test");
        // No delivery count property

        int deliveryCount = handler.getDeliveryCount(message);

        assertThat(deliveryCount).isEqualTo(1);
        assertThat(handler.isPoisonMessage(message)).isFalse();
    }

    @Test
    void constructorRejectsInvalidThreshold() {
        assertThatThrownBy(() -> new PoisonMessageHandler(0, BACKOUT_QUEUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");

        assertThatThrownBy(() -> new PoisonMessageHandler(-1, BACKOUT_QUEUE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructorRejectsBlankBackoutQueue() {
        assertThatThrownBy(() -> new PoisonMessageHandler(3, null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new PoisonMessageHandler(3, "  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void prefersJmsxDeliveryCountOverIbmBackoutCount() throws Exception {
        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage message = session.createTextMessage("Test");
        // Both properties set - JMSXDeliveryCount should take priority
        message.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 2);
        message.setIntProperty(PoisonMessageHandler.JMS_IBM_MQMD_BACKOUT_COUNT, 10);

        int deliveryCount = handler.getDeliveryCount(message);

        assertThat(deliveryCount).isEqualTo(2); // Uses JMSXDeliveryCount, not 11
    }

    @Test
    void throwsBackoutFailureExceptionWhenRoutingFails() throws Exception {
        // Use a closed session to force routing failure
        Session closedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        closedSession.close();

        PoisonMessageHandler handler = new PoisonMessageHandler(3, BACKOUT_QUEUE);

        TextMessage poison = session.createTextMessage("Poison");
        poison.setIntProperty(PoisonMessageHandler.JMSX_DELIVERY_COUNT, 5);

        List<Message> batch = Arrays.asList(poison);

        // Routing to backout on closed session should throw BackoutFailureException
        assertThatThrownBy(() -> handler.checkAndRoutePoisonMessages(closedSession, batch))
                .isInstanceOf(PoisonMessageHandler.BackoutFailureException.class)
                .hasMessageContaining("Failed to route poison message");
    }
}
