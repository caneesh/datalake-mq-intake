package com.hcsc.datalake.mqintake.rms.integration;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.mq.CredentialProvider;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.jms.*;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

/**
 * Integration tests against real IBM MQ in Docker.
 *
 * <p>Run these tests with:
 * <pre>
 * # Start MQ first
 * docker-compose up -d ibm-mq
 *
 * # Set credentials
 * export MQ_USER=app
 * export MQ_PASSWORD=passw0rd
 *
 * # Run tests
 * mvn test -pl rms -Dtest=IbmMqIntegrationTest
 * </pre>
 *
 * <p>These tests are skipped if MQ_USER env var is not set.
 */
@EnabledIfEnvironmentVariable(named = "MQ_USER", matches = ".+")
@DisplayName("IBM MQ Docker Integration Tests")
class IbmMqIntegrationTest {

    private static final String HOST = "localhost";
    private static final int PORT = 1414;
    private static final String QUEUE_MANAGER = "QM1";
    private static final String CHANNEL = "DEV.APP.SVRCONN";
    private static final String TEST_QUEUE = "MQ.HPS.MEMBERSHIP.IN";

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        String user = System.getenv("MQ_USER");
        String password = System.getenv("MQ_PASSWORD");

        assumeTrue(user != null && !user.isBlank(), "MQ_USER not set - skipping");
        assumeTrue(password != null && !password.isBlank(), "MQ_PASSWORD not set - skipping");

        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setHostName(HOST);
        factory.setPort(PORT);
        factory.setQueueManager(QUEUE_MANAGER);
        factory.setChannel(CHANNEL);
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);

        try {
            connection = factory.createConnection(user, password);
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
        } catch (JMSException e) {
            assumeTrue(false, "Cannot connect to MQ - is Docker running? " + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        try {
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            // ignore
        }
    }

    @Test
    @DisplayName("Connect to IBM MQ and verify queue manager")
    void connectToMq() throws Exception {
        assertThat(connection).isNotNull();
        assertThat(session).isNotNull();
    }

    @Test
    @DisplayName("Send and receive message in transaction")
    void sendAndReceiveTransacted() throws Exception {
        Queue queue = session.createQueue(TEST_QUEUE);

        // Clear queue first
        try (MessageConsumer cleaner = session.createConsumer(queue)) {
            while (cleaner.receive(100) != null) {
                // drain
            }
            session.commit();
        }

        // Send a message
        try (MessageProducer producer = session.createProducer(queue)) {
            TextMessage msg = session.createTextMessage(
                    "<TestMessage><MessageID>integration-test-1</MessageID></TestMessage>");
            producer.send(msg);
            session.commit();
        }

        // Receive the message
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            Message received = consumer.receive(5000);

            assertThat(received).isNotNull();
            assertThat(received).isInstanceOf(TextMessage.class);
            assertThat(((TextMessage) received).getText()).contains("integration-test-1");

            session.commit();
        }

        // Verify queue is empty
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            Message remaining = consumer.receive(500);
            assertThat(remaining).isNull();
        }
    }

    @Test
    @DisplayName("Rollback leaves message on queue")
    void rollbackLeavesMessage() throws Exception {
        Queue queue = session.createQueue(TEST_QUEUE);

        // Clear queue
        try (MessageConsumer cleaner = session.createConsumer(queue)) {
            while (cleaner.receive(100) != null) { }
            session.commit();
        }

        // Send a message
        try (MessageProducer producer = session.createProducer(queue)) {
            TextMessage msg = session.createTextMessage(
                    "<TestMessage><MessageID>rollback-test</MessageID></TestMessage>");
            producer.send(msg);
            session.commit();
        }

        // Receive then rollback
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            Message received = consumer.receive(5000);
            assertThat(received).isNotNull();
            session.rollback(); // Don't commit - message should remain
        }

        // Message should still be on queue
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            Message stillThere = consumer.receive(5000);
            assertThat(stillThere).isNotNull();
            assertThat(((TextMessage) stillThere).getText()).contains("rollback-test");
            session.commit();
        }
    }

    @Test
    @DisplayName("MqConnectionManager connects to real MQ")
    void mqConnectionManagerWorks() throws Exception {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId("test-conn");
        config.setHost(HOST);
        config.setPort(PORT);
        config.setQueueManager(QUEUE_MANAGER);
        config.setChannel(CHANNEL);
        config.setCredentialRef("env:MQ_USER,MQ_PASSWORD");

        CredentialProvider credProvider = ref -> {
            if (ref.equals("env:MQ_USER,MQ_PASSWORD")) {
                return Optional.of(new CredentialProvider.Credentials(
                        System.getenv("MQ_USER"),
                        System.getenv("MQ_PASSWORD")));
            }
            return Optional.empty();
        };

        MqConnectionManager manager = new MqConnectionManager(
                Map.of("test-conn", config), credProvider);

        try {
            Connection conn = manager.getConnection("test-conn");
            assertThat(conn).isNotNull();

            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertThat(sess).isNotNull();
            sess.close();
        } finally {
            manager.close();
        }
    }

    @Test
    @DisplayName("Batch receive and commit as single unit")
    void batchReceiveAndCommit() throws Exception {
        Queue queue = session.createQueue(TEST_QUEUE);
        int batchSize = 5;

        // Clear and send batch
        try (MessageConsumer cleaner = session.createConsumer(queue)) {
            while (cleaner.receive(100) != null) { }
            session.commit();
        }

        try (MessageProducer producer = session.createProducer(queue)) {
            for (int i = 0; i < batchSize; i++) {
                TextMessage msg = session.createTextMessage(
                        "<TestMessage><Index>" + i + "</Index></TestMessage>");
                producer.send(msg);
            }
            session.commit();
        }

        // Receive batch in single transaction
        int received = 0;
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            Message msg;
            while ((msg = consumer.receive(1000)) != null && received < batchSize) {
                received++;
            }
            // Single commit for entire batch
            session.commit();
        }

        assertThat(received).isEqualTo(batchSize);

        // Verify queue empty
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            assertThat(consumer.receive(500)).isNull();
        }
    }
}
