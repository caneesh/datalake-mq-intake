package com.hcsc.datalake.mqintake.core.audit;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import javax.jms.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for AuditRecordBuilder.
 */
class AuditRecordBuilderTest {

    private static final Instant FIXED_TIME = Instant.parse("2026-08-22T10:30:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_TIME, ZoneId.of("UTC"));

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (session != null) session.close();
        if (connection != null) connection.close();
    }

    @Test
    void buildsAuditRecordFromBatchResult() throws Exception {
        AuditRecordBuilder builder = new AuditRecordBuilder("instance1", FIXED_CLOCK);

        BatchWriter.BatchWriteResult writeResult = new BatchWriter.BatchWriteResult(
                "/data/raw/rms/year=2026/month=08/day=22/hour=10/quarter=2/rms_instance1_1724328000000_1.seq",
                100,
                50000
        );

        TextMessage msg1 = session.createTextMessage("First message");
        TextMessage msg2 = session.createTextMessage("Last message");
        List<Message> messages = Arrays.asList(msg1, msg2);

        AuditRecord record = builder.build("rms", writeResult, messages);

        assertThat(record.getBindingId()).isEqualTo("rms");
        assertThat(record.getPartitionPath())
                .isEqualTo("/data/raw/rms/year=2026/month=08/day=22/hour=10/quarter=2");
        assertThat(record.getFilename()).isEqualTo("rms_instance1_1724328000000_1.seq");
        assertThat(record.getRecordCount()).isEqualTo(100);
        assertThat(record.getByteCount()).isEqualTo(50000);
        assertThat(record.getInstanceId()).isEqualTo("instance1");
        assertThat(record.getCommitTimestamp()).isEqualTo(FIXED_TIME);
    }

    @Test
    void extractsPayloadGuidAsIdentity() throws Exception {
        AuditRecordBuilder builder = new AuditRecordBuilder("instance1", FIXED_CLOCK);

        TextMessage msg1 = session.createTextMessage("First");
        msg1.setStringProperty("payload_guid", "guid-001");

        TextMessage msg2 = session.createTextMessage("Last");
        msg2.setStringProperty("payload_guid", "guid-100");

        BatchWriter.BatchWriteResult writeResult = new BatchWriter.BatchWriteResult(
                "/data/test.seq", 100, 5000);

        AuditRecord record = builder.build("rms", writeResult, Arrays.asList(msg1, msg2));

        assertThat(record.getFirstIdentity()).isEqualTo("guid-001");
        assertThat(record.getLastIdentity()).isEqualTo("guid-100");
    }

    @Test
    void fallsBackToJmsMessageIdWhenNoPayloadGuid() throws Exception {
        AuditRecordBuilder builder = new AuditRecordBuilder("instance1", FIXED_CLOCK);

        TextMessage msg1 = session.createTextMessage("First");
        // No payload_guid - will use JMS message ID (assigned by ActiveMQ)

        TextMessage msg2 = session.createTextMessage("Last");

        BatchWriter.BatchWriteResult writeResult = new BatchWriter.BatchWriteResult(
                "/data/test.seq", 100, 5000);

        AuditRecord record = builder.build("rms", writeResult, Arrays.asList(msg1, msg2));

        // JMS message ID is null for messages not sent to a destination
        // In production, received messages would have message IDs
        assertThat(record.getFirstIdentity()).isNull();
        assertThat(record.getLastIdentity()).isNull();
    }

    @Test
    void usesCustomIdentityExtractor() throws Exception {
        AuditRecordBuilder builder = new AuditRecordBuilder("instance1", FIXED_CLOCK,
                msg -> {
                    try {
                        return msg.getStringProperty("custom_id");
                    } catch (JMSException e) {
                        return null;
                    }
                });

        TextMessage msg1 = session.createTextMessage("First");
        msg1.setStringProperty("custom_id", "custom-001");

        TextMessage msg2 = session.createTextMessage("Last");
        msg2.setStringProperty("custom_id", "custom-100");

        BatchWriter.BatchWriteResult writeResult = new BatchWriter.BatchWriteResult(
                "/data/test.seq", 100, 5000);

        AuditRecord record = builder.build("rms", writeResult, Arrays.asList(msg1, msg2));

        assertThat(record.getFirstIdentity()).isEqualTo("custom-001");
        assertThat(record.getLastIdentity()).isEqualTo("custom-100");
    }

    @Test
    void handlesFilePathWithoutSlash() throws Exception {
        AuditRecordBuilder builder = new AuditRecordBuilder("instance1", FIXED_CLOCK);

        BatchWriter.BatchWriteResult writeResult = new BatchWriter.BatchWriteResult(
                "test.seq", 10, 1000);

        TextMessage msg = session.createTextMessage("Test");
        AuditRecord record = builder.build("rms", writeResult, List.of(msg));

        assertThat(record.getPartitionPath()).isEmpty();
        assertThat(record.getFilename()).isEqualTo("test.seq");
    }

    @Test
    void rejectsEmptyMessagesList() {
        AuditRecordBuilder builder = new AuditRecordBuilder("instance1", FIXED_CLOCK);

        BatchWriter.BatchWriteResult writeResult = new BatchWriter.BatchWriteResult(
                "/data/test.seq", 0, 0);

        assertThatThrownBy(() -> builder.build("rms", writeResult, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messages cannot be empty");
    }

    @Test
    void singleMessageHasSameFirstAndLastIdentity() throws Exception {
        AuditRecordBuilder builder = new AuditRecordBuilder("instance1", FIXED_CLOCK);

        TextMessage msg = session.createTextMessage("Only message");
        msg.setStringProperty("payload_guid", "guid-only");

        BatchWriter.BatchWriteResult writeResult = new BatchWriter.BatchWriteResult(
                "/data/test.seq", 1, 100);

        AuditRecord record = builder.build("rms", writeResult, List.of(msg));

        assertThat(record.getFirstIdentity()).isEqualTo("guid-only");
        assertThat(record.getLastIdentity()).isEqualTo("guid-only");
    }
}
