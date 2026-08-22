package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.*;

import javax.jms.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RmsRecordSerializer.
 *
 * <p>WARNING: This tests a PLACEHOLDER serializer (§9.1). Output format
 * is non-contractual and will change when metadata placement decision
 * (open item #2) is finalized.
 */
class RmsRecordSerializerTest {

    private Connection connection;
    private Session session;
    private RmsRecordSerializer serializer;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        serializer = new RmsRecordSerializer();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (session != null) session.close();
        if (connection != null) connection.close();
    }

    @Test
    void serializesTextMessageWithMetadata() throws Exception {
        String payload = "<MemberEvent><MessageID>f935a79a-e782-43b6-b874-test</MessageID></MemberEvent>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .mqMessageId("ID:test-123")
                .mqPutTimestamp(Instant.parse("2026-08-22T10:30:00Z"))
                .consumeTimestamp(Instant.parse("2026-08-22T10:30:01Z"))
                .sourceFile("rms_inst1_123_1.seq")
                .recordOffset(5)
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        assertThat(record.getKey()).isInstanceOf(Text.class);
        assertThat(record.getValue()).isInstanceOf(BytesWritable.class);

        // Key contains metadata
        String key = record.getKey().toString();
        assertThat(key).contains("binding_id=rms");
        assertThat(key).contains("payload_guid=f935a79a-e782-43b6-b874-test");
        assertThat(key).contains("mq_message_id=ID:test-123");
        assertThat(key).contains("source_file=rms_inst1_123_1.seq");
        assertThat(key).contains("record_offset=5");

        // Value is the raw payload
        BytesWritable value = (BytesWritable) record.getValue();
        String valueStr = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
        assertThat(valueStr).isEqualTo(payload);
    }

    @Test
    void extractsMessageIdFromRawTags() throws Exception {
        String payload = "<Root><MessageID>uuid-12345-abcde</MessageID></Root>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=uuid-12345-abcde");
    }

    @Test
    void extractsMessageIdFromEscapedTags() throws Exception {
        String payload = "&lt;Root&gt;&lt;MessageID&gt;escaped-uuid-67890&lt;/MessageID&gt;&lt;/Root&gt;";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=escaped-uuid-67890");
    }

    @Test
    void handlesPayloadWithoutMessageId() throws Exception {
        String payload = "<MemberEvent><Name>Test</Name></MemberEvent>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        // payload_guid should be empty but key should still be valid
        assertThat(key).contains("payload_guid=");
        assertThat(key).contains("binding_id=rms");
    }

    @Test
    void throwsOnNullMessageBody() throws Exception {
        TextMessage message = session.createTextMessage(null);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();

        assertThatThrownBy(() -> serializer.serialize(message, metadata))
                .isInstanceOf(RecordSerializer.SerializationException.class)
                .hasMessageContaining("null");
    }

    @Test
    void throwsOnNonTextMessage() throws Exception {
        BytesMessage message = session.createBytesMessage();
        message.writeBytes("test".getBytes());

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();

        assertThatThrownBy(() -> serializer.serialize(message, metadata))
                .isInstanceOf(RecordSerializer.SerializationException.class)
                .hasMessageContaining("TextMessage");
    }

    @Test
    void keyClassIsText() {
        assertThat(serializer.getKeyClass()).isEqualTo(Text.class);
    }

    @Test
    void valueClassIsBytesWritable() {
        assertThat(serializer.getValueClass()).isEqualTo(BytesWritable.class);
    }

    @Test
    void handlesNullMetadataFields() throws Exception {
        String payload = "<Test><MessageID>guid-test</MessageID></Test>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("test.seq")
                .recordOffset(0)
                // Other fields null
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("binding_id=rms");
        assertThat(key).contains("mq_message_id=");  // Empty but present
        assertThat(key).contains("mq_put_datetime=");
    }

    @Test
    void preservesUnicodeInPayload() throws Exception {
        String payload = "<Test><Name>Tëst Üñîcödé 日本語</Name><MessageID>unicode-guid</MessageID></Test>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        BytesWritable value = (BytesWritable) record.getValue();
        String valueStr = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
        assertThat(valueStr).contains("Tëst Üñîcödé 日本語");
    }
}
