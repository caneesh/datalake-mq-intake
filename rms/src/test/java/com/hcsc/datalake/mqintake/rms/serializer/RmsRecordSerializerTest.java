package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.*;

import javax.jms.*;
import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RmsRecordSerializer.
 *
 * <p>The layout under test is {@code LongWritable} key / {@code Text} value,
 * matching the production SequenceFile types established from the legacy MDB.
 * This remains a PLACEHOLDER serializer: the key's exact expression is
 * unconfirmed, the payload is not yet whitespace-normalised as the MDB does,
 * and record metadata has no home in this layout pending open item #2.
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

    // --- Production layout ---

    @Test
    void keyClassIsLongWritable() {
        // Production writes LongWritable keys. Declaring Text here would make
        // every file unreadable by a consumer opening it with the production
        // key class, even though the payload bytes would be identical.
        assertThat(serializer.getKeyClass()).isEqualTo(LongWritable.class);
    }

    @Test
    void valueClassIsText() {
        // Text and BytesWritable have different wire formats — Text writes a
        // VInt length prefix, BytesWritable a fixed 4-byte int — so this is a
        // compatibility requirement, not a cosmetic preference.
        assertThat(serializer.getValueClass()).isEqualTo(Text.class);
    }

    @Test
    void serializesToLongWritableKeyAndTextValue() throws Exception {
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

        assertThat(record.getKey()).isInstanceOf(LongWritable.class);
        assertThat(record.getValue()).isInstanceOf(Text.class);

        assertThat(((LongWritable) record.getKey()).get()).isEqualTo(5L);
        assertThat(record.getValue().toString()).isEqualTo(payload);
    }

    @Test
    void keyTracksRecordOffset() throws Exception {
        TextMessage message = session.createTextMessage("<Test><MessageID>g</MessageID></Test>");

        for (int offset = 0; offset < 3; offset++) {
            RecordMetadata metadata = RecordMetadata.builder()
                    .bindingId("rms").sourceFile("test.seq").recordOffset(offset).build();

            RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);
            assertThat(((LongWritable) record.getKey()).get()).isEqualTo(offset);
        }
    }

    @Test
    void valuePreservesPayloadExactlyIncludingUnicode() throws Exception {
        String payload = "<Test><Name>Tëst Üñîcödé 日本語</Name><MessageID>unicode-guid</MessageID></Test>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms").sourceFile("test.seq").recordOffset(0).build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        assertThat(record.getValue().toString()).isEqualTo(payload);
    }

    // --- Payload GUID extraction ---
    // Survives whichever metadata placement is chosen, so it is kept and
    // tested even though the value is not currently written to the file.

    @Test
    void extractsPayloadGuidFromRawTags() {
        assertThat(serializer.extractPayloadGuid(
                "<Root><MessageID>uuid-12345-abcde</MessageID></Root>"))
                .isEqualTo("uuid-12345-abcde");
    }

    @Test
    void extractsPayloadGuidFromEscapedTags() {
        // Some upstream senders deliver XML-escaped content (§20.3); handling
        // only one variant would silently stop identifying those messages.
        assertThat(serializer.extractPayloadGuid(
                "&lt;Root&gt;&lt;MessageID&gt;escaped-uuid-67890&lt;/MessageID&gt;&lt;/Root&gt;"))
                .isEqualTo("escaped-uuid-67890");
    }

    @Test
    void payloadGuidIsNullWhenAbsent() {
        assertThat(serializer.extractPayloadGuid("<MemberEvent><Name>Test</Name></MemberEvent>"))
                .isNull();
        assertThat(serializer.extractPayloadGuid(null)).isNull();
    }

    @Test
    void payloadWithoutMessageIdStillSerializes() throws Exception {
        // Metadata is not written today, so a missing GUID must not fail the
        // record. If a future placement makes the GUID contractual, this
        // becomes a validation point — as it already is for claims.
        TextMessage message = session.createTextMessage("<MemberEvent><Name>Test</Name></MemberEvent>");

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms").sourceFile("test.seq").recordOffset(0).build();

        assertThatCode(() -> serializer.serialize(message, metadata)).doesNotThrowAnyException();
    }

    // --- Failure modes ---

    @Test
    void throwsOnNullMessageBody() throws Exception {
        TextMessage message = session.createTextMessage(null);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms").sourceFile("test.seq").recordOffset(0).build();

        assertThatThrownBy(() -> serializer.serialize(message, metadata))
                .isInstanceOf(RecordSerializer.SerializationException.class)
                .hasMessageContaining("null");
    }

    @Test
    void throwsOnNonTextMessage() throws Exception {
        BytesMessage message = session.createBytesMessage();
        message.writeBytes("test".getBytes());

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms").sourceFile("test.seq").recordOffset(0).build();

        assertThatThrownBy(() -> serializer.serialize(message, metadata))
                .isInstanceOf(RecordSerializer.SerializationException.class)
                .hasMessageContaining("TextMessage");
    }
}
