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
 * The key is the record's byte offset and the payload is whitespace-normalised
 * as the MDB does, so this is the production contract, not a placeholder.
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
                .recordOffset(5)          // batch index — traceability only
                .fileByteOffset(129L)     // byte position — this is the key
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        assertThat(record.getKey()).isInstanceOf(LongWritable.class);
        assertThat(record.getValue()).isInstanceOf(Text.class);

        // The key is the byte offset, not the batch index — 129, not 5
        assertThat(((LongWritable) record.getKey()).get()).isEqualTo(129L);
        assertThat(record.getValue().toString()).isEqualTo(payload);
    }

    @Test
    void keyTracksFileByteOffsetNotBatchIndex() throws Exception {
        TextMessage message = session.createTextMessage("<Test><MessageID>g</MessageID></Test>");

        // Byte offsets grow by record size, so they are neither sequential nor
        // equal to the batch index. Pinning both here catches a regression that
        // swapped one for the other.
        long[] byteOffsets = {129L, 174L, 220L};

        for (int i = 0; i < byteOffsets.length; i++) {
            RecordMetadata metadata = RecordMetadata.builder()
                    .bindingId("rms").sourceFile("test.seq")
                    .recordOffset(i)
                    .fileByteOffset(byteOffsets[i])
                    .build();

            RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);
            assertThat(((LongWritable) record.getKey()).get()).isEqualTo(byteOffsets[i]);
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

    @Test
    void valueIsWhitespaceNormalisedLikeTheMdb() throws Exception {
        // Multi-line payload: the MDB's processMessage replaces each \n, \r
        // and \t with a single space before writing. Without this the file
        // bytes diverge from what consumers have always received.
        String payload = "<MemberEvent>\n\t<MessageID>guid-1</MessageID>\r\n</MemberEvent>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("rms").sourceFile("test.seq").recordOffset(0).build();

        String stored = serializer.serialize(message, metadata).getValue().toString();

        assertThat(stored).doesNotContain("\n").doesNotContain("\r").doesNotContain("\t");
        // One space per character, runs not collapsed: \n\t -> two spaces,
        // \r\n -> two spaces
        assertThat(stored).isEqualTo("<MemberEvent>  <MessageID>guid-1</MessageID>  </MemberEvent>");
    }

    @Test
    void aPayloadWithoutMessageIdIsCountedNotSilent() throws Exception {
        // The review finding: identity extraction failed with zero signal,
        // and one miss poisons the whole file's index for reconciliation.
        TextMessage message = session.createTextMessage("<Member><Name>anon</Name></Member>");

        RecordSerializer.SerializedRecord record =
                serializer.serialize(message, metadataAtOffset(129));

        assertThat(record.getIdentity()).isNull();
        assertThat(serializer.getIdentityMissCount()).isEqualTo(1);

        serializer.serialize(message, metadataAtOffset(200));
        assertThat(serializer.getIdentityMissCount()).isEqualTo(2);
    }

    @Test
    void aPresentButBlankMessageIdIsAMissNotAnEmptyIdentity() throws Exception {
        // Regression: <MessageID>   </MessageID> used to come back as "" after
        // trim, which bypassed the null-gated miss counter and warning — the
        // operator alarm never fired while the file's index was still refused.
        TextMessage message = session.createTextMessage(
                "<Member><MessageID>   </MessageID></Member>");

        RecordSerializer.SerializedRecord record =
                serializer.serialize(message, metadataAtOffset(129));

        assertThat(record.getIdentity()).isNull();
        assertThat(record.hasIdentity()).isFalse();
        assertThat(serializer.getIdentityMissCount()).isEqualTo(1);
    }

    @Test
    void aBlankEscapedMessageIdIsAlsoAMiss() throws Exception {
        TextMessage message = session.createTextMessage(
                "&lt;Member&gt;&lt;MessageID&gt; &lt;/MessageID&gt;&lt;/Member&gt;");

        RecordSerializer.SerializedRecord record =
                serializer.serialize(message, metadataAtOffset(129));

        assertThat(record.getIdentity()).isNull();
        assertThat(serializer.getIdentityMissCount()).isEqualTo(1);
    }

    @Test
    void aPayloadWithMessageIdDoesNotCountAsAMiss() throws Exception {
        TextMessage message = session.createTextMessage(
                "<Member><MessageID>guid-1</MessageID></Member>");

        RecordSerializer.SerializedRecord record =
                serializer.serialize(message, metadataAtOffset(129));

        assertThat(record.getIdentity()).isEqualTo("guid-1");
        assertThat(serializer.getIdentityMissCount()).isZero();
    }

    private RecordMetadata metadataAtOffset(long offset) {
        return RecordMetadata.builder()
                .bindingId("rms")
                .sourceFile("f.seq")
                .recordOffset(0)
                .fileByteOffset(offset)
                .consumeTimestamp(java.time.Instant.now())
                .build();
    }

}
