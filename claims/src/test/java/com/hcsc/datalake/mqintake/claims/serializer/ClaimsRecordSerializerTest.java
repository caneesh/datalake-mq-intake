package com.hcsc.datalake.mqintake.claims.serializer;

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
 * Tests for ClaimsRecordSerializer.
 *
 * <p>WARNING: This tests a PLACEHOLDER serializer (§9.1). Output format
 * is non-contractual and will change when metadata placement decision
 * (open item #2) is finalized.
 *
 * <p>LAND_ONLY mode: Claims has no tracker queue. Messages are landed
 * to HDFS only.
 *
 * <p>OPEN ITEM #17: Identity field is not yet confirmed. The serializer
 * uses a pluggable ClaimsIdentityExtractor that defaults to trying
 * CLM_XMITSN_ID then REC_CTL_NBR.
 */
class ClaimsRecordSerializerTest {

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

    // --- Basic serialization with default extractor ---

    @Test
    void serializesTextMessageWithMetadata() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<ClaimEvent><CLM_XMITSN_ID>claim-id-12345</CLM_XMITSN_ID></ClaimEvent>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("claims")
                .mqMessageId("ID:claims-456")
                .mqPutTimestamp(Instant.parse("2026-08-22T14:30:00Z"))
                .consumeTimestamp(Instant.parse("2026-08-22T14:30:01Z"))
                .sourceFile("claims_inst1_789_1.seq")
                .recordOffset(3)
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        assertThat(record.getKey()).isInstanceOf(Text.class);
        assertThat(record.getValue()).isInstanceOf(BytesWritable.class);

        String key = record.getKey().toString();
        assertThat(key).contains("binding_id=claims");
        assertThat(key).contains("payload_guid=claim-id-12345");
        assertThat(key).contains("mq_message_id=ID:claims-456");
        assertThat(key).contains("source_file=claims_inst1_789_1.seq");
        assertThat(key).contains("record_offset=3");

        BytesWritable value = (BytesWritable) record.getValue();
        String valueStr = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
        assertThat(valueStr).isEqualTo(payload);
    }

    // --- Default extractor priority (CLM_XMITSN_ID then REC_CTL_NBR) ---

    @Test
    void defaultExtractorUsesClmXmitsnIdFirst() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim><CLM_XMITSN_ID>xmit-id</CLM_XMITSN_ID><REC_CTL_NBR>ctl-nbr</REC_CTL_NBR></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=xmit-id");
    }

    @Test
    void defaultExtractorFallsBackToRecCtlNbr() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim><REC_CTL_NBR>fallback-ctl-123</REC_CTL_NBR></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=fallback-ctl-123");
    }

    // --- Pluggable identity extractor ---

    @Test
    void usesCustomIdentityExtractor() throws Exception {
        ClaimsIdentityExtractor customExtractor = payload -> "custom-extracted-id";
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(customExtractor);

        String payload = "<Claim><ANYTHING>ignored</ANYTHING></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=custom-extracted-id");
    }

    @Test
    void customExtractorCanTargetSpecificTag() throws Exception {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_ADJSTMT_NBR");
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(extractor);

        String payload = "<Claim><CLM_ADJSTMT_NBR>adjustment-001</CLM_ADJSTMT_NBR></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=adjustment-001");
    }

    @Test
    void getIdentityExtractorReturnsConfiguredExtractor() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CUSTOM");
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(extractor);

        assertThat(serializer.getIdentityExtractor()).isSameAs(extractor);
    }

    // --- Handles escaped XML format ---

    @Test
    void extractsIdentityFromEscapedTags() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "&lt;Claim&gt;&lt;CLM_XMITSN_ID&gt;escaped-id-999&lt;/CLM_XMITSN_ID&gt;&lt;/Claim&gt;";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=escaped-id-999");
    }

    // --- Handles missing identity ---

    @Test
    void handlesPayloadWithoutIdentityField() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<ClaimEvent><OTHER_FIELD>data</OTHER_FIELD></ClaimEvent>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=");
        assertThat(key).contains("binding_id=claims");
    }

    // --- Error cases ---

    @Test
    void throwsOnNullMessageBody() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();
        TextMessage message = session.createTextMessage(null);

        RecordMetadata metadata = buildMinimalMetadata();

        assertThatThrownBy(() -> serializer.serialize(message, metadata))
                .isInstanceOf(RecordSerializer.SerializationException.class)
                .hasMessageContaining("null");
    }

    @Test
    void throwsOnNonTextMessage() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();
        BytesMessage message = session.createBytesMessage();
        message.writeBytes("test".getBytes());

        RecordMetadata metadata = buildMinimalMetadata();

        assertThatThrownBy(() -> serializer.serialize(message, metadata))
                .isInstanceOf(RecordSerializer.SerializationException.class)
                .hasMessageContaining("TextMessage");
    }

    @Test
    void throwsOnNullExtractor() {
        assertThatThrownBy(() -> new ClaimsRecordSerializer(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("identityExtractor");
    }

    // --- Key/value classes ---

    @Test
    void keyClassIsText() {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();
        assertThat(serializer.getKeyClass()).isEqualTo(Text.class);
    }

    @Test
    void valueClassIsBytesWritable() {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();
        assertThat(serializer.getValueClass()).isEqualTo(BytesWritable.class);
    }

    // --- Handles null metadata fields ---

    @Test
    void handlesNullMetadataFields() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim><CLM_XMITSN_ID>id-test</CLM_XMITSN_ID></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = RecordMetadata.builder()
                .bindingId("claims")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();

        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("binding_id=claims");
        assertThat(key).contains("mq_message_id=");
        assertThat(key).contains("mq_put_datetime=");
    }

    // --- Unicode preservation ---

    @Test
    void preservesUnicodeInPayload() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim><CLM_XMITSN_ID>unicode-test</CLM_XMITSN_ID><Name>日本語テスト</Name></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        BytesWritable value = (BytesWritable) record.getValue();
        String valueStr = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
        assertThat(valueStr).contains("日本語テスト");
    }

    // --- Claims schema divergence (§19.1) ---

    @Test
    void handlesClaimsTimestampFormat() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        // Claims timestamp format: 2025-05-21-12.45.00.835539
        String payload = "<Claim><CLM_XMITSN_ID>ts-test</CLM_XMITSN_ID>" +
                "<SRC_LST_UPD_TS>2025-05-21-12.45.00.835539</SRC_LST_UPD_TS></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        BytesWritable value = (BytesWritable) record.getValue();
        String valueStr = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
        assertThat(valueStr).contains("2025-05-21-12.45.00.835539");
    }

    @Test
    void handlesEventTypeField() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        // Claims uses EVENT_TYPE instead of ActionCode
        String payload = "<Claim><CLM_XMITSN_ID>event-test</CLM_XMITSN_ID>" +
                "<EVENT_TYPE>I</EVENT_TYPE></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        BytesWritable value = (BytesWritable) record.getValue();
        String valueStr = new String(value.getBytes(), 0, value.getLength(), StandardCharsets.UTF_8);
        assertThat(valueStr).contains("<EVENT_TYPE>I</EVENT_TYPE>");
    }

    @Test
    void handlesClmAdjstmtNbrOrdering() throws Exception {
        // CLM_ADJSTMT_NBR is source-monotonic revision counter per §19.1
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_ADJSTMT_NBR");
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(extractor);

        String payload = "<Claim><CLM_ADJSTMT_NBR>000042</CLM_ADJSTMT_NBR></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String key = record.getKey().toString();
        assertThat(key).contains("payload_guid=000042");
    }

    private RecordMetadata buildMinimalMetadata() {
        return RecordMetadata.builder()
                .bindingId("claims")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();
    }
}
