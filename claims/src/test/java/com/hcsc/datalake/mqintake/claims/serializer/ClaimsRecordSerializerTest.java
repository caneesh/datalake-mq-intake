package com.hcsc.datalake.mqintake.claims.serializer;

import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.io.LongWritable;
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

        // Production layout: positional LongWritable key, Text value
        assertThat(record.getKey()).isInstanceOf(LongWritable.class);
        assertThat(record.getValue()).isInstanceOf(Text.class);
        assertThat(((LongWritable) record.getKey()).get()).isEqualTo(3L);
        assertThat(record.getValue().toString()).isEqualTo(payload);

        // The metadata that used to ride in the key has no home in this layout.
        // Identity is still extracted — it drives the failOnMissingIdentity
        // check — but is not written to the file (open item #2).
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload))
                .isEqualTo("claim-id-12345");
    }

    // --- Default extractor priority (CLM_XMITSN_ID then REC_CTL_NBR) ---

    @Test
    void defaultExtractorUsesClmXmitsnIdFirst() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim><CLM_XMITSN_ID>xmit-id</CLM_XMITSN_ID><REC_CTL_NBR>ctl-nbr</REC_CTL_NBR></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        serializer.serialize(message, metadata); // must still serialize

        // Identity no longer rides in the key (production uses a LongWritable
        // positional key). The extraction strategy is still live — it drives
        // the failOnMissingIdentity check — so assert it directly.
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload))
                .isEqualTo("xmit-id");
    }

    @Test
    void defaultExtractorFallsBackToRecCtlNbr() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim><REC_CTL_NBR>fallback-ctl-123</REC_CTL_NBR></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        serializer.serialize(message, metadata); // must still serialize

        // Identity no longer rides in the key (production uses a LongWritable
        // positional key). The extraction strategy is still live — it drives
        // the failOnMissingIdentity check — so assert it directly.
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload))
                .isEqualTo("fallback-ctl-123");
    }

    // --- Pluggable identity extractor ---

    @Test
    void usesCustomIdentityExtractor() throws Exception {
        ClaimsIdentityExtractor customExtractor = payload -> "custom-extracted-id";
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(customExtractor);

        String payload = "<Claim><ANYTHING>ignored</ANYTHING></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        serializer.serialize(message, metadata); // must still serialize

        // Identity no longer rides in the key (production uses a LongWritable
        // positional key). The extraction strategy is still live — it drives
        // the failOnMissingIdentity check — so assert it directly.
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload))
                .isEqualTo("custom-extracted-id");
    }

    @Test
    void customExtractorCanTargetSpecificTag() throws Exception {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_ADJSTMT_NBR");
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(extractor);

        String payload = "<Claim><CLM_ADJSTMT_NBR>adjustment-001</CLM_ADJSTMT_NBR></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        serializer.serialize(message, metadata); // must still serialize

        // Identity no longer rides in the key (production uses a LongWritable
        // positional key). The extraction strategy is still live — it drives
        // the failOnMissingIdentity check — so assert it directly.
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload))
                .isEqualTo("adjustment-001");
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
        serializer.serialize(message, metadata); // must still serialize

        // Identity no longer rides in the key (production uses a LongWritable
        // positional key). The extraction strategy is still live — it drives
        // the failOnMissingIdentity check — so assert it directly.
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload))
                .isEqualTo("escaped-id-999");
    }

    // --- Handles missing identity ---

    @Test
    void handlesPayloadWithoutIdentityField() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<ClaimEvent><OTHER_FIELD>data</OTHER_FIELD></ClaimEvent>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        // No identity in the payload: tolerated here because this serializer
        // was built without failOnMissingIdentity.
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload)).isNull();
        assertThat(record.getValue().toString()).isEqualTo(payload);
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
    void keyClassIsLongWritable() {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();
        assertThat(serializer.getKeyClass()).isEqualTo(LongWritable.class);
    }

    @Test
    void valueClassIsText() {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();
        assertThat(serializer.getValueClass()).isEqualTo(Text.class);
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

        // Null metadata fields are simply absent from the record now that the
        // key is positional — serialization must still succeed.
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        assertThat(((LongWritable) record.getKey()).get()).isEqualTo(0L);
        assertThat(record.getValue().toString()).isEqualTo(payload);
    }

    // --- Unicode preservation ---

    @Test
    void preservesUnicodeInPayload() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim><CLM_XMITSN_ID>unicode-test</CLM_XMITSN_ID><Name>日本語テスト</Name></Claim>";
        TextMessage message = session.createTextMessage(payload);

        RecordMetadata metadata = buildMinimalMetadata();
        RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);

        String valueStr = record.getValue().toString();
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

        String valueStr = record.getValue().toString();
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

        String valueStr = record.getValue().toString();
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
        serializer.serialize(message, metadata); // must still serialize

        // Identity no longer rides in the key (production uses a LongWritable
        // positional key). The extraction strategy is still live — it drives
        // the failOnMissingIdentity check — so assert it directly.
        assertThat(serializer.getIdentityExtractor().extractIdentity(payload))
                .isEqualTo("000042");
    }

    private RecordMetadata buildMinimalMetadata() {
        return RecordMetadata.builder()
                .bindingId("claims")
                .sourceFile("test.seq")
                .recordOffset(0)
                .build();
    }

    @Test
    void valueIsWhitespaceNormalisedLikeTheMdb() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        String payload = "<Claim>\n\t<CLM_XMITSN_ID>id-1</CLM_XMITSN_ID>\r\n</Claim>";
        TextMessage message = session.createTextMessage(payload);

        String stored = serializer.serialize(message, buildMinimalMetadata())
                .getValue().toString();

        assertThat(stored).doesNotContain("\n").doesNotContain("\r").doesNotContain("\t");
        assertThat(stored).isEqualTo("<Claim>  <CLM_XMITSN_ID>id-1</CLM_XMITSN_ID>  </Claim>");
    }

    @Test
    void identityIsExtractedFromTheNormalisedPayload() throws Exception {
        // Identity must correspond to what is written to the file. Extracting
        // from the raw body could yield a value containing newlines that no
        // reader of the stored payload could ever recover.
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(
                ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID"));

        String payload = "<Claim><CLM_XMITSN_ID>\nid-2\n</CLM_XMITSN_ID></Claim>";
        TextMessage message = session.createTextMessage(payload);

        String stored = serializer.serialize(message, buildMinimalMetadata())
                .getValue().toString();

        // The stored payload carries spaces, so the recoverable identity does too
        assertThat(stored).contains("<CLM_XMITSN_ID> id-2 </CLM_XMITSN_ID>");
        assertThat(serializer.getIdentityExtractor().extractIdentity(stored))
                .isEqualTo(" id-2 ");
    }
}
