package com.hcsc.datalake.mqintake.claims;

import com.hcsc.datalake.mqintake.claims.serializer.ClaimsIdentityExtractor;
import com.hcsc.datalake.mqintake.claims.serializer.ClaimsRecordSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer.SerializationException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the explicit claims identity contract (round 2 prompt 9).
 *
 * <p>The stable claims identity is unresolved (open item #17), so:
 * <ul>
 *   <li>Identity must be configured explicitly — no silent default</li>
 *   <li>Production startup fails without a configured identity</li>
 *   <li>Missing identity in payload fails serialization when required</li>
 * </ul>
 */
class ClaimsIdentityGatingTest {

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (Exception ignored) {
        }
    }

    // --- Configuration gating ---

    @Test
    void productionModeWithoutIdentityFieldFailsStartup() {
        ClaimsConfiguration config = new ClaimsConfiguration("", true);

        assertThatThrownBy(config::resolveIdentityExtractor)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("claims.identity-field")
                .hasMessageContaining("Production startup is blocked");
    }

    @Test
    void productionModeWithExplicitClmXmitsnIdStarts() {
        ClaimsConfiguration config = new ClaimsConfiguration("CLM_XMITSN_ID", true);

        ClaimsIdentityExtractor extractor = config.resolveIdentityExtractor();

        assertThat(extractor.extractIdentity("<CLM_XMITSN_ID>ABC-123</CLM_XMITSN_ID>"))
                .isEqualTo("ABC-123");
        // Only the configured field — no candidate fallback
        assertThat(extractor.extractIdentity("<REC_CTL_NBR>999</REC_CTL_NBR>")).isNull();
        assertThat(config.isIdentityConfigured()).isTrue();
    }

    @Test
    void productionModeWithExplicitRecCtlNbrStarts() {
        ClaimsConfiguration config = new ClaimsConfiguration("REC_CTL_NBR", true);

        ClaimsIdentityExtractor extractor = config.resolveIdentityExtractor();

        assertThat(extractor.extractIdentity("<REC_CTL_NBR>999</REC_CTL_NBR>"))
                .isEqualTo("999");
        assertThat(extractor.extractIdentity("<CLM_XMITSN_ID>ABC</CLM_XMITSN_ID>")).isNull();
    }

    @Test
    void nonProductionModeWithoutIdentityUsesFixture() {
        ClaimsConfiguration config = new ClaimsConfiguration("", false);

        ClaimsIdentityExtractor extractor = config.resolveIdentityExtractor();

        // Fixture behavior: tries both candidate fields
        assertThat(extractor.extractIdentity("<CLM_XMITSN_ID>A</CLM_XMITSN_ID>")).isEqualTo("A");
        assertThat(extractor.extractIdentity("<REC_CTL_NBR>B</REC_CTL_NBR>")).isEqualTo("B");
        assertThat(config.isIdentityConfigured()).isFalse();
    }

    // --- Serializer identity policy ---

    @Test
    void missingIdentityFailsSerializationWhenRequired() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(
                ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID"), true);

        TextMessage message = session.createTextMessage("<Claim><OTHER>x</OTHER></Claim>");

        assertThatThrownBy(() -> serializer.serialize(message, testMetadata()))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("identity");
    }

    @Test
    void blankIdentityFailsSerializationWhenRequired() throws Exception {
        // Extractor returns a blank value
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(
                payload -> "   ", true);

        TextMessage message = session.createTextMessage("<Claim/>");

        assertThatThrownBy(() -> serializer.serialize(message, testMetadata()))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("identity");
    }

    @Test
    void stableReplayReturnsSameIdentity() throws Exception {
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer(
                ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID"), true);

        String payload = "<Claim><CLM_XMITSN_ID>STABLE-42</CLM_XMITSN_ID></Claim>";

        // Same business payload replayed as two distinct JMS messages
        // (different mq_message_id) must yield the same payload identity.
        TextMessage first = session.createTextMessage(payload);
        TextMessage second = session.createTextMessage(payload);

        var record1 = serializer.serialize(first, testMetadata());
        var record2 = serializer.serialize(second, testMetadata());

        String key1 = record1.getKey().toString();
        String key2 = record2.getKey().toString();

        assertThat(key1).contains("payload_guid=STABLE-42");
        assertThat(key2).contains("payload_guid=STABLE-42");
    }

    @Test
    void missingIdentityToleratedInFixtureMode() throws Exception {
        // No-arg ctor = non-production fixture, tolerant of missing identity
        ClaimsRecordSerializer serializer = new ClaimsRecordSerializer();

        TextMessage message = session.createTextMessage("<Claim><OTHER>x</OTHER></Claim>");

        var record = serializer.serialize(message, testMetadata());
        assertThat(record.getKey().toString()).contains("payload_guid=");
    }

    private RecordMetadata testMetadata() {
        return RecordMetadata.builder()
                .bindingId("claims")
                .mqMessageId("ID:transport-only")
                .consumeTimestamp(Instant.parse("2026-08-22T10:00:00Z"))
                .build();
    }
}
