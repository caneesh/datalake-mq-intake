package com.hcsc.datalake.mqintake.rms.tracker;

import com.hcsc.datalake.mqintake.core.config.TrackerBodyMode;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import javax.jms.*;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RmsTrackerMessageBuilder.
 *
 * <p>Verifies the two critical behaviours from §20.3:
 * <ol>
 *   <li>Null MessageHeaderDetails → return Optional.empty() (no send)</li>
 *   <li>Handles both raw (&lt;tag&gt;) and escaped (&amp;lt;tag&amp;gt;) variants</li>
 * </ol>
 */
class RmsTrackerMessageBuilderTest {

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

    // --- Null MessageHeaderDetails guard (§20.3 behaviour #1) ---

    @Test
    void returnsEmptyWhenMessageHeaderDetailsIsNull() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        // MessageHeaderDetails is NOT set

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isEmpty();
    }

    @Test
    void returnsMessageWhenMessageHeaderDetailsIsPresent() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetails></MessageHeaderDetails>");

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
    }

    @Test
    void nullGuardPreventsSendRegardlessOfMode() throws Exception {
        // Test all body modes — null guard should apply to all
        for (TrackerBodyMode mode : TrackerBodyMode.values()) {
            RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                    mode, RmsTrackerMessageBuilder.TrackerFields.defaultRms());

            TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
            // MessageHeaderDetails is NOT set

            Optional<Message> result = builder.build(session, sourceMessage);

            assertThat(result)
                    .as("Mode %s should respect null header guard", mode)
                    .isEmpty();
        }
    }

    // --- Body mode tests (§2.2) ---

    @Test
    void fullCopyModeCopiesEntirePayload() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                TrackerBodyMode.FULL_COPY,
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        String payload = "<MemberEvent><Data>Important content here</Data></MemberEvent>";
        TextMessage sourceMessage = session.createTextMessage(payload);
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetails></MessageHeaderDetails>");

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        assertThat(trackerMessage.getText()).isEqualTo(payload);
    }

    @Test
    void headerOnlyModeReturnsEmptyBody() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                TrackerBodyMode.HEADER_ONLY,
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Big>payload</Big>");
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetails></MessageHeaderDetails>");

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        assertThat(trackerMessage.getText()).isEmpty();
    }

    @Test
    void customModeReturnsEmptyBodyByDefault() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                TrackerBodyMode.CUSTOM,
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetails></MessageHeaderDetails>");

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        assertThat(trackerMessage.getText()).isEmpty();
    }

    // --- Raw vs escaped tag handling (§20.3 behaviour #2) ---

    @Test
    void handlesRawTagFormat() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        String rawHeader = "<MessageHeaderDetails><SomeTag>value</SomeTag></MessageHeaderDetails>";
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, rawHeader);

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        // Header should contain injected fields in raw format
        assertThat(rewrittenHeader).contains("<ReportingSystem>DMIH/DL</ReportingSystem>");
        assertThat(rewrittenHeader).contains("<SourceSystem>IIB</SourceSystem>");
        assertThat(rewrittenHeader).contains("<MessageStatus>RCVD</MessageStatus>");
    }

    @Test
    void handlesEscapedTagFormat() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        String escapedHeader = "&lt;MessageHeaderDetails&gt;&lt;SomeTag&gt;value&lt;/SomeTag&gt;&lt;/MessageHeaderDetails&gt;";
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, escapedHeader);

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        // Header should contain injected fields in escaped format
        assertThat(rewrittenHeader).contains("&lt;ReportingSystem&gt;DMIH/DL&lt;/ReportingSystem&gt;");
        assertThat(rewrittenHeader).contains("&lt;SourceSystem&gt;IIB&lt;/SourceSystem&gt;");
        assertThat(rewrittenHeader).contains("&lt;MessageStatus&gt;RCVD&lt;/MessageStatus&gt;");
    }

    @Test
    void injectedFieldsAppearBeforeClosingTag() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        String header = "<MessageHeaderDetails><ExistingTag>data</ExistingTag></MessageHeaderDetails>";
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, header);

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        // Fields should be spliced before the closing tag
        int reportingPos = rewrittenHeader.indexOf("<ReportingSystem>");
        int closingPos = rewrittenHeader.indexOf("</MessageHeaderDetails>");

        assertThat(reportingPos).isLessThan(closingPos);
    }

    // --- TrackerFields configuration ---

    @Test
    void defaultRmsFieldsMatchSpec() {
        RmsTrackerMessageBuilder.TrackerFields fields =
                RmsTrackerMessageBuilder.TrackerFields.defaultRms();

        // Per §20.1: reportingSystem="DMIH/DL", sourceSystem="IIB", messageStatus="RCVD"
        assertThat(fields.getReportingSystem()).isEqualTo("DMIH/DL");
        assertThat(fields.getSourceSystem()).isEqualTo("IIB");
        assertThat(fields.getMessageStatus()).isEqualTo("RCVD");
        assertThat(fields.getDestinationStatus()).isEmpty();
    }

    @Test
    void customFieldsAreUsed() throws Exception {
        RmsTrackerMessageBuilder.TrackerFields customFields =
                new RmsTrackerMessageBuilder.TrackerFields(
                        "CustomReporter", "CustomSource", "PROC", "DONE");

        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(customFields);

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetails></MessageHeaderDetails>");

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        assertThat(rewrittenHeader).contains("<ReportingSystem>CustomReporter</ReportingSystem>");
        assertThat(rewrittenHeader).contains("<SourceSystem>CustomSource</SourceSystem>");
        assertThat(rewrittenHeader).contains("<MessageStatus>PROC</MessageStatus>");
        assertThat(rewrittenHeader).contains("<DestinationStatus>DONE</DestinationStatus>");
    }

    // --- Property preservation ---

    @Test
    void copiesCorrelationId() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        sourceMessage.setJMSCorrelationID("correlation-12345");
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetails></MessageHeaderDetails>");

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        assertThat(result.get().getJMSCorrelationID()).isEqualTo("correlation-12345");
    }

    // --- Edge cases ---

    @Test
    void handlesEmptyHeader() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, "");

        Optional<Message> result = builder.build(session, sourceMessage);

        // Empty string is not null — should still produce a message
        assertThat(result).isPresent();
    }

    @Test
    void gettersReturnConfiguredValues() {
        RmsTrackerMessageBuilder.TrackerFields fields =
                new RmsTrackerMessageBuilder.TrackerFields("A", "B", "C", "D");
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                TrackerBodyMode.HEADER_ONLY, fields);

        assertThat(builder.getBodyMode()).isEqualTo(TrackerBodyMode.HEADER_ONLY);
        assertThat(builder.getTrackerFields()).isSameAs(fields);
    }
}
