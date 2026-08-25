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
                "<MessageHeaderDetailsType></MessageHeaderDetailsType>");

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
                "<MessageHeaderDetailsType></MessageHeaderDetailsType>");

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
                "<MessageHeaderDetailsType></MessageHeaderDetailsType>");

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
                "<MessageHeaderDetailsType></MessageHeaderDetailsType>");

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
        String rawHeader = "<MessageHeaderDetailsType><SomeTag>value</SomeTag></MessageHeaderDetailsType>";
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, rawHeader);

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        // Header should contain injected fields in raw format
        assertThat(rewrittenHeader).contains("<ReportingSystem>DMIH/DL</ReportingSystem>");
        assertThat(rewrittenHeader).contains("<SourceSystem>IIB</SourceSystem>");
        assertThat(rewrittenHeader).contains("<MesgStatus>RCVD</MesgStatus>");
    }

    @Test
    void handlesEscapedTagFormat() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        String escapedHeader = "&lt;MessageHeaderDetailsType&gt;&lt;SomeTag&gt;value&lt;/SomeTag&gt;&lt;/MessageHeaderDetailsType&gt;";
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, escapedHeader);

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        // Header should contain injected fields in escaped format
        assertThat(rewrittenHeader).contains("&lt;ReportingSystem&gt;DMIH/DL&lt;/ReportingSystem&gt;");
        assertThat(rewrittenHeader).contains("&lt;SourceSystem&gt;IIB&lt;/SourceSystem&gt;");
        assertThat(rewrittenHeader).contains("&lt;MesgStatus&gt;RCVD&lt;/MesgStatus&gt;");
    }

    @Test
    void injectedFieldsAppearBeforeClosingTag() throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        String header = "<MessageHeaderDetailsType><ExistingTag>data</ExistingTag></MessageHeaderDetailsType>";
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, header);

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        // Fields should be spliced before the closing tag
        int reportingPos = rewrittenHeader.indexOf("<ReportingSystem>");
        int closingPos = rewrittenHeader.indexOf("</MessageHeaderDetailsType>");

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
                "<MessageHeaderDetailsType></MessageHeaderDetailsType>");

        Optional<Message> result = builder.build(session, sourceMessage);

        assertThat(result).isPresent();
        TextMessage trackerMessage = (TextMessage) result.get();
        String rewrittenHeader = trackerMessage.getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);

        assertThat(rewrittenHeader).contains("<ReportingSystem>CustomReporter</ReportingSystem>");
        assertThat(rewrittenHeader).contains("<SourceSystem>CustomSource</SourceSystem>");
        assertThat(rewrittenHeader).contains("<MesgStatus>PROC</MesgStatus>");
        // DestSystem takes destinationStatus — reads odd, but matches the legacy
        assertThat(rewrittenHeader).contains("<DestSystem>DONE</DestSystem>");
    }

    // --- Property preservation ---


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

    // --- Captured legacy constants (§20.4) ---
    // These pin values taken from EJBHelper. They are cheap, and the cost of
    // getting one wrong is a tracker message that looks correct and is not.

    @Test
    void rootEndTagMatchesTheLegacyConstant() {
        // The placeholder used "</MessageHeaderDetailsType>". The real element is
        // MessageHeaderDetailsType — a one-word difference that would make every
        // splice miss and silently append at the end of the header instead.
        assertThat(RmsTrackerMessageBuilder.HeaderRewriter.ROOT_END_TAG)
                .isEqualTo("</MessageHeaderDetailsType>");
        assertThat(RmsTrackerMessageBuilder.HeaderRewriter.ROOT_END_TAG_ESCAPED)
                .isEqualTo("&lt;/MessageHeaderDetailsType&gt;");
    }

    @Test
    void tagListMatchesTheLegacyOrderAndContents() {
        assertThat(RmsTrackerMessageBuilder.HeaderRewriter.TAG_LIST)
                .containsExactly("ReportingSystem", "SourceSystem", "DestSystem",
                        "MesgStatus", "CreatedTimeStamp");
    }

    @Test
    void tagsAreBuiltInBothRawAndEscapedForms() {
        assertThat(RmsTrackerMessageBuilder.HeaderRewriter.completeStartTag("MesgStatus", 0))
                .isEqualTo("<MesgStatus>");
        assertThat(RmsTrackerMessageBuilder.HeaderRewriter.completeEndTag("MesgStatus", 0))
                .isEqualTo("</MesgStatus>");
        assertThat(RmsTrackerMessageBuilder.HeaderRewriter.completeStartTag("MesgStatus", 1))
                .isEqualTo("&lt;MesgStatus&gt;");
        assertThat(RmsTrackerMessageBuilder.HeaderRewriter.completeEndTag("MesgStatus", 1))
                .isEqualTo("&lt;/MesgStatus&gt;");
    }

    // --- Faithful reproduction of EJBHelper.getStringMessageHeader ---

    private String rewrite(String header) throws Exception {
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                new RmsTrackerMessageBuilder.TrackerFields("DMIH/DL", "IIB", "RCVD", ""));
        TextMessage m = session.createTextMessage("<p/>");
        m.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS, header);
        return ((TextMessage) builder.build(session, m).get())
                .getStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS);
    }

    @Test
    void allFiveTagsAreInjectedWithTheLegacyValueMapping() throws Exception {
        String out = rewrite("<MessageHeaderDetailsType><Keep>x</Keep></MessageHeaderDetailsType>");

        assertThat(out).contains("<ReportingSystem>DMIH/DL</ReportingSystem>");
        assertThat(out).contains("<SourceSystem>IIB</SourceSystem>");
        assertThat(out).contains("<MesgStatus>RCVD</MesgStatus>");
        // DestSystem takes destinationStatus (empty here) — the legacy mapping
        assertThat(out).contains("<DestSystem></DestSystem>");
        assertThat(out).containsPattern("<CreatedTimeStamp>\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}</CreatedTimeStamp>");

        // Unrelated content is preserved
        assertThat(out).contains("<Keep>x</Keep>");
    }

    @Test
    void injectionLandsImmediatelyBeforeTheRootEndTag() throws Exception {
        String out = rewrite("<MessageHeaderDetailsType><Keep>x</Keep></MessageHeaderDetailsType>");

        // Everything injected sits inside the root element, not appended after it
        assertThat(out).endsWith("</MessageHeaderDetailsType>");
        assertThat(out.indexOf("<ReportingSystem>"))
                .isLessThan(out.indexOf("</MessageHeaderDetailsType>"));
    }

    @Test
    void anExistingTagIsRemovedBeforeItIsReAdded() throws Exception {
        String out = rewrite("<MessageHeaderDetailsType>"
                + "<MesgStatus>STALE</MesgStatus></MessageHeaderDetailsType>");

        // The stale value is gone and appears exactly once with the new value
        assertThat(out).doesNotContain("STALE");
        assertThat(out.split("<MesgStatus>", -1).length - 1)
                .as("MesgStatus must appear exactly once, not duplicated")
                .isEqualTo(1);
        assertThat(out).contains("<MesgStatus>RCVD</MesgStatus>");
    }

    @Test
    void escapedHeadersAreRewrittenInEscapedForm() throws Exception {
        String out = rewrite("&lt;MessageHeaderDetailsType&gt;&lt;Keep&gt;x&lt;/Keep&gt;"
                + "&lt;/MessageHeaderDetailsType&gt;");

        assertThat(out).contains("&lt;ReportingSystem&gt;DMIH/DL&lt;/ReportingSystem&gt;");
        assertThat(out).contains("&lt;MesgStatus&gt;RCVD&lt;/MesgStatus&gt;");
        // and not the raw form
        assertThat(out).doesNotContain("<ReportingSystem>");
    }

    @Test
    void headerWithoutRootEndTagIsLeftAlone() throws Exception {
        // buildResultData only runs when a root end tag is present, so nothing
        // accumulates and the header passes through unchanged.
        String input = "<SomethingElse>x</SomethingElse>";
        assertThat(rewrite(input)).isEqualTo(input);
    }

    @Test
    void regexMetacharactersInTagContentAreALegacyHazard() throws Exception {
        // setReplacedTagData uses replaceAll, which treats the extracted span as
        // a REGEX. Content with metacharacters therefore misbehaves or throws.
        // Reproduced deliberately: using literal replace would emit a tracker
        // message where the legacy loses one. Documented here so nobody
        // "fixes" it without realising it changes observable output.
        String header = "<MessageHeaderDetailsType><MesgStatus>a(b</MesgStatus>"
                + "</MessageHeaderDetailsType>";

        assertThatThrownBy(() -> rewrite(header))
                .isInstanceOf(java.util.regex.PatternSyntaxException.class);
    }

    @Test
    void trackerMessageCarriesOnlyBodyAndHeaderLikeTheLegacy() throws Exception {
        // EJBHelper builds `session.createTextMessage(textMessage.getText())`
        // and sets only MessageHeaderDetails. Every other source property is
        // dropped — including JMSCorrelationID, which an earlier version of
        // this builder copied. Anything extra is a property the tracker
        // consumer has never received.
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage source = session.createTextMessage("<Test>payload</Test>");
        source.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetailsType/></MessageHeaderDetailsType>");
        source.setJMSCorrelationID("CORR-123");
        source.setStringProperty("SomeOtherProperty", "should-not-propagate");

        TextMessage tracker = (TextMessage) builder.build(session, source).get();

        assertThat(tracker.getJMSCorrelationID())
                .as("legacy does not copy JMSCorrelationID")
                .isNull();
        assertThat(tracker.propertyExists("SomeOtherProperty")).isFalse();

        // Body is a verbatim copy, and the header property is present
        assertThat(tracker.getText()).isEqualTo("<Test>payload</Test>");
        assertThat(tracker.propertyExists(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS)).isTrue();
    }
}
