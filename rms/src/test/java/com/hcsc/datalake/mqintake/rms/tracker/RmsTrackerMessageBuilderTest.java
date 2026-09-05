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
    void customBodyModeFailsLoudlyInsteadOfSendingEmptyBodies() throws Exception {
        // body-mode is free-form YAML; a typo selecting CUSTOM used to
        // silently produce empty-bodied trackers. RMS defines no custom body,
        // so it must say so at the first build, not ship blanks downstream.
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                com.hcsc.datalake.mqintake.core.config.TrackerBodyMode.CUSTOM,
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage sourceMessage = session.createTextMessage("<Test>payload</Test>");
        sourceMessage.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetails></MessageHeaderDetails>");

        assertThatThrownBy(() -> builder.build(session, sourceMessage))
                .isInstanceOf(java.lang.IllegalStateException.class)
                .hasMessageContaining("CUSTOM");
    }

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

    // customModeReturnsEmptyBodyByDefault was deliberately removed: it pinned
    // the silent-empty-body behaviour the production-readiness review flagged
    // as a config-typo hazard. CUSTOM now fails loudly — see
    // customBodyModeFailsLoudlyInsteadOfSendingEmptyBodies above.

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
        assertThat(HeaderRewriter.ROOT_END_TAG)
                .isEqualTo("</MessageHeaderDetailsType>");
        assertThat(HeaderRewriter.ROOT_END_TAG_ESCAPED)
                .isEqualTo("&lt;/MessageHeaderDetailsType&gt;");
    }

    @Test
    void tagListMatchesTheLegacyOrderAndContents() {
        assertThat(HeaderRewriter.TAG_LIST)
                .containsExactly("ReportingSystem", "SourceSystem", "DestSystem",
                        "MesgStatus", "CreatedTimeStamp");
    }

    @Test
    void tagsAreBuiltInBothRawAndEscapedForms() {
        assertThat(HeaderRewriter.completeStartTag("MesgStatus", 0))
                .isEqualTo("<MesgStatus>");
        assertThat(HeaderRewriter.completeEndTag("MesgStatus", 0))
                .isEqualTo("</MesgStatus>");
        assertThat(HeaderRewriter.completeStartTag("MesgStatus", 1))
                .isEqualTo("&lt;MesgStatus&gt;");
        assertThat(HeaderRewriter.completeEndTag("MesgStatus", 1))
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
    void anExistingTagInAnEscapedHeaderIsAlsoRemovedBeforeItIsReAdded() throws Exception {
        // The escaped tests above all use tags that are NOT in TAG_LIST
        // (SomeTag, Keep), so the escaped half of the removal branch was never
        // exercised: deleting it entirely left the suite green. Without it an
        // escaped header that already carries a tracker tag ends up with the
        // tag twice — the stale value and the new one — where the legacy
        // replaces it.
        String out = rewrite("&lt;MessageHeaderDetailsType&gt;"
                + "&lt;MesgStatus&gt;STALE&lt;/MesgStatus&gt;"
                + "&lt;/MessageHeaderDetailsType&gt;");

        assertThat(out).doesNotContain("STALE");
        assertThat(out.split("&lt;MesgStatus&gt;", -1).length - 1)
                .as("MesgStatus must appear exactly once, not duplicated")
                .isEqualTo(1);
        assertThat(out).contains("&lt;MesgStatus&gt;RCVD&lt;/MesgStatus&gt;");
    }

    @Test
    void aRepeatedTagHasEverythingBetweenFirstAndLastOccurrenceRemoved() throws Exception {
        // A legacy behaviour reproduced on purpose and documented as such, but
        // held by nothing: setReplacedTagData spans the FIRST start tag to the
        // LAST end tag, so a tag appearing twice takes everything between them
        // with it. Narrowing the span to the first end tag — which is what a
        // reader would "fix" it to — changed no test result.
        String out = rewrite("<MessageHeaderDetailsType>"
                + "<MesgStatus>FIRST</MesgStatus>"
                + "<Between>BETWEEN</Between>"
                + "<MesgStatus>SECOND</MesgStatus>"
                + "</MessageHeaderDetailsType>");

        assertThat(out).doesNotContain("FIRST").doesNotContain("SECOND");
        assertThat(out)
                .as("the legacy span swallows whatever sits between the two occurrences")
                .doesNotContain("BETWEEN");
        assertThat(out.split("<MesgStatus>", -1).length - 1)
                .as("re-added exactly once").isEqualTo(1);
    }

    @Test
    void theLegacyTimestampIsFormattedInTheJvmDefaultZone() throws Exception {
        // SimpleDateFormat with no explicit zone formats in the JVM default,
        // which is what the legacy does. Setting UTC instead shifts every
        // CreatedTimeStamp by the local offset — silently, and only visible to
        // whoever reads the tracker queue. Nothing held it.
        String out = rewrite("<MessageHeaderDetailsType></MessageHeaderDetailsType>");

        String stamp = out.substring(
                out.indexOf("<CreatedTimeStamp>") + "<CreatedTimeStamp>".length(),
                out.indexOf("</CreatedTimeStamp>"));

        String expected = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
                .format(new java.util.Date());
        // Same minute is enough: a wrong zone is out by at least 15 minutes,
        // and every real offset in use is a multiple of that.
        assertThat(stamp.substring(0, stamp.lastIndexOf(':')))
                .as("formatted in the default zone, not UTC")
                .isEqualTo(expected.substring(0, expected.lastIndexOf(':')));
    }

    @Test
    void aQuantifierInTheSpanSilentlyLeavesTheStaleTagInPlace() throws Exception {
        // The other half of the regex hazard, and the worse half. The test
        // above covers metacharacters that THROW, where the tracker message is
        // lost. A metacharacter that merely changes what the pattern matches —
        // a quantifier such as '?' — makes replaceAll match nothing, so the
        // stale tag is never removed and the message goes out carrying the old
        // value AND the new one for the same tag.
        //
        // Faithful to the legacy, which calls the same replaceAll on the same
        // span, so this is reproduced rather than introduced. Found by
        // accident: a '?' in an unrelated test payload.
        String out = rewrite("<MessageHeaderDetailsType>"
                + "<MesgStatus>STALE</MesgStatus>"
                + "<Note>why?</Note>"
                + "<MesgStatus>ALSO_STALE</MesgStatus>"
                + "</MessageHeaderDetailsType>");

        assertThat(out)
                .as("nothing was removed — the quantifier stopped the span matching itself")
                .contains("STALE");
        assertThat(out.split("<MesgStatus>", -1).length - 1)
                .as("so the tag ships three times: two stale, one fresh")
                .isEqualTo(3);
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

    // ------------------------------------------------------------------
    // Pinned legacy hazards (review finding #10). These tests assert what
    // the code DOES, not what would be safest: the rewrite is a line-by-line
    // port of EJBHelper, whose unguarded index arithmetic these paths share.
    // A guard here would DIVERGE from legacy in exactly these cases, so per
    // the standing parity decision the behaviour is pinned, not changed. A
    // hardened mode, if ever wanted, must be a separately-flagged opt-in.
    // ------------------------------------------------------------------

    @Test
    void startTagWithoutItsEndTagThrowsExactlyLikeTheLegacy() throws Exception {
        // lastIndexOf(endTag) is -1, so substring(begin, -1 + endTag.length())
        // has begin > end whenever the tag does not open within the first few
        // characters. The RuntimeException is caught by sendTrackerMessages:
        // the message still lands, only this tracker notification is lost —
        // the same observable outcome as the legacy crash.
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage source = session.createTextMessage("body");
        source.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MessageHeaderDetailsType><MesgStatus>open-but-never-closed"
                        + "</MessageHeaderDetailsType>");

        assertThatThrownBy(() -> builder.build(session, source))
                .isInstanceOf(StringIndexOutOfBoundsException.class);
    }

    @Test
    void startTagAtHeaderStartWithoutEndTagSilentlyRemovesAGarbageSpan() throws Exception {
        // The nastier sibling: when the tag opens within the end tag's length
        // of position zero, substring(0, endTag.length()-1) SUCCEEDS with a
        // nonsense span and replaceAll strips it — no exception, no log. With
        // "<MesgStatus>x" the span is exactly "<MesgStatus>" (12 chars), so
        // the opener vanishes and the bare content is what gets sent.
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage source = session.createTextMessage("body");
        source.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MesgStatus>orphan-content");

        Optional<Message> result = builder.build(session, source);

        assertThat(result).isPresent();
        assertThat(result.get().getStringProperty(
                RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS))
                .as("silent mutilation, pinned so a refactor cannot change it unnoticed")
                .isEqualTo("orphan-content");
    }

    @Test
    void knownTagWithoutRootEndTagIsStrippedAndNeverReinjected() throws Exception {
        // Removal runs whenever a known tag is present; re-injection runs only
        // when the ROOT end tag is present. A header with the former but not
        // the latter loses the tag with nothing put back, and the mutilated
        // header is what the tracker consumer receives.
        RmsTrackerMessageBuilder builder = new RmsTrackerMessageBuilder(
                RmsTrackerMessageBuilder.TrackerFields.defaultRms());

        TextMessage source = session.createTextMessage("body");
        source.setStringProperty(RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS,
                "<MesgStatus>OLD</MesgStatus><Other>kept</Other>");

        Optional<Message> result = builder.build(session, source);

        assertThat(result).isPresent();
        assertThat(result.get().getStringProperty(
                RmsTrackerMessageBuilder.MESSAGE_HEADER_DETAILS))
                .isEqualTo("<Other>kept</Other>");
    }

}
