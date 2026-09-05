package com.hcsc.datalake.mqintake.rms.tracker;

import com.hcsc.datalake.mqintake.core.config.TrackerBodyMode;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Objects;
import java.util.Optional;

/**
 * TrackerMessageBuilder for the RMS/HPS membership feed.
 *
 * <p>Implements the tracker message contract reverse-engineered in DESIGN.md §20.
 * The tracker message carries specific content that is a downstream contract —
 * any consumer of the tracker queue is unaware this service was rewritten.
 *
 * <p><strong>Two behaviours preserved from §20.3:</strong>
 * <ol>
 *   <li><strong>Null MessageHeaderDetails guard:</strong> If the source message
 *       lacks MessageHeaderDetails, return {@code Optional.empty()} and send
 *       nothing. This guard is why the claims feed produces no tracker messages
 *       today — without it, messages lacking the header would newly appear on
 *       the tracker queue.</li>
 *   <li><strong>Dual raw/escaped tag handling:</strong> Some upstream senders
 *       deliver XML-escaped content ({@code &lt;tag&gt;}). A builder handling
 *       only one variant will silently stop tagging those messages.</li>
 * </ol>
 *
 * <p>Supports three body modes per §2.2:
 * <ul>
 *   <li>FULL_COPY (default): verbatim copy of source payload, bit-compatible
 *       with current behaviour</li>
 *   <li>HEADER_ONLY: empty body, rewritten properties only</li>
 *   <li>CUSTOM: whatever this builder returns (for future extensibility)</li>
 * </ul>
 */
public class RmsTrackerMessageBuilder implements TrackerMessageBuilder {

    private static final Logger log = LoggerFactory.getLogger(RmsTrackerMessageBuilder.class);

    /**
     * The JMS property containing the header to rewrite.
     */
    public static final String MESSAGE_HEADER_DETAILS = "MessageHeaderDetails";

    /**
     * Returns the legacy artifacts still missing before the tracker header
     * rewrite is contract-complete (§20.4). An empty list means the contract
     * is ready for production.
     *
     * <p>Each gap is derived from the evidence flags in {@link HeaderRewriter}
     * ({@code TAG_LIST}, {@code ROOT_END_TAG_VERIFIED}, …), which are set as
     * legacy artifacts are captured and verified. The production startup gate
     * reads this list, so a regression in captured evidence fails startup
     * rather than silently shipping a divergent rewrite.
     */
    public static java.util.List<String> trackerContractGaps() {
        java.util.List<String> gaps = new java.util.ArrayList<>();
        if (HeaderRewriter.TAG_LIST.length == 0) {
            gaps.add("tagList contents not captured (§20.4)");
        }
        if (!HeaderRewriter.ROOT_END_TAG_VERIFIED) {
            gaps.add("ROOT_END_TAG / ROOT_END_TAG_CHAR unverified against legacy (§20.4)");
        }
        if (!HeaderRewriter.TAG_VALUE_MAPPING_CAPTURED) {
            gaps.add("buildResultData / setReplacedTagData bodies not captured (§20.4) — " +
                    "which of the four supplied values each of the five tags receives is " +
                    "not derivable from the call site");
        }
        if (!HeaderRewriter.GOLDEN_MASTER_AVAILABLE) {
            gaps.add("No before/after MessageHeaderDetails sample to validate against");
        }
        return gaps;
    }

    /**
     * Returns true when the legacy tracker header rewrite contract is complete
     * and this builder is safe to run in production. RMS TRACKED production
     * startup is gated on this.
     */
    public static boolean isTrackerContractReady() {
        return trackerContractGaps().isEmpty();
    }

    private final TrackerBodyMode bodyMode;
    private final TrackerFields trackerFields;
    private final HeaderRewriter headerRewriter;

    /**
     * Creates a builder with the specified configuration.
     *
     * @param bodyMode      controls body content (FULL_COPY, HEADER_ONLY, CUSTOM)
     * @param trackerFields the fields to inject into the header
     */
    public RmsTrackerMessageBuilder(TrackerBodyMode bodyMode, TrackerFields trackerFields) {
        this.bodyMode = Objects.requireNonNull(bodyMode, "bodyMode required");
        this.trackerFields = Objects.requireNonNull(trackerFields, "trackerFields required");
        this.headerRewriter = new HeaderRewriter();
    }

    /**
     * Creates a builder with FULL_COPY mode and the specified tracker fields.
     */
    public RmsTrackerMessageBuilder(TrackerFields trackerFields) {
        this(TrackerBodyMode.FULL_COPY, trackerFields);
    }

    @Override
    public Optional<Message> build(Session session, Message sourceMessage) throws JMSException {
        // GUARD: If MessageHeaderDetails is null, return empty and send nothing
        // This is the §20.3 null-header guard that prevents claims messages
        // (which lack this property) from appearing on the tracker queue
        String headerDetails = sourceMessage.getStringProperty(MESSAGE_HEADER_DETAILS);
        if (headerDetails == null) {
            log.debug("MessageHeaderDetails is null — suppressing tracker message");
            return Optional.empty();
        }

        // Rewrite the header with tracker fields
        String rewrittenHeader = headerRewriter.rewrite(headerDetails, trackerFields);

        // Build the tracker message body based on mode
        String body = buildBody(sourceMessage);

        // The legacy builds a fresh message carrying ONLY the body and the
        // rewritten header — every other source property is dropped. Matching
        // that exactly: anything extra we set is a property the tracker
        // consumer has never received before.
        TextMessage trackerMessage = session.createTextMessage(body);

        // Legacy sets the property only when the rewrite returned non-null
        if (rewrittenHeader != null) {
            trackerMessage.setStringProperty(MESSAGE_HEADER_DETAILS, rewrittenHeader);
        }

        copyMessageProperties(sourceMessage, trackerMessage);

        return Optional.of(trackerMessage);
    }

    /**
     * Builds the message body based on the configured body mode.
     */
    private String buildBody(Message sourceMessage) throws JMSException {
        switch (bodyMode) {
            case FULL_COPY:
                // Verbatim copy of source payload — bit-compatible with current behaviour
                if (sourceMessage instanceof TextMessage) {
                    String text = ((TextMessage) sourceMessage).getText();
                    return text != null ? text : "";
                }
                log.warn("Source message is not TextMessage, using empty body");
                return "";

            case HEADER_ONLY:
                // Empty body — rewritten properties only
                return "";

            case CUSTOM:
                // For CUSTOM mode, subclasses can override buildCustomBody()
                return buildCustomBody(sourceMessage);

            default:
                throw new java.lang.IllegalStateException("Unknown body mode: " + bodyMode);
        }
    }

    /**
     * Override this method in subclasses to provide custom body content
     * when bodyMode is CUSTOM.
     *
     * <p>Throws rather than returning an empty body: {@code body-mode} is a
     * free-form YAML value, and a config typo selecting CUSTOM for RMS used
     * to silently produce empty-bodied tracker messages — indistinguishable
     * from a deliberate HEADER_ONLY choice, invisible until a downstream
     * consumer noticed the payload was missing. RMS defines no custom body,
     * so selecting CUSTOM here is always a mistake and says so at the first
     * tracker build.
     */
    protected String buildCustomBody(Message sourceMessage) throws JMSException {
        throw new java.lang.IllegalStateException(
                "tracker.body-mode CUSTOM is not supported by RmsTrackerMessageBuilder — "
                        + "use FULL_COPY (production) or HEADER_ONLY, or subclass and "
                        + "override buildCustomBody()");
    }

    /**
     * Copies relevant properties from source to tracker message.
     * Override to customize property copying.
     */
    protected void copyMessageProperties(Message source, Message target) throws JMSException {
        // Intentionally empty. The legacy EJBHelper copies NOTHING beyond the
        // body and MessageHeaderDetails, so neither do we. An earlier version
        // copied JMSCorrelationID, which would have put a property on the
        // tracker queue that its consumers have never seen. Kept as an
        // extension point for a future binding with a different contract.
    }

    public TrackerBodyMode getBodyMode() {
        return bodyMode;
    }

    public TrackerFields getTrackerFields() {
        return trackerFields;
    }

    /**
     * Configuration for tracker fields injected into MessageHeaderDetails.
     * Maps to tracker_fields in binding config.
     */
    public static class TrackerFields {
        private final String reportingSystem;
        private final String sourceSystem;
        private final String messageStatus;
        private final String destinationStatus;

        public TrackerFields(String reportingSystem, String sourceSystem,
                              String messageStatus, String destinationStatus) {
            this.reportingSystem = reportingSystem;
            this.sourceSystem = sourceSystem;
            this.messageStatus = messageStatus;
            this.destinationStatus = destinationStatus;
        }

        /**
         * Creates default RMS tracker fields matching §20.1.
         */
        public static TrackerFields defaultRms() {
            return new TrackerFields("DMIH/DL", "IIB", "RCVD", "");
        }

        public String getReportingSystem() { return reportingSystem; }
        public String getSourceSystem() { return sourceSystem; }
        public String getMessageStatus() { return messageStatus; }
        public String getDestinationStatus() { return destinationStatus; }
    }

}