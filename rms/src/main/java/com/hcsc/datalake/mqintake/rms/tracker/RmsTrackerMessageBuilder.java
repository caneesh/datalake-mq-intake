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
     * <p>Derived from evidence in this class: an empty {@code TAG_LIST} and
     * placeholder {@code processTag} mean the legacy transformation cannot be
     * reproduced exactly.
     */
    public static java.util.List<String> trackerContractGaps() {
        java.util.List<String> gaps = new java.util.ArrayList<>();
        if (HeaderRewriter.TAG_LIST.length == 0) {
            gaps.add("tagList contents (§20.4): actual rewritten tag names not captured");
            gaps.add("setReplacedTagData / getCompleteStartTag / getCompleteEndTag bodies not captured (§20.4)");
        }
        if (!HeaderRewriter.ROOT_END_TAG_VERIFIED) {
            gaps.add("ROOT_END_TAG / ROOT_END_TAG_CHAR values unverified against legacy (§20.4)");
        }
        if (!HeaderRewriter.GOLDEN_MASTER_AVAILABLE) {
            gaps.add("No golden-master before/after MessageHeaderDetails fixture");
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

        // Create the tracker message
        TextMessage trackerMessage = session.createTextMessage(body);

        // Set the rewritten header property
        trackerMessage.setStringProperty(MESSAGE_HEADER_DETAILS, rewrittenHeader);

        // Copy other relevant properties if needed
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
     */
    protected String buildCustomBody(Message sourceMessage) throws JMSException {
        // Default implementation returns empty body
        return "";
    }

    /**
     * Copies relevant properties from source to tracker message.
     * Override to customize property copying.
     */
    protected void copyMessageProperties(Message source, Message target) throws JMSException {
        // Copy JMSCorrelationID if present
        String correlationId = source.getJMSCorrelationID();
        if (correlationId != null) {
            target.setJMSCorrelationID(correlationId);
        }
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

    /**
     * Rewrites MessageHeaderDetails to inject tracker fields.
     *
     * <p>Handles both raw ({@code <tag>}) and XML-escaped ({@code &lt;tag&gt;})
     * tag variants per §20.3.
     *
     * <p>TODO (§20.4): The following details are not yet captured and need
     * to be added before this builder can be used in production:
     * <ul>
     *   <li>Contents of tagList — the actual tag names rewritten</li>
     *   <li>ROOT_END_TAG and ROOT_END_TAG_CHAR values</li>
     *   <li>Bodies of getCompleteStartTag, getCompleteEndTag, setReplacedTagData</li>
     *   <li>Sample MessageHeaderDetails value before and after rewriting</li>
     * </ul>
     */
    static class HeaderRewriter {

        // TODO (§20.4): Replace with actual values from current implementation.
        // Flip ROOT_END_TAG_VERIFIED to true once verified against legacy code.
        static final boolean ROOT_END_TAG_VERIFIED = false;
        private static final String ROOT_END_TAG = "</MessageHeaderDetails>";
        private static final String ROOT_END_TAG_ESCAPED = "&lt;/MessageHeaderDetails&gt;";

        // TODO (§20.4): Flip to true once a real before/after header fixture
        // from the legacy system is checked into the test resources.
        static final boolean GOLDEN_MASTER_AVAILABLE = false;

        // TODO (§20.4): Populate from tagList in current implementation
        static final String[] TAG_LIST = {
            // Placeholder — actual tag names TBD
        };

        /**
         * Rewrites the header to inject tracker fields.
         *
         * @param header the original MessageHeaderDetails value
         * @param fields the tracker fields to inject
         * @return the rewritten header
         */
        String rewrite(String header, TrackerFields fields) {
            if (header == null || header.isEmpty()) {
                return header;
            }

            // Detect whether we're dealing with raw or escaped tags
            boolean isEscaped = isEscapedFormat(header);

            // Build the replacement data string
            String replaceData = buildReplaceData(fields, isEscaped);

            // Process tag replacements
            // TODO (§20.4): Iterate tagList and call setReplacedTagData for each
            String processed = header;
            for (String tag : TAG_LIST) {
                processed = processTag(processed, tag, isEscaped);
            }

            // Splice replacement data before root closing tag
            processed = spliceBeforeRootEnd(processed, replaceData, isEscaped);

            return processed;
        }

        /**
         * Detects whether the header uses escaped XML format.
         */
        private boolean isEscapedFormat(String header) {
            // If we find &lt; before <, it's escaped
            int escapedPos = header.indexOf("&lt;");
            int rawPos = header.indexOf('<');

            if (escapedPos == -1) {
                return false;
            }
            if (rawPos == -1) {
                return true;
            }
            return escapedPos < rawPos;
        }

        /**
         * Builds the replacement data string with tracker fields.
         * TODO (§20.4): Match exact format from buildResultData
         */
        private String buildReplaceData(TrackerFields fields, boolean escaped) {
            StringBuilder sb = new StringBuilder();

            if (escaped) {
                // Escaped format
                sb.append("&lt;ReportingSystem&gt;").append(fields.getReportingSystem())
                  .append("&lt;/ReportingSystem&gt;");
                sb.append("&lt;SourceSystem&gt;").append(fields.getSourceSystem())
                  .append("&lt;/SourceSystem&gt;");
                sb.append("&lt;MessageStatus&gt;").append(fields.getMessageStatus())
                  .append("&lt;/MessageStatus&gt;");
                if (fields.getDestinationStatus() != null && !fields.getDestinationStatus().isEmpty()) {
                    sb.append("&lt;DestinationStatus&gt;").append(fields.getDestinationStatus())
                      .append("&lt;/DestinationStatus&gt;");
                }
            } else {
                // Raw format
                sb.append("<ReportingSystem>").append(fields.getReportingSystem())
                  .append("</ReportingSystem>");
                sb.append("<SourceSystem>").append(fields.getSourceSystem())
                  .append("</SourceSystem>");
                sb.append("<MessageStatus>").append(fields.getMessageStatus())
                  .append("</MessageStatus>");
                if (fields.getDestinationStatus() != null && !fields.getDestinationStatus().isEmpty()) {
                    sb.append("<DestinationStatus>").append(fields.getDestinationStatus())
                      .append("</DestinationStatus>");
                }
            }

            return sb.toString();
        }

        /**
         * Processes a single tag from tagList.
         * TODO (§20.4): Implement actual tag processing from setReplacedTagData
         */
        private String processTag(String header, String tagName, boolean escaped) {
            // Placeholder — tag processing logic TBD
            return header;
        }

        /**
         * Splices replacement data before the root closing tag.
         */
        private String spliceBeforeRootEnd(String header, String replaceData, boolean escaped) {
            String endTag = escaped ? ROOT_END_TAG_ESCAPED : ROOT_END_TAG;
            int endPos = header.lastIndexOf(endTag);

            if (endPos == -1) {
                // Root end tag not found — append at end (defensive)
                log.warn("Root end tag not found in header, appending data at end");
                return header + replaceData;
            }

            return header.substring(0, endPos) + replaceData + header.substring(endPos);
        }
    }
}
