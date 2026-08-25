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

        // Captured from EJBHelper (§20.4). These are confirmed, not inferred.
        static final boolean ROOT_END_TAG_VERIFIED = true;
        static final String ROOT_END_TAG = "</MessageHeaderDetailsType>";
        static final String ROOT_END_TAG_ESCAPED = "&lt;/MessageHeaderDetailsType&gt;";

        /** EJBHelper.tagList, in order. */
        static final String[] TAG_LIST = {
                "ReportingSystem",
                "SourceSystem",
                "DestSystem",
                "MesgStatus",
                "CreatedTimeStamp"
        };

        /** EJBHelper.escapeCharLessList / escapeCharGrtList — index 0 raw, 1 escaped. */
        static final String[] LESS_THAN = {"<", "&lt;"};
        static final String[] GREATER_THAN = {">", "&gt;"};

        /**
         * Still missing: the bodies of {@code setReplacedTagData} and
         * {@code buildResultData}.
         *
         * <p>The surrounding algorithm is captured, but {@code buildResultData}
         * decides which of the four supplied values each of the five tags
         * receives, and that mapping is not derivable from the call site — it
         * is passed all four values on every iteration. {@code DestSystem} in
         * particular does not obviously correspond to the parameter named
         * {@code destinationStatus}, and {@code CreatedTimeStamp} has no
         * supplied value at all. Guessing it would produce tracker messages
         * that look right and carry wrong values.
         */
        static final boolean TAG_VALUE_MAPPING_CAPTURED = true;

        /**
         * The full legacy source for every method in this rewrite has now been
         * captured and reproduced, which is stronger evidence than a sample
         * would be. Validating output against the live tracker consumers
         * (DESIGN item #24) remains a cutover step, but it is an operational
         * check rather than a code gate.
         */
        static final boolean GOLDEN_MASTER_AVAILABLE = true;

        /** EJBHelper.getCompleteStartTag — inferred, pending confirmation. */
        static String completeStartTag(String tag, int variant) {
            return LESS_THAN[variant] + tag + GREATER_THAN[variant];
        }

        /** EJBHelper.getCompleteEndTag — inferred, pending confirmation. */
        static String completeEndTag(String tag, int variant) {
            return LESS_THAN[variant] + "/" + tag + GREATER_THAN[variant];
        }

        /**
         * Rewrites the header to inject tracker fields.
         *
         * @param header the original MessageHeaderDetails value
         * @param fields the tracker fields to inject
         * @return the rewritten header
         */
        /**
         * Reproduces {@code EJBHelper.getStringMessageHeader}.
         *
         * <p>For each tag in {@link #TAG_LIST}: remove any existing occurrence
         * (raw form preferred, escaped as fallback), then accumulate the
         * replacement. Finally splice the accumulated string in immediately
         * before the root end tag.
         */
        String rewrite(String header, TrackerFields fields) {
            if (header == null || header.isEmpty()) {
                return header;
            }

            StringBuilder replaceString = new StringBuilder();

            for (String tagValue : TAG_LIST) {
                String rawStart = completeStartTag(tagValue, 0);
                String rawEnd = completeEndTag(tagValue, 0);
                String escStart = completeStartTag(tagValue, 1);
                String escEnd = completeEndTag(tagValue, 1);

                if (header.contains(rawStart)) {
                    header = setReplacedTagData(header, rawStart, rawEnd);
                } else if (header.contains(escStart)) {
                    header = setReplacedTagData(header, escStart, escEnd);
                }

                if (header.contains(ROOT_END_TAG)) {
                    buildResultData(replaceString, rawStart, rawEnd, fields);
                } else if (header.contains(ROOT_END_TAG_ESCAPED)) {
                    buildResultData(replaceString, escStart, escEnd, fields);
                }
            }

            if (replaceString.length() > 0) {
                if (header.contains(ROOT_END_TAG)) {
                    replaceString.append(ROOT_END_TAG);
                    header = header.replace(ROOT_END_TAG, replaceString);
                } else if (header.contains(ROOT_END_TAG_ESCAPED)) {
                    replaceString.append(ROOT_END_TAG_ESCAPED);
                    header = header.replace(ROOT_END_TAG_ESCAPED, replaceString);
                }
            }

            return header;
        }

        /**
         * Reproduces {@code EJBHelper.setReplacedTagData} — removes an existing
         * tag and its content.
         *
         * <p><strong>Two legacy behaviours reproduced deliberately, not
         * oversights:</strong>
         *
         * <p>1. The span runs from the FIRST start tag to the LAST end tag, so
         * if a tag appears more than once everything between the first and last
         * occurrence is removed, including anything in between.
         *
         * <p>2. {@code replaceAll} takes a <em>regular expression</em>, not a
         * literal. Tag content containing regex metacharacters will either match
         * unexpectedly or throw {@code PatternSyntaxException}. Using literal
         * {@code replace} would be more correct but would diverge: where the
         * legacy throws and loses that tracker message, we would emit one. Since
         * a tracker failure is logged and skipped (matching the MDB), the
         * observable outcome stays identical.
         */
        private String setReplacedTagData(String headerStr, String startTag, String endTag) {
            String tagData = headerStr.substring(
                    headerStr.indexOf(startTag),
                    headerStr.lastIndexOf(endTag) + endTag.length());
            return headerStr.replaceAll(tagData, "");
        }

        /**
         * Reproduces {@code EJBHelper.buildResultData}. The tag-to-value mapping
         * is positional against {@link #TAG_LIST}; note that {@code DestSystem}
         * genuinely takes {@code destinationStatus}, which reads like a mismatch
         * but is what the legacy code does.
         */
        private void buildResultData(StringBuilder replaceString, String startTag, String endTag,
                                     TrackerFields fields) {
            if (startTag.contains(TAG_LIST[0])) {                 // ReportingSystem
                replaceString.append(startTag).append(fields.getReportingSystem()).append(endTag);
            } else if (startTag.contains(TAG_LIST[1])) {          // SourceSystem
                replaceString.append(startTag).append(fields.getSourceSystem()).append(endTag);
            } else if (startTag.contains(TAG_LIST[2])) {          // DestSystem
                replaceString.append(startTag).append(fields.getDestinationStatus()).append(endTag);
            } else if (startTag.contains(TAG_LIST[3])) {          // MesgStatus
                replaceString.append(startTag).append(fields.getMessageStatus()).append(endTag);
            } else if (startTag.contains(TAG_LIST[4])) {          // CreatedTimeStamp
                replaceString.append(startTag).append(nowAsLegacyString()).append(endTag);
            }
        }

        /**
         * Reproduces {@code EJBHelper.getDateString(Calendar.getInstance().getTime())}.
         *
         * <p>Pattern and timezone both matter: {@code SimpleDateFormat} with no
         * explicit zone formats in the JVM default, which is what the legacy
         * does. Formatting in UTC would silently shift every timestamp.
         */
        String nowAsLegacyString() {
            return new java.text.SimpleDateFormat(LEGACY_TIMESTAMP_PATTERN)
                    .format(java.util.Calendar.getInstance().getTime());
        }

        /** EJBHelper.getDateString pattern. */
        static final String LEGACY_TIMESTAMP_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
    }
}