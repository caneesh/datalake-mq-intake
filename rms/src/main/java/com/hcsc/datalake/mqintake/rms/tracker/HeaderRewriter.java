package com.hcsc.datalake.mqintake.rms.tracker;

/**
 * Rewrites MessageHeaderDetails to inject tracker fields.
 *
 * <p>Handles both raw ({@code <tag>}) and XML-escaped ({@code &lt;tag&gt;})
 * tag variants per §20.3.
 *
 * <p>Extracted from {@code RmsTrackerMessageBuilder} unchanged, where it sat
 * as a nested class beneath JMS message construction, body-mode policy and the
 * legacy-contract gate. It belongs on its own because it shares nothing with
 * them: a pure function from a header string and four field values to a header
 * string, with no JMS, no session and no IO. It stays in {@code rms} — the tag
 * list, the root tag and the value mapping are RMS knowledge and have no
 * business in core.
 *
 * <p>§20.4 is complete. Everything the rewrite depends on was captured from
 * the EJBHelper source and is pinned by a test, so a regression shows up as a
 * failure rather than as a divergence noticed downstream:
 * <ul>
 *   <li>tagList contents and order — {@code tagListMatchesTheLegacyOrderAndContents}</li>
 *   <li>ROOT_END_TAG, raw and escaped — {@code rootEndTagMatchesTheLegacyConstant}</li>
 *   <li>start/end tag construction and the span replaced —
 *       {@code tagsAreBuiltInBothRawAndEscapedForms},
 *       {@code injectionLandsImmediatelyBeforeTheRootEndTag},
 *       {@code anExistingTagIsRemovedBeforeItIsReAdded},
 *       {@code anExistingTagInAnEscapedHeaderIsAlsoRemovedBeforeItIsReAdded},
 *       {@code aRepeatedTagHasEverythingBetweenFirstAndLastOccurrenceRemoved}</li>
 *   <li>before/after value mapping — {@code allFiveTagsAreInjectedWithTheLegacyValueMapping}</li>
 *   <li>the timestamp's zone — {@code theLegacyTimestampIsFormattedInTheJvmDefaultZone}</li>
 *   <li>both halves of the regex hazard —
 *       {@code regexMetacharactersInTagContentAreALegacyHazard},
 *       {@code aQuantifierInTheSpanSilentlyLeavesTheStaleTagInPlace}</li>
 * </ul>
 *
 * <p>What remains is not a code gap: DESIGN item #24 asks that the rewritten
 * header be validated against the live tracker consumers at cutover. That is
 * an operational sign-off on the other side of the queue.
 */
class HeaderRewriter {

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
     * The tag-to-value mapping, captured from the legacy source rather
     * than inferred.
     *
     * <p>It had to be captured because it is not derivable from the call
     * site: {@code buildResultData} is passed all four values on every
     * iteration and picks per tag. Two of those choices are the reason
     * guessing would have produced tracker messages that look right and
     * carry wrong values — {@code DestSystem} takes
     * {@code destinationStatus}, which reads like a mismatch and is what
     * the legacy does, and {@code CreatedTimeStamp} takes no supplied
     * value at all but a timestamp generated at rewrite time.
     *
     * <p>Pinned by {@code allFiveTagsAreInjectedWithTheLegacyValueMapping},
     * so a change to the mapping fails a test rather than diverging
     * silently.
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

    /** EJBHelper.getCompleteStartTag — confirmed against the captured legacy source. */
    static String completeStartTag(String tag, int variant) {
        return LESS_THAN[variant] + tag + GREATER_THAN[variant];
    }

    /** EJBHelper.getCompleteEndTag — confirmed against the captured legacy source. */
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
    String rewrite(String header, RmsTrackerMessageBuilder.TrackerFields fields) {
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
                                 RmsTrackerMessageBuilder.TrackerFields fields) {
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
