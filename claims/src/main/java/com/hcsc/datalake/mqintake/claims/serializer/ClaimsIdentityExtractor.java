package com.hcsc.datalake.mqintake.claims.serializer;

/**
 * Extracts identity (payload GUID equivalent) from claims messages.
 *
 * <p><strong>OPEN ITEM #17:</strong> The claims identity field is not yet
 * located. RMS has {@code <MessageID>} (UUID); claims has no confirmed
 * equivalent. Candidates:
 * <ul>
 *   <li>{@code CLM_XMITSN_ID}</li>
 *   <li>{@code REC_CTL_NBR}</li>
 *   <li>A wrapper field outside the visible payload</li>
 * </ul>
 *
 * <p>This interface exists so the identity extraction is a single
 * configurable/pluggable point — when the answer is confirmed, it drops
 * in without touching anything else.
 *
 * <p>The identity field is needed for:
 * <ul>
 *   <li>Reconciliation (§12)</li>
 *   <li>Orphan file classification (§10)</li>
 *   <li>Downstream dedup</li>
 * </ul>
 */
@FunctionalInterface
public interface ClaimsIdentityExtractor {

    /**
     * Extracts the identity value from a claims payload.
     *
     * @param payload the XML payload
     * @return the identity value, or null if not found
     */
    String extractIdentity(String payload);

    /**
     * Creates an extractor that looks for a specific XML tag.
     * Handles both raw ({@code <tag>}) and escaped ({@code &lt;tag&gt;}) formats.
     *
     * @param tagName the tag name to extract (e.g., "CLM_XMITSN_ID")
     * @return an extractor for that tag
     */
    static ClaimsIdentityExtractor forTag(String tagName) {
        return new TagBasedExtractor(tagName);
    }

    /**
     * Creates an extractor that tries multiple tags in order,
     * returning the first non-null value found.
     *
     * @param tagNames the tag names to try, in priority order
     * @return an extractor that tries each tag
     */
    static ClaimsIdentityExtractor forTags(String... tagNames) {
        return payload -> {
            for (String tagName : tagNames) {
                String value = new TagBasedExtractor(tagName).extractIdentity(payload);
                if (value != null) {
                    return value;
                }
            }
            return null;
        };
    }

    /**
     * Default extractor that tries the candidate fields from §9.2:
     * CLM_XMITSN_ID first, then REC_CTL_NBR.
     *
     * <p><strong>This is a placeholder</strong> until open item #17 is resolved.
     */
    static ClaimsIdentityExtractor defaultExtractor() {
        return forTags("CLM_XMITSN_ID", "REC_CTL_NBR");
    }
}
