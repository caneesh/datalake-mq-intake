package com.hcsc.datalake.mqintake.claims.serializer;

/**
 * Extracts identity (payload GUID equivalent) from claims messages.
 *
 * <p><strong>OPEN ITEM #17:</strong> The claims identity field is not yet
 * confirmed by the source system. RMS has {@code <MessageID>} (UUID); claims
 * has no confirmed equivalent. Candidates:
 * <ul>
 *   <li>{@code CLM_XMITSN_ID}</li>
 *   <li>{@code REC_CTL_NBR}</li>
 *   <li>A wrapper field outside the visible payload</li>
 * </ul>
 *
 * <p><strong>The identity field must be configured explicitly.</strong>
 * There is no silent default: production startup fails when no identity
 * field is configured (see {@code ClaimsConfiguration}). Test/dev
 * environments must opt in to {@link #nonProductionFixture()} knowingly.
 *
 * <p>The identity field is needed for:
 * <ul>
 *   <li>Reconciliation (§12)</li>
 *   <li>Orphan file classification (§10)</li>
 *   <li>Downstream dedup</li>
 * </ul>
 *
 * <p>Note: {@code mq_message_id} is transport trace metadata only. It changes
 * on re-put, so it must never be used as the primary stable identity.
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
     * Fixture extractor for TEST/DEV ONLY. Tries the §9.2 candidate fields:
     * CLM_XMITSN_ID first, then REC_CTL_NBR.
     *
     * <p><strong>NEVER valid in production.</strong> Production requires an
     * explicitly configured identity field; startup is gated on it. This
     * fixture exists so test environments can exercise the pipeline while
     * open item #17 is unresolved.
     */
    static ClaimsIdentityExtractor nonProductionFixture() {
        return forTags("CLM_XMITSN_ID", "REC_CTL_NBR");
    }
}
