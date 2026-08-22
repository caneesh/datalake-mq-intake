package com.hcsc.datalake.mqintake.claims.serializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for ClaimsIdentityExtractor.
 *
 * <p>Tests the pluggable identity extraction mechanism for claims.
 * The identity field is not yet confirmed (open item #17).
 */
class ClaimsIdentityExtractorTest {

    // --- TagBasedExtractor: raw format ---

    @Test
    void forTag_extractsFromRawFormat() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID");

        String payload = "<Claim><CLM_XMITSN_ID>12345678</CLM_XMITSN_ID><Other>data</Other></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("12345678");
    }

    @Test
    void forTag_extractsFromEscapedFormat() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID");

        String payload = "&lt;Claim&gt;&lt;CLM_XMITSN_ID&gt;87654321&lt;/CLM_XMITSN_ID&gt;&lt;/Claim&gt;";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("87654321");
    }

    @Test
    void forTag_returnsNullWhenTagNotFound() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID");

        String payload = "<Claim><REC_CTL_NBR>999</REC_CTL_NBR></Claim>";

        assertThat(extractor.extractIdentity(payload)).isNull();
    }

    @Test
    void forTag_returnsNullForEmptyValue() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID");

        String payload = "<Claim><CLM_XMITSN_ID></CLM_XMITSN_ID></Claim>";

        assertThat(extractor.extractIdentity(payload)).isNull();
    }

    @Test
    void forTag_returnsNullForNullPayload() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID");

        assertThat(extractor.extractIdentity(null)).isNull();
    }

    // --- forTags: priority-ordered extraction ---

    @Test
    void forTags_extractsFromFirstMatchingTag() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTags(
                "CLM_XMITSN_ID", "REC_CTL_NBR");

        String payload = "<Claim><CLM_XMITSN_ID>first</CLM_XMITSN_ID><REC_CTL_NBR>second</REC_CTL_NBR></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("first");
    }

    @Test
    void forTags_fallsBackToSecondTag() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTags(
                "CLM_XMITSN_ID", "REC_CTL_NBR");

        String payload = "<Claim><REC_CTL_NBR>fallback-value</REC_CTL_NBR></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("fallback-value");
    }

    @Test
    void forTags_skipsEmptyAndUsesNext() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTags(
                "CLM_XMITSN_ID", "REC_CTL_NBR");

        String payload = "<Claim><CLM_XMITSN_ID></CLM_XMITSN_ID><REC_CTL_NBR>non-empty</REC_CTL_NBR></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("non-empty");
    }

    @Test
    void forTags_returnsNullWhenNoneMatch() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTags(
                "CLM_XMITSN_ID", "REC_CTL_NBR");

        String payload = "<Claim><OTHER_FIELD>value</OTHER_FIELD></Claim>";

        assertThat(extractor.extractIdentity(payload)).isNull();
    }

    // --- defaultExtractor ---

    @Test
    void nonProductionFixture_triesClmXmitsnIdFirst() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.nonProductionFixture();

        String payload = "<Claim><CLM_XMITSN_ID>default-test-1</CLM_XMITSN_ID></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("default-test-1");
    }

    @Test
    void nonProductionFixture_fallsBackToRecCtlNbr() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.nonProductionFixture();

        String payload = "<Claim><REC_CTL_NBR>fallback-ctl</REC_CTL_NBR></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("fallback-ctl");
    }

    // --- Custom extractor (functional interface) ---

    @Test
    void customExtractor_worksAsLambda() {
        ClaimsIdentityExtractor extractor = payload -> "custom-identity";

        assertThat(extractor.extractIdentity("<anything>")).isEqualTo("custom-identity");
    }

    @Test
    void customExtractor_canCombineLogic() {
        ClaimsIdentityExtractor extractor = payload -> {
            if (payload.contains("PRIORITY")) {
                return "priority-path";
            }
            return ClaimsIdentityExtractor.forTag("REC_CTL_NBR").extractIdentity(payload);
        };

        assertThat(extractor.extractIdentity("<Claim>PRIORITY</Claim>"))
                .isEqualTo("priority-path");
        assertThat(extractor.extractIdentity("<Claim><REC_CTL_NBR>normal</REC_CTL_NBR></Claim>"))
                .isEqualTo("normal");
    }

    // --- Edge cases ---

    @Test
    void handlesTagWithSpecialRegexChars() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("FIELD.NAME");

        String payload = "<Root><FIELD.NAME>value-with-dot</FIELD.NAME></Root>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("value-with-dot");
    }

    @Test
    void extractsFirstOccurrenceWhenMultiple() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID");

        String payload = "<Root><CLM_XMITSN_ID>first</CLM_XMITSN_ID><CLM_XMITSN_ID>second</CLM_XMITSN_ID></Root>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("first");
    }

    @Test
    void handlesValueWithNumbers() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("REC_CTL_NBR");

        String payload = "<Claim><REC_CTL_NBR>0012345678901234567890</REC_CTL_NBR></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("0012345678901234567890");
    }

    @Test
    void handlesValueWithHyphens() {
        ClaimsIdentityExtractor extractor = ClaimsIdentityExtractor.forTag("CLM_XMITSN_ID");

        String payload = "<Claim><CLM_XMITSN_ID>ABC-123-XYZ</CLM_XMITSN_ID></Claim>";

        assertThat(extractor.extractIdentity(payload)).isEqualTo("ABC-123-XYZ");
    }
}
