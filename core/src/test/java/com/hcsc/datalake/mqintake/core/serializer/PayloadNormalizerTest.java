package com.hcsc.datalake.mqintake.core.serializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Pins the legacy MDB's payload normalisation semantics.
 *
 * <p>These assertions exist to stop a well-meaning "cleanup" — collapsing
 * whitespace runs, or adding a trim — from silently changing bytes that
 * downstream consumers already depend on.
 */
class PayloadNormalizerTest {

    @Test
    void replacesEachWhitespaceCharacterWithASingleSpace() {
        assertThat(PayloadNormalizer.normalize("a\nb")).isEqualTo("a b");
        assertThat(PayloadNormalizer.normalize("a\rb")).isEqualTo("a b");
        assertThat(PayloadNormalizer.normalize("a\tb")).isEqualTo("a b");
    }

    @Test
    void doesNotCollapseRuns() {
        // CRLF is two characters, so it becomes two spaces — not one.
        assertThat(PayloadNormalizer.normalize("a\r\nb")).isEqualTo("a  b");
        assertThat(PayloadNormalizer.normalize("a\n\n\nb")).isEqualTo("a   b");
        assertThat(PayloadNormalizer.normalize("a\t\tb")).isEqualTo("a  b");
    }

    @Test
    void doesNotTrim() {
        // processMessage performs no trim(); leading and trailing whitespace
        // survives as spaces.
        assertThat(PayloadNormalizer.normalize("\nabc\n")).isEqualTo(" abc ");
        assertThat(PayloadNormalizer.normalize("\t abc \t")).isEqualTo("  abc  ");
    }

    @Test
    void leavesExistingSpacesAndOtherCharactersAlone() {
        assertThat(PayloadNormalizer.normalize("a b  c")).isEqualTo("a b  c");
        // Vertical tab and form feed are NOT in the MDB's replacement set
        assertThat(PayloadNormalizer.normalize("ab\fc")).isEqualTo("ab\fc");
    }

    @Test
    void preservesUnicodeAndPayloadContent() {
        assertThat(PayloadNormalizer.normalize("<Name>日本語</Name>"))
                .isEqualTo("<Name>日本語</Name>");
    }

    @Test
    void handlesNullAndEmpty() {
        assertThat(PayloadNormalizer.normalize(null)).isNull();
        assertThat(PayloadNormalizer.normalize("")).isEmpty();
    }

    @Test
    void isIdempotent() {
        // Normalising an already-normalised payload must not change it again,
        // which matters if a payload is ever reprocessed on replay.
        String once = PayloadNormalizer.normalize("<A>\r\n\tvalue\n</A>");
        assertThat(PayloadNormalizer.normalize(once)).isEqualTo(once);
    }

    @Test
    void normalisedPayloadContainsNoLineStructure() {
        String normalised = PayloadNormalizer.normalize("<A>1</A>\n<B>2</B>\r\n<C>3</C>");

        assertThat(normalised).doesNotContain("\n").doesNotContain("\r").doesNotContain("\t");
        assertThat(normalised).contains("<A>1</A>").contains("<B>2</B>").contains("<C>3</C>");
    }
}
