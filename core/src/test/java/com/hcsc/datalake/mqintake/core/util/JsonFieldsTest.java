package com.hcsc.datalake.mqintake.core.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The one shared field parser both hand-rolled JSON readers now use. */
class JsonFieldsTest {

    @Test
    void extractsPlainStringAndNumericFields() {
        String line = "{\"name\":\"rms_1.seq\",\"count\":42,\"path\":\"/data/p\"}";

        assertThat(JsonFields.stringField(line, "name")).isEqualTo("rms_1.seq");
        assertThat(JsonFields.stringField(line, "path")).isEqualTo("/data/p");
        assertThat(JsonFields.longField(line, "count", -1)).isEqualTo(42);
    }

    @Test
    void escapedQuotesAndBackslashesDecode() {
        // The regex readers this replaces truncated at the escaped quote.
        String line = "{\"v\":\"a\\\"b\\\\c\"}";

        assertThat(JsonFields.stringField(line, "v")).isEqualTo("a\"b\\c");
    }

    @Test
    void namedEscapesDecode() {
        String line = "{\"v\":\"x\\ny\\tz\\rw\"}";

        assertThat(JsonFields.stringField(line, "v")).isEqualTo("x\ny\tz\rw");
    }

    @Test
    void unicodeEscapesDecodeInsteadOfCorrupting() {
        // The index reader's old switch appended the literal 'u' and digits,
        // so an identity containing U+0001 came back as "u0001...".
        String line = "{\"v\":\"a\\u0001b\"}";

        assertThat(JsonFields.stringField(line, "v")).isEqualTo("ab");
    }

    @Test
    void malformedUnicodeEscapeKeepsTheLiteralRatherThanThrowing() {
        // Not a valid escape: the literal characters are kept rather than an
        // exception thrown — same skip-don't-fail posture as the callers.
        assertThat(JsonFields.stringField("{\"v\":\"a\\uZZZZb\"}", "v")).isEqualTo("auZZZZb");
        assertThat(JsonFields.stringField("{\"v\":\"a\\u12\"}", "v")).isEqualTo("au12");
    }

    @Test
    void unterminatedStringIsNullNotAFragment() {
        // A truncated write must not be mistaken for a shorter value.
        assertThat(JsonFields.stringField("{\"v\":\"cut-off", "v")).isNull();
    }

    @Test
    void absentAndNullFieldsAreNull() {
        assertThat(JsonFields.stringField("{\"other\":\"x\"}", "v")).isNull();
        assertThat(JsonFields.stringField("{\"v\":null}", "v")).isNull();
    }

    @Test
    void corruptNumbersReturnTheFallbackInsteadOfThrowing() {
        // The defect that let one corrupt audit file kill a binding's
        // reconciliation: parseInt on an overlong digit string.
        assertThat(JsonFields.longField("{\"n\":99999999999999999999}", "n", -1)).isEqualTo(-1);
        assertThat(JsonFields.longField("{\"n\":}", "n", -1)).isEqualTo(-1);
        assertThat(JsonFields.longField("{\"n\":\"text\"}", "n", -1)).isEqualTo(-1);
        assertThat(JsonFields.longField("{}", "n", -1)).isEqualTo(-1);
    }

    @Test
    void negativeNumbersParse() {
        assertThat(JsonFields.longField("{\"n\":-5}", "n", 0)).isEqualTo(-5);
    }

    @Test
    void completeObjectDetection() {
        assertThat(JsonFields.isCompleteObject("{\"a\":1}")).isTrue();
        assertThat(JsonFields.isCompleteObject("  {\"a\":1}  ")).isTrue();
        assertThat(JsonFields.isCompleteObject("{\"a\":1")).isFalse();
        assertThat(JsonFields.isCompleteObject("")).isFalse();
    }
}
