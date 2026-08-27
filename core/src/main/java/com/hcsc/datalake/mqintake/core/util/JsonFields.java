package com.hcsc.datalake.mqintake.core.util;

/**
 * Escape-aware field extraction for this service's own single-line JSON.
 *
 * <p>The audit trail and the sidecar index both emit small hand-rolled JSON,
 * and both were read back by hand-rolled parsers that had quietly diverged:
 * the audit reader used regexes whose {@code [^"]*} groups truncated at any
 * escaped quote (misreading a legitimately audited file as an unaudited
 * orphan), and the index reader's unescape switch dropped {@code \\uXXXX}
 * sequences, corrupting control characters in identities. One shared
 * implementation, tested once, replaces both — the same reasoning that keeps
 * shared machinery in {@code core} rather than duplicated per module.
 *
 * <p>This is deliberately not a JSON parser. It extracts named fields from a
 * single JSON object on one line, matching exactly what the emitters write.
 * Anything structurally beyond that belongs to a real JSON library, and
 * introducing one is a dependency decision, not a bug fix.
 */
public final class JsonFields {

    private JsonFields() {
    }

    /**
     * Extracts a string field's decoded value.
     *
     * @return the unescaped value, or null when the field is absent or null
     */
    public static String stringField(String line, String field) {
        String marker = "\"" + field + "\":";
        int start = line.indexOf(marker);
        if (start < 0) {
            return null;
        }
        int valueStart = start + marker.length();
        if (valueStart >= line.length() || line.startsWith("null", valueStart)) {
            return null;
        }
        if (line.charAt(valueStart) != '"') {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = valueStart + 1; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\\' && i + 1 < line.length()) {
                char next = line.charAt(++i);
                switch (next) {
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        // The writers emit \\u%04x for control characters. The
                        // previous reader appended the literal 'u' and digits,
                        // silently corrupting the value.
                        if (i + 4 < line.length()) {
                            try {
                                sb.append((char) Integer.parseInt(
                                        line.substring(i + 1, i + 5), 16));
                                i += 4;
                            } catch (NumberFormatException e) {
                                sb.append(next);   // not a valid escape; keep literal
                            }
                        } else {
                            sb.append(next);
                        }
                        break;
                    default:
                        sb.append(next);
                }
            } else if (c == '"') {
                return sb.toString();
            } else {
                sb.append(c);
            }
        }
        // Ran off the end of the line without a closing quote: a truncated
        // write. Callers must not mistake the fragment for the value.
        return null;
    }

    /**
     * Extracts a numeric field.
     *
     * <p>Never throws: a corrupted digit string returns the fallback rather
     * than {@code NumberFormatException}. The audit reader previously called
     * {@code Integer.parseInt} unguarded inside a loop whose catch handled
     * only {@code IOException} — one corrupt {@code record_count} aborted the
     * entire binding's reconciliation pass, every run, indefinitely. A control
     * must degrade to "skip this record and say so", never to "stop checking".
     */
    public static long longField(String line, String field, long fallback) {
        String marker = "\"" + field + "\":";
        int start = line.indexOf(marker);
        if (start < 0) {
            return fallback;
        }
        int i = start + marker.length();
        int end = i;
        while (end < line.length()
                && (Character.isDigit(line.charAt(end)) || (end == i && line.charAt(end) == '-'))) {
            end++;
        }
        if (end == i) {
            return fallback;
        }
        try {
            return Long.parseLong(line.substring(i, end));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** True when the line is one complete JSON object, as the writers emit. */
    public static boolean isCompleteObject(String line) {
        String trimmed = line.trim();
        return trimmed.startsWith("{") && trimmed.endsWith("}");
    }
}
