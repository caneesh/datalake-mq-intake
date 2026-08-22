package com.hcsc.datalake.mqintake.claims.serializer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts a tag value from XML, handling both raw and escaped formats.
 *
 * <p>Supports:
 * <ul>
 *   <li>Raw format: {@code <TagName>value</TagName>}</li>
 *   <li>Escaped format: {@code &lt;TagName&gt;value&lt;/TagName&gt;}</li>
 * </ul>
 */
class TagBasedExtractor implements ClaimsIdentityExtractor {

    private final Pattern rawPattern;
    private final Pattern escapedPattern;

    TagBasedExtractor(String tagName) {
        this.rawPattern = Pattern.compile(
                "<" + Pattern.quote(tagName) + ">([^<]*)</" + Pattern.quote(tagName) + ">");
        this.escapedPattern = Pattern.compile(
                "&lt;" + Pattern.quote(tagName) + "&gt;([^&]*)&lt;/" + Pattern.quote(tagName) + "&gt;");
    }

    @Override
    public String extractIdentity(String payload) {
        if (payload == null) {
            return null;
        }

        // Try raw format first
        Matcher rawMatcher = rawPattern.matcher(payload);
        if (rawMatcher.find()) {
            String value = rawMatcher.group(1);
            return value.isEmpty() ? null : value;
        }

        // Try escaped format
        Matcher escapedMatcher = escapedPattern.matcher(payload);
        if (escapedMatcher.find()) {
            String value = escapedMatcher.group(1);
            return value.isEmpty() ? null : value;
        }

        return null;
    }
}
