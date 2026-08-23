package com.hcsc.datalake.mqintake.core.serializer;

/**
 * Applies the legacy MDB's payload whitespace normalisation.
 *
 * <p>The MDB passes every message body through {@code processMessage(...)}
 * before writing it, which replaces each {@code \n}, {@code \r} and {@code \t}
 * with a single space. Downstream consumers have therefore only ever seen
 * normalised payloads, so reproducing this exactly is a parity requirement,
 * not a formatting preference.
 *
 * <p>Two details matter and are easy to get wrong:
 * <ul>
 *   <li><strong>Runs are not collapsed.</strong> Each character is replaced
 *       one-for-one, so {@code "\r\n"} becomes two spaces, not one.</li>
 *   <li><strong>There is no trim.</strong> Leading and trailing whitespace
 *       survives as spaces; {@code processMessage} performs no {@code trim()}.</li>
 * </ul>
 *
 * <p>Lives in core rather than in each binding module because the MDB applies
 * it upstream of the per-feed write, so both feeds share it — and duplicating
 * it across {@code rms} and {@code claims} would let the two drift.
 *
 * <p>The transformation is lossy: line structure in the payload is destroyed.
 * That is the existing contract.
 */
public final class PayloadNormalizer {

    private PayloadNormalizer() {
        // Static utility
    }

    /**
     * Normalises a payload the way the legacy MDB does.
     *
     * @param payload the raw message body, may be null
     * @return the normalised payload, or null if the input was null
     */
    public static String normalize(String payload) {
        if (payload == null) {
            return null;
        }
        // Written as three literal replacements to mirror processMessage(...)
        // directly, so the two can be compared line for line.
        return payload
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ");
    }
}
