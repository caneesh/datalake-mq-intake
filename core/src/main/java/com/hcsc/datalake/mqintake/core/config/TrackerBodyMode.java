package com.hcsc.datalake.mqintake.core.config;

/**
 * Controls what body content is sent in tracker messages.
 */
public enum TrackerBodyMode {

    /**
     * Verbatim copy of the source payload.
     * Default — bit-compatible with current behaviour.
     */
    FULL_COPY,

    /**
     * Empty or minimal body; rewritten properties only.
     * Use if confirmed the consumer reads only MessageHeaderDetails.
     */
    HEADER_ONLY,

    /**
     * Whatever the binding's TrackerMessageBuilder returns.
     * For future bindings with different contracts.
     */
    CUSTOM
}
