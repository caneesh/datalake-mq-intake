package com.hcsc.datalake.mqintake.core.mq;

import com.ibm.msg.client.wmq.WMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * How the client reaches the queue manager.
 *
 * <p>An enum rather than a string parsed at connect time. The previous
 * if/else chain silently fell back to CLIENT for anything it did not
 * recognise, so a typo in {@code transport-type} produced a working connection
 * of the wrong kind rather than a complaint — and BINDINGS versus CLIENT is
 * the difference between a local queue manager and a network hop.
 */
public enum MqTransportType {

    /** Network connection over a SVRCONN channel. The default. */
    CLIENT(WMQConstants.WMQ_CM_CLIENT),

    /** In-process attachment to a queue manager on the same host. */
    BINDINGS(WMQConstants.WMQ_CM_BINDINGS);

    private static final Logger log = LoggerFactory.getLogger(MqTransportType.class);

    private final int wmqConstant;

    MqTransportType(int wmqConstant) {
        this.wmqConstant = wmqConstant;
    }

    public int wmqConstant() {
        return wmqConstant;
    }

    /**
     * Resolves configuration text, defaulting to {@link #CLIENT}.
     *
     * <p>An unrecognised value is logged at WARN and treated as CLIENT, which
     * preserves the previous behaviour. It is deliberately not an error: this
     * runs on the connect path, and refusing to start over a transport typo
     * would be a harsher change than this refactor should make on its own.
     */
    public static MqTransportType fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return CLIENT;
        }
        String normalised = value.trim().toUpperCase(Locale.ROOT);
        for (MqTransportType type : values()) {
            if (type.name().equals(normalised)) {
                return type;
            }
        }
        log.warn("Unknown transport type '{}', defaulting to {}", value, CLIENT);
        return CLIENT;
    }
}
