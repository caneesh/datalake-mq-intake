package com.hcsc.datalake.mqintake.core.failure;

/**
 * Classification of failures for degraded-mode decisions.
 *
 * <p>From DESIGN.md §6.2: The taxonomy must be explicit and testable,
 * not inferred at the catch site.
 *
 * <p>CRITICAL: Only MESSAGE_DATA triggers degraded mode. UNKNOWN must
 * NEVER trigger degraded mode — roll back and alert instead.
 */
public enum FailureClass {

    /**
     * Message/data failure: serializer failure, malformed payload, unparseable field.
     * Triggers degraded mode: YES.
     */
    MESSAGE_DATA(true, AlertLevel.ON_DEGRADED_ENTRY),

    /**
     * MQ infrastructure: connection loss, queue manager unavailable, tracker queue full.
     * Triggers degraded mode: NO.
     */
    MQ_INFRASTRUCTURE(false, AlertLevel.IMMEDIATE),

    /**
     * HDFS infrastructure: NameNode failover, DataNode unavailable, write/rename failure.
     * Triggers degraded mode: NO.
     */
    HDFS_INFRASTRUCTURE(false, AlertLevel.IMMEDIATE),

    /**
     * Security/configuration: Kerberos expiry, permission denied, missing _tmp.
     * Triggers degraded mode: NO.
     */
    SECURITY_CONFIG(false, AlertLevel.PAGE),

    /**
     * Shutdown/cancellation: interrupt during drain, bounded-timeout expiry.
     * Triggers degraded mode: NO.
     */
    SHUTDOWN(false, AlertLevel.INFO),

    /**
     * Unknown: anything unclassified.
     * Triggers degraded mode: NEVER — guessing "poison" risks routing valid messages to backout.
     */
    UNKNOWN(false, AlertLevel.PAGE);

    private final boolean triggersDegradedMode;
    private final AlertLevel alertLevel;

    FailureClass(boolean triggersDegradedMode, AlertLevel alertLevel) {
        this.triggersDegradedMode = triggersDegradedMode;
        this.alertLevel = alertLevel;
    }

    /**
     * Returns true if this failure class should trigger degraded mode.
     */
    public boolean triggersDegradedMode() {
        return triggersDegradedMode;
    }

    /**
     * Returns the alert level for this failure class.
     */
    public AlertLevel getAlertLevel() {
        return alertLevel;
    }

    /**
     * Alert severity levels.
     */
    public enum AlertLevel {
        /** Log only, no alert. */
        INFO,
        /** Alert on degraded mode entry. */
        ON_DEGRADED_ENTRY,
        /** Immediate alert. */
        IMMEDIATE,
        /** Immediate alert + page. */
        PAGE
    }
}
