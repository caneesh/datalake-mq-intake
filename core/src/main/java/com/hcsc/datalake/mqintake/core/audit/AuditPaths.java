package com.hcsc.datalake.mqintake.core.audit;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * The audit store's path layout, in one place.
 *
 * <p>{@code {base}/{binding}/{yyyyMMdd}/audit_{datafile-stem}.json} was
 * previously hand-assembled in three files — the emitter, the reader, and the
 * startup validator. Any layout change needed three synchronized edits with no
 * compile-time signal when one was missed.
 */
public final class AuditPaths {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private AuditPaths() {
    }

    /** The per-binding root the startup validator checks for writability. */
    public static String bindingDir(String auditBasePath, String bindingId) {
        return auditBasePath + "/" + bindingId;
    }

    /** The per-day directory the reader lists. */
    public static String dateDir(String auditBasePath, String bindingId, LocalDate date) {
        return bindingDir(auditBasePath, bindingId) + "/" + DATE_FORMAT.format(date);
    }

    /**
     * The binding's pending-partition backlog — partitions reconciliation
     * examined but could not resolve, kept so they are re-examined after they
     * age out of the lookback window and across restarts.
     *
     * <p>Under the binding directory rather than a date directory: it is
     * per binding and rewritten in place, not one file per day.
     */
    public static String pendingFile(String auditBasePath, String bindingId) {
        return bindingDir(auditBasePath, bindingId) + "/_pending-partitions";
    }

    /** The record file the emitter writes. */
    public static String recordFile(String auditBasePath, String bindingId, LocalDate date,
                                    String auditFilename) {
        return dateDir(auditBasePath, bindingId, date) + "/" + auditFilename;
    }
}
