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

    /** The record file the emitter writes. */
    public static String recordFile(String auditBasePath, String bindingId, LocalDate date,
                                    String auditFilename) {
        return dateDir(auditBasePath, bindingId, date) + "/" + auditFilename;
    }
}
