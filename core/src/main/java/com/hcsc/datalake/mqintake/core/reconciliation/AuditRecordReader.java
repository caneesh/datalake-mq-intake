package com.hcsc.datalake.mqintake.core.reconciliation;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.hcsc.datalake.mqintake.core.util.JsonFields;

/**
 * Reads immutable audit records written by HdfsAuditRecordEmitter.
 *
 * <p>Audit layout: {auditBasePath}/{bindingId}/{yyyyMMdd}/audit_*.json —
 * one JSON object per file, one file per committed batch.
 */
public class AuditRecordReader {

    private static final Logger log = LoggerFactory.getLogger(AuditRecordReader.class);


    private final FileSystem fileSystem;
    private final String auditBasePath;

    public AuditRecordReader(FileSystem fileSystem, String auditBasePath) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.auditBasePath = Objects.requireNonNull(auditBasePath, "auditBasePath required");
    }

    /**
     * Reads all audit records for a binding on the given date.
     * A missing date directory yields an empty list (no batches committed).
     */
    /**
     * Reads a day's audit records, reporting what it could not read.
     *
     * <p>Returns an {@link AuditScan} rather than a bare list so a caller
     * cannot conclude anything from a scan that was incomplete. Skipping an
     * unreadable record is right — see the catch below — but a control that
     * skips silently is worse than one that stops: with a corrupt audit record
     * whose data file is also missing, both sides vanish from the comparison
     * and the partition reconciles CLEAN.
     *
     * <p>A single method rather than a records-only convenience alongside it,
     * deliberately: an overload that drops the failure signal is exactly how
     * this would come back.
     */
    public AuditScan readForDate(String bindingId, LocalDate date) throws IOException {
        Path dateDir = new Path(
                com.hcsc.datalake.mqintake.core.audit.AuditPaths.dateDir(
                        auditBasePath, bindingId, date));

        List<ParsedAuditRecord> records = new ArrayList<>();
        List<String> unreadable = new ArrayList<>();
        if (!fileSystem.exists(dateDir)) {
            return new AuditScan(records, unreadable);
        }

        for (FileStatus status : fileSystem.listStatus(dateDir)) {
            if (!status.isFile() || !status.getPath().getName().startsWith("audit_")) {
                continue;
            }
            try {
                ParsedAuditRecord record = parse(status.getPath());
                if (record != null) {
                    records.add(record);
                } else {
                    // parse() returns null for a malformed record — a missing
                    // filename, an unusable record_count — and that is the
                    // COMMON shape. An IOException here means the file could
                    // not be read at all, which is rarer. Both leave the scan
                    // incomplete, so both are reported; counting only the
                    // exception would have left the usual case silent.
                    unreadable.add(status.getPath().toString());
                }
            } catch (IOException | RuntimeException e) {
                // Catching RuntimeException is deliberate. This used to catch
                // IOException only, so one corrupt file (an unparseable
                // record_count, for instance) escaped the loop and aborted the
                // ENTIRE binding's reconciliation pass — every window, every
                // run, indefinitely, since the bad file never goes away by
                // itself. A control must skip the record it cannot read and
                // keep checking everything else.
                log.warn("Failed to read audit record {} — skipping it, continuing with the "
                        + "rest: {}", status.getPath(), e.getMessage());
                unreadable.add(status.getPath().toString());
            }
        }

        return new AuditScan(records, unreadable);
    }

    /**
     * What one day's audit directory yielded, including what it did not.
     *
     * <p>The unreadable paths are carried rather than a count because the
     * operator's next action is to look at the file, and a count does not say
     * which one.
     */
    public static final class AuditScan {

        private final List<ParsedAuditRecord> records;
        private final List<String> unreadable;

        AuditScan(List<ParsedAuditRecord> records, List<String> unreadable) {
            this.records = List.copyOf(records);
            this.unreadable = List.copyOf(unreadable);
        }

        /** The records that parsed. Never null, possibly empty. */
        public List<ParsedAuditRecord> records() {
            return records;
        }

        /** Paths of audit records present but unreadable or malformed. */
        public List<String> unreadable() {
            return unreadable;
        }

        /** False when at least one record could not be read. */
        public boolean isComplete() {
            return unreadable.isEmpty();
        }
    }

    private ParsedAuditRecord parse(Path path) throws IOException {
        String json;
        try (InputStream in = fileSystem.open(path)) {
            json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        // Escape-aware extraction: the emitter escapes quotes/backslashes and
        // u-escape-encodes control characters. The previous regex readers
        // ([^"]*) truncated any value at an escaped quote, which misread a
        // correctly audited file as an unaudited orphan.
        String filename = JsonFields.stringField(json, "filename");
        String partitionPath = JsonFields.stringField(json, "partition_path");
        long count = JsonFields.longField(json, "record_count", -1);

        if (filename == null || count < 0 || count > Integer.MAX_VALUE) {
            log.warn("Audit record {} is missing or has an unusable filename/record_count — "
                    + "skipping", path);
            return null;
        }

        return new ParsedAuditRecord(filename, partitionPath, (int) count, path.toString());
    }


    /**
     * The subset of audit record fields reconciliation needs.
     */
    public static class ParsedAuditRecord {
        private final String filename;
        private final String partitionPath;
        private final int recordCount;
        private final String auditFilePath;

        public ParsedAuditRecord(String filename, String partitionPath,
                                 int recordCount, String auditFilePath) {
            this.filename = filename;
            this.partitionPath = partitionPath;
            this.recordCount = recordCount;
            this.auditFilePath = auditFilePath;
        }

        public String getFilename() { return filename; }
        public String getPartitionPath() { return partitionPath; }
        public int getRecordCount() { return recordCount; }
        public String getAuditFilePath() { return auditFilePath; }
    }
}
