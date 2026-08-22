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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads immutable audit records written by HdfsAuditRecordEmitter.
 *
 * <p>Audit layout: {auditBasePath}/{bindingId}/{yyyyMMdd}/audit_*.json —
 * one JSON object per file, one file per committed batch.
 */
public class AuditRecordReader {

    private static final Logger log = LoggerFactory.getLogger(AuditRecordReader.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final Pattern FILENAME_PATTERN = Pattern.compile("\"filename\":\"([^\"]*)\"");
    private static final Pattern PARTITION_PATTERN = Pattern.compile("\"partition_path\":\"([^\"]*)\"");
    private static final Pattern RECORD_COUNT_PATTERN = Pattern.compile("\"record_count\":(\\d+)");

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
    public List<ParsedAuditRecord> readForDate(String bindingId, LocalDate date) throws IOException {
        Path dateDir = new Path(String.format("%s/%s/%s",
                auditBasePath, bindingId, DATE_FORMAT.format(date)));

        List<ParsedAuditRecord> records = new ArrayList<>();
        if (!fileSystem.exists(dateDir)) {
            return records;
        }

        for (FileStatus status : fileSystem.listStatus(dateDir)) {
            if (!status.isFile() || !status.getPath().getName().startsWith("audit_")) {
                continue;
            }
            try {
                ParsedAuditRecord record = parse(status.getPath());
                if (record != null) {
                    records.add(record);
                }
            } catch (IOException e) {
                log.warn("Failed to read audit record {}: {}", status.getPath(), e.getMessage());
            }
        }

        return records;
    }

    private ParsedAuditRecord parse(Path path) throws IOException {
        String json;
        try (InputStream in = fileSystem.open(path)) {
            json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        String filename = firstGroup(FILENAME_PATTERN, json);
        String partitionPath = firstGroup(PARTITION_PATTERN, json);
        String countStr = firstGroup(RECORD_COUNT_PATTERN, json);

        if (filename == null || countStr == null) {
            log.warn("Audit record {} is missing filename or record_count — skipping", path);
            return null;
        }

        return new ParsedAuditRecord(filename, partitionPath, Integer.parseInt(countStr),
                path.toString());
    }

    private String firstGroup(Pattern pattern, String input) {
        Matcher m = pattern.matcher(input);
        return m.find() ? m.group(1) : null;
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
