package com.hcsc.datalake.mqintake.core.audit;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

/**
 * Emits audit records to HDFS as JSON lines.
 *
 * <p>From DESIGN.md §12: Audit records are written to an HDFS audit path.
 * The format is one JSON object per line (JSONL), which allows for efficient
 * appending and simple parsing.
 *
 * <p>Audit files are organized by binding and date:
 * {auditBasePath}/{bindingId}/audit_{date}.jsonl
 */
public class HdfsAuditRecordEmitter implements AuditRecordEmitter {

    private static final Logger log = LoggerFactory.getLogger(HdfsAuditRecordEmitter.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final FileSystem fileSystem;
    private final String auditBasePath;
    private final AuditRecordBuilder auditRecordBuilder;

    public HdfsAuditRecordEmitter(FileSystem fileSystem, String auditBasePath) {
        this(fileSystem, auditBasePath, "unknown", Clock.systemUTC());
    }

    public HdfsAuditRecordEmitter(FileSystem fileSystem, String auditBasePath,
                                   String instanceId, Clock clock) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.auditBasePath = Objects.requireNonNull(auditBasePath, "auditBasePath required");
        this.auditRecordBuilder = new AuditRecordBuilder(instanceId, clock);
    }

    @Override
    public void emit(AuditRecord record) throws IOException {
        String auditPath = buildAuditPath(record);
        String json = toJson(record);

        Path path = new Path(auditPath);

        // Ensure parent directory exists
        fileSystem.mkdirs(path.getParent());

        // Append to audit file (create if not exists)
        // Handle filesystems that don't support append by reading existing content
        byte[] newLine = (json + "\n").getBytes(StandardCharsets.UTF_8);

        if (fileSystem.exists(path)) {
            try (FSDataOutputStream out = fileSystem.append(path)) {
                out.write(newLine);
                out.hflush();
            } catch (UnsupportedOperationException e) {
                // Filesystem doesn't support append - read and rewrite
                appendByRewrite(path, newLine);
            }
        } else {
            try (FSDataOutputStream out = fileSystem.create(path)) {
                out.write(newLine);
                out.hflush();
            }
        }

        log.debug("Emitted audit record for {}/{}", record.getBindingId(), record.getFilename());
    }

    /**
     * Fallback for filesystems that don't support append.
     */
    private void appendByRewrite(Path path, byte[] newContent) throws IOException {
        // Read existing content
        byte[] existing;
        try (java.io.InputStream in = fileSystem.open(path)) {
            existing = in.readAllBytes();
        }

        // Write back with new content appended
        try (FSDataOutputStream out = fileSystem.create(path, true)) {
            out.write(existing);
            out.write(newContent);
            out.hflush();
        }
    }

    /**
     * Builds the audit file path for a record.
     */
    private String buildAuditPath(AuditRecord record) {
        String date = DATE_FORMAT.format(
                record.getCommitTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate());
        return String.format("%s/%s/audit_%s.jsonl",
                auditBasePath, record.getBindingId(), date);
    }

    /**
     * Converts an audit record to JSON.
     * Simple implementation without external JSON library.
     */
    private String toJson(AuditRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"binding_id\":").append(jsonString(record.getBindingId())).append(",");
        sb.append("\"partition_path\":").append(jsonString(record.getPartitionPath())).append(",");
        sb.append("\"filename\":").append(jsonString(record.getFilename())).append(",");
        sb.append("\"record_count\":").append(record.getRecordCount()).append(",");
        sb.append("\"byte_count\":").append(record.getByteCount()).append(",");
        sb.append("\"first_identity\":").append(jsonString(record.getFirstIdentity())).append(",");
        sb.append("\"last_identity\":").append(jsonString(record.getLastIdentity())).append(",");
        sb.append("\"instance_id\":").append(jsonString(record.getInstanceId())).append(",");
        sb.append("\"commit_timestamp\":").append(jsonString(record.getCommitTimestamp().toString()));
        sb.append("}");
        return sb.toString();
    }

    private String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") + "\"";
    }

    public String getAuditBasePath() {
        return auditBasePath;
    }

    @Override
    public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult, List<Message> messages) throws IOException {
        AuditRecord record = auditRecordBuilder.build(bindingId, writeResult, messages);
        emit(record);
    }
}
