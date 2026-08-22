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
 * Emits audit records to HDFS as JSON.
 *
 * <p>From DESIGN.md §12: Audit records are written to an HDFS audit path.
 * Each audit record is written to its own immutable file — no concurrent
 * appends, no corruption risk.
 *
 * <p>Audit files are organized by binding and date with unique batch IDs:
 * {auditBasePath}/{bindingId}/{date}/audit_{filename}.json
 *
 * <p>The filename is derived from the data file being audited, ensuring a
 * 1:1 correspondence between data files and audit records.
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

        // Write audit record to its own unique, immutable file.
        // Each batch gets its own file - no concurrent appends, no corruption.
        byte[] content = (json + "\n").getBytes(StandardCharsets.UTF_8);

        try (FSDataOutputStream out = fileSystem.create(path, false)) {
            out.write(content);
            out.hflush();
        }

        log.debug("Emitted audit record for {}/{}", record.getBindingId(), record.getFilename());
    }

    /**
     * Builds the audit file path for a record.
     *
     * <p>Each audit record gets its own unique file, named after the data file
     * it audits. This ensures:
     * <ul>
     *   <li>No concurrent writes to the same file</li>
     *   <li>1:1 correspondence between data files and audit records</li>
     *   <li>Immutable audit trail</li>
     * </ul>
     */
    private String buildAuditPath(AuditRecord record) {
        String date = DATE_FORMAT.format(
                record.getCommitTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate());
        // Derive audit filename from the data filename (strip extension, add audit prefix)
        String dataFilename = record.getFilename();
        String auditFilename = "audit_" + stripExtension(dataFilename) + ".json";
        return String.format("%s/%s/%s/%s",
                auditBasePath, record.getBindingId(), date, auditFilename);
    }

    private String stripExtension(String filename) {
        int lastDot = filename.lastIndexOf('.');
        return lastDot > 0 ? filename.substring(0, lastDot) : filename;
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
