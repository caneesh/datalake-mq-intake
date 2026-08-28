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
    private final String instanceId;
    private final java.time.Clock clock;

    public HdfsAuditRecordEmitter(FileSystem fileSystem, String auditBasePath) {
        this(fileSystem, auditBasePath, "unknown", Clock.systemUTC());
    }

    public HdfsAuditRecordEmitter(FileSystem fileSystem, String auditBasePath,
                                   String instanceId, Clock clock) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.auditBasePath = Objects.requireNonNull(auditBasePath, "auditBasePath required");
        this.auditRecordBuilder = new AuditRecordBuilder(instanceId, clock);
        this.instanceId = instanceId;
        this.clock = clock;
    }

    @Override
    public void emit(AuditRecord record) throws IOException {
        String auditPath = buildAuditPath(record);
        String json = toJson(record);

        Path path = new Path(auditPath);

        // Idempotent by name: audit records are immutable and one-per-batch,
        // so an existing target means this exact record was already written.
        // Reconciliation re-classifies a SOLE_COPY orphan on every pass until
        // its retrospective audit is found, and the rename below does not
        // overwrite — without this check the re-emit failed noisily and
        // surfaced as a discrepancy on an otherwise-fine file.
        if (fileSystem.exists(path)) {
            log.debug("Audit record already present, not rewriting: {}", path);
            return;
        }

        byte[] content = (json + "\n").getBytes(StandardCharsets.UTF_8);

        // Staged and renamed rather than written in place. A record streamed
        // straight into its final path is visible while still partial, and a
        // balancing control that can read half a record is not a control — it
        // would report a batch's counts as missing or malformed depending on
        // when it happened to look.
        Path tempPath = new Path(path.getParent(), "." + path.getName() + ".tmp");

        fileSystem.mkdirs(path.getParent());

        boolean landed = false;
        try {
            try (FSDataOutputStream out = fileSystem.create(tempPath, true)) {
                out.write(content);
                // hsync, not hflush: an audit record certifies a commit, so it
                // must not be less durable than the data it accounts for. For
                // batch records a lost audit is reconstructable from the landed
                // file (retrospective audit); a BACKOUT-ONLY record has no data
                // file to reconstruct from — losing it to a correlated crash
                // erases that unit of work from the balance undetectably.
                out.hsync();
            }
            if (!fileSystem.rename(tempPath, path)) {
                throw new IOException(
                        "Failed to rename audit record into place: " + tempPath + " -> " + path);
            }
            landed = true;
        } finally {
            if (!landed) {
                try {
                    fileSystem.delete(tempPath, false);
                } catch (IOException e) {
                    log.debug("Could not remove partial audit record {}: {}",
                            tempPath, e.getMessage());
                }
            }
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
    @Override
    public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                     List<Message> messages, int backoutCount) throws IOException {
        emit(auditRecordBuilder.build(bindingId, writeResult, messages, backoutCount));
    }

    @Override
    public void emitBackoutOnly(String bindingId, List<Message> messages, int backoutCount)
            throws IOException {
        // No data file, so no filename to name the record after. Named for the
        // unit of work instead, so it still lands somewhere a control can find.
        emit(AuditRecord.builder()
                .bindingId(bindingId)
                .partitionPath("")
                .filename("backout-only-" + java.util.UUID.randomUUID())
                .recordCount(0)
                .byteCount(0)
                .backoutCount(backoutCount)
                .instanceId(instanceId)
                .commitTimestamp(java.time.Instant.now(clock))
                .build());
    }

    private String buildAuditPath(AuditRecord record) {
        java.time.LocalDate date =
                record.getCommitTimestamp().atZone(java.time.ZoneOffset.UTC).toLocalDate();
        String auditFilename = "audit_" + stripExtension(record.getFilename()) + ".json";
        return AuditPaths.recordFile(auditBasePath, record.getBindingId(), date, auditFilename);
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
        sb.append("\"backout_count\":").append(record.getBackoutCount()).append(",");
        sb.append("\"consumed_count\":").append(record.getConsumedCount()).append(",");
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
