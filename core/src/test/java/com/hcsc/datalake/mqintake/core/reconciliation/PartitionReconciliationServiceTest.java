package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.AuditRecord;
import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.reconciliation.PartitionReconciliationService.DiscrepancyType;
import com.hcsc.datalake.mqintake.core.reconciliation.PartitionReconciliationService.ReconciliationReport;
import com.hcsc.datalake.mqintake.core.reconciliation.PartitionReconciliationService.ReconciliationStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for PartitionReconciliationService (round 2 prompt 6).
 *
 * <p>Maps to DESIGN §12: partition close + grace → enumerate files and audit
 * records → compare → classify orphans by identity set → alert / quarantine
 * (move) / retrospective audit. No auto-delete path exists.
 */
class PartitionReconciliationServiceTest {

    /** An instant safely inside a quarter: 2026-08-20T10:07:00Z → quarter=0. */
    private static final Instant PARTITION_INSTANT = Instant.parse("2026-08-20T10:07:00Z");
    /** Well past partition close (10:15) + any reasonable grace. */
    private static final Instant LONG_AFTER = Instant.parse("2026-08-21T00:00:00Z");
    private static final Duration GRACE = Duration.ofMinutes(30);

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem fileSystem;
    private Configuration conf;
    private String basePath;
    private String auditBasePath;
    private SequenceFileIdentityReader identityReader;
    private HdfsAuditRecordEmitter auditEmitter;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(conf);
        basePath = tempDir.resolve("data").toString();
        auditBasePath = tempDir.resolve("audit").toString();
        identityReader = new SequenceFileIdentityReader(conf);
        auditEmitter = new HdfsAuditRecordEmitter(fileSystem, auditBasePath,
                "inst1", Clock.fixed(PARTITION_INSTANT, ZoneOffset.UTC));
    }

    private PartitionReconciliationService service(Instant now) {
        return new PartitionReconciliationService(
                fileSystem, identityReader,
                new AuditRecordReader(fileSystem, auditBasePath),
                auditEmitter, GRACE,
                Clock.fixed(now, ZoneOffset.UTC), "inst1");
    }

    // --- Scenarios ---

    @Test
    void matchingPartitionIsClean() throws Exception {
        writeSeqFile("file1.seq", "guid-1", "guid-2");
        writeAudit("file1.seq", 2);

        ReconciliationReport report = reconcile("rms", true, false);

        assertThat(report.getStatus()).isEqualTo(ReconciliationStatus.CLEAN);
        assertThat(report.getDiscrepancies()).isEmpty();
        assertThat(report.getFileCount()).isEqualTo(1);
        assertThat(report.getAuditRecordCount()).isEqualTo(1);
        assertThat(report.getActualRecordSum()).isEqualTo(2);
        assertThat(report.getAuditedRecordSum()).isEqualTo(2);
    }

    @Test
    void missingDataFileWithAuditRecordIsLoudestAlert() throws Exception {
        writeAudit("ghost.seq", 5);

        ReconciliationReport report = reconcile("rms", true, false);

        assertThat(report.getStatus()).isEqualTo(ReconciliationStatus.DISCREPANCIES);
        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> {
                    assertThat(d.getType()).isEqualTo(DiscrepancyType.MISSING_FILE);
                    assertThat(d.getFilename()).isEqualTo("ghost.seq");
                });
    }

    @Test
    void countMismatchReported() throws Exception {
        writeSeqFile("file1.seq", "guid-1", "guid-2", "guid-3");
        writeAudit("file1.seq", 5); // audit claims 5, file has 3

        ReconciliationReport report = reconcile("rms", true, false);

        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> {
                    assertThat(d.getType()).isEqualTo(DiscrepancyType.COUNT_MISMATCH);
                    assertThat(d.getDetail()).contains("5").contains("3");
                });
    }

    @Test
    void duplicateOrphanQuarantineIsAMoveNotADelete() throws Exception {
        // Audited file holds guid-1, guid-2; orphan holds the same identities
        writeSeqFile("audited.seq", "guid-1", "guid-2");
        writeAudit("audited.seq", 2);
        writeSeqFile("orphan.seq", "guid-1", "guid-2");

        ReconciliationReport report = reconcile("rms", true, true);

        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> assertThat(d.getType()).isEqualTo(DiscrepancyType.ORPHAN_DUPLICATE));

        // Quarantine = move: gone from partition, intact under _quarantine
        Path partitionFile = new Path(partitionPath(), "orphan.seq");
        Path quarantined = new Path(basePath + "/_quarantine/orphan.seq");
        assertThat(fileSystem.exists(partitionFile)).isFalse();
        assertThat(fileSystem.exists(quarantined)).isTrue();
        // Content preserved — identities still readable from the moved file
        assertThat(identityReader.extractIdentities(quarantined.toString()))
                .containsExactlyInAnyOrder("guid-1", "guid-2");
        // The audited file was never touched
        assertThat(fileSystem.exists(new Path(partitionPath(), "audited.seq"))).isTrue();
    }

    @Test
    void duplicateOrphanWithoutQuarantineFlagIsOnlyReported() throws Exception {
        writeSeqFile("audited.seq", "guid-1");
        writeAudit("audited.seq", 1);
        writeSeqFile("orphan.seq", "guid-1");

        ReconciliationReport report = reconcile("rms", true, false);

        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> {
                    assertThat(d.getType()).isEqualTo(DiscrepancyType.ORPHAN_DUPLICATE);
                    assertThat(d.getDetail()).contains("no action taken");
                });
        assertThat(fileSystem.exists(new Path(partitionPath(), "orphan.seq"))).isTrue();
    }

    @Test
    void soleCopyOrphanIsKeptAndRetrospectivelyAudited() throws Exception {
        writeSeqFile("audited.seq", "guid-1");
        writeAudit("audited.seq", 1);
        // Orphan carries an identity that exists nowhere else — sole copy
        writeSeqFile("solecopy.seq", "guid-1", "guid-UNIQUE");

        ReconciliationReport report = reconcile("rms", true, true);

        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> {
                    assertThat(d.getType()).isEqualTo(DiscrepancyType.ORPHAN_SOLE_COPY);
                    assertThat(d.getDetail()).contains("KEPT")
                            .contains("retrospective audit record emitted");
                });

        // File kept in place even with quarantine enabled
        assertThat(fileSystem.exists(new Path(partitionPath(), "solecopy.seq"))).isTrue();

        // Retrospective audit record exists and reconciles on the next run:
        AuditRecordReader reader = new AuditRecordReader(fileSystem, auditBasePath);
        var records = reader.readForDate("rms",
                LONG_AFTER.atZone(ZoneOffset.UTC).toLocalDate());
        assertThat(records)
                .anySatisfy(r -> assertThat(r.getFilename()).isEqualTo("solecopy.seq"));
    }

    @Test
    void unreadableOrphanIsKeptAndRetriedLater() throws Exception {
        writeSeqFile("audited.seq", "guid-1");
        writeAudit("audited.seq", 1);
        // Garbage bytes with a .seq name — cannot be read as a SequenceFile
        Path garbage = new Path(partitionPath(), "corrupt.seq");
        try (FSDataOutputStream out = fileSystem.create(garbage)) {
            out.write("not a sequence file".getBytes(StandardCharsets.UTF_8));
        }

        ReconciliationReport report = reconcile("rms", true, true);

        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> assertThat(d.getType()).isEqualTo(DiscrepancyType.UNREADABLE_FILE));
        assertThat(report.isRetryLater()).isTrue();
        // Never deleted, never quarantined
        assertThat(fileSystem.exists(garbage)).isTrue();
    }

    @Test
    void gracePeriodIsRespected() throws Exception {
        writeSeqFile("file1.seq", "guid-1");
        // Now = partition close (10:15) + 10 min, grace is 30 min → skip
        Instant insideGrace = Instant.parse("2026-08-20T10:25:00Z");

        ReconciliationReport report = service(insideGrace).reconcilePartition(
                "rms", basePath, PARTITION_INSTANT, true, true, null);

        assertThat(report.getStatus()).isEqualTo(ReconciliationStatus.SKIPPED_GRACE_PERIOD);
        assertThat(report.isRetryLater()).isTrue();
        assertThat(fileSystem.exists(new Path(partitionPath(), "file1.seq"))).isTrue();
    }

    @Test
    void unresolvedIdentityRefusesReadinessAndTouchesNothing() throws Exception {
        writeSeqFile("orphan.seq", "guid-1");

        // Claims: identityApproved=false until open item #17 is resolved
        ReconciliationReport report = reconcile("claims", false, true);

        assertThat(report.getStatus()).isEqualTo(ReconciliationStatus.NOT_READY);
        assertThat(report.getMessage()).contains("identity unresolved");
        assertThat(fileSystem.exists(new Path(partitionPath(), "orphan.seq"))).isTrue();
        assertThat(fileSystem.exists(new Path(basePath + "/_quarantine/orphan.seq"))).isFalse();
    }

    @Test
    void bindingsReconcileIndependently() throws Exception {
        // Binding rms: clean. Binding other: has a missing-file discrepancy.
        writeSeqFile("file1.seq", "guid-1");
        writeAudit("file1.seq", 1);

        AuditRecord otherAudit = AuditRecord.builder()
                .bindingId("other")
                .partitionPath(partitionPath())
                .filename("lost.seq")
                .recordCount(3)
                .byteCount(10)
                .instanceId("inst1")
                .commitTimestamp(PARTITION_INSTANT)
                .build();
        auditEmitter.emit(otherAudit);

        BindingMetrics rmsMetrics = new BindingMetrics("rms");
        BindingMetrics otherMetrics = new BindingMetrics("other");

        ReconciliationReport rmsReport = service(LONG_AFTER).reconcilePartition(
                "rms", basePath, PARTITION_INSTANT, true, false, rmsMetrics);
        ReconciliationReport otherReport = service(LONG_AFTER).reconcilePartition(
                "other", basePath, PARTITION_INSTANT, true, false, otherMetrics);

        // 'other' sees its missing file; but it also sees rms's file1.seq as an
        // orphan within the shared test partition — the key isolation property
        // is that rms's report is unaffected by other's discrepancies
        assertThat(rmsReport.getStatus()).isEqualTo(ReconciliationStatus.CLEAN);
        assertThat(rmsMetrics.getReconciliationDiscrepancyCount()).isZero();
        assertThat(otherReport.getStatus()).isEqualTo(ReconciliationStatus.DISCREPANCIES);
        assertThat(otherMetrics.getReconciliationDiscrepancyCount()).isGreaterThan(0);
    }

    @Test
    void identityReaderParsesPayloadGuidWithMqMessageIdFallback() {
        assertThat(SequenceFileIdentityReader.parseIdentity(
                "binding_id=rms|payload_guid=G1|mq_message_id=M1|x=y")).isEqualTo("G1");
        assertThat(SequenceFileIdentityReader.parseIdentity(
                "binding_id=rms|payload_guid=|mq_message_id=M1|x=y")).isEqualTo("M1");
        assertThat(SequenceFileIdentityReader.parseIdentity(
                "binding_id=rms|payload_guid=|mq_message_id=|x=y")).isNull();
    }

    // --- helpers ---

    private ReconciliationReport reconcile(String bindingId, boolean identityApproved,
                                           boolean quarantine) {
        return service(LONG_AFTER).reconcilePartition(
                bindingId, basePath, PARTITION_INSTANT, identityApproved, quarantine, null);
    }

    private String partitionPath() {
        return PartitionPath.compute(basePath, PARTITION_INSTANT);
    }

    private void writeSeqFile(String filename, String... identities) throws Exception {
        Path dir = new Path(partitionPath());
        fileSystem.mkdirs(dir);
        Path file = new Path(dir, filename);
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(BytesWritable.class))) {
            for (String identity : identities) {
                Text key = new Text("binding_id=rms|payload_guid=" + identity +
                        "|mq_message_id=ID:" + identity + "|consume_ts_utc=" + PARTITION_INSTANT);
                writer.append(key, new BytesWritable(("payload-" + identity)
                        .getBytes(StandardCharsets.UTF_8)));
            }
        }
    }

    private void writeAudit(String filename, int recordCount) throws Exception {
        AuditRecord record = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath(partitionPath())
                .filename(filename)
                .recordCount(recordCount)
                .byteCount(100)
                .firstIdentity("guid-first")
                .lastIdentity("guid-last")
                .instanceId("inst1")
                .commitTimestamp(PARTITION_INSTANT)
                .build();
        auditEmitter.emit(record);
    }
}
