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
    void anIncompleteAuditScanMustNotAuthoriseQuarantine() throws Exception {
        // Quarantine MOVES a file, and the decision rests on the audit records
        // read for the partition. If one of those could not be parsed, the
        // comparison is missing evidence — and the file it was skipped for
        // then looks unaudited, which is exactly what makes something a
        // quarantine candidate.
        //
        // So a correctly-audited file could be moved because its AUDIT RECORD
        // was corrupt, not because anything was wrong with the file. An
        // unreadable scan already sets retryLater; it must also withhold
        // authority to mutate anything.
        writeSeqFile("audited.seq", "guid-1", "guid-2");
        writeAudit("audited.seq", 2);
        writeSeqFile("orphan.seq", "guid-1", "guid-2");
        writeRawAudit("audit_corrupt.json", "{\"binding_id\":\"rms\",\"filename\":\"cut");

        ReconciliationReport report = reconcile("rms", true, true);   // quarantine ENABLED

        assertThat(fileSystem.exists(new Path(partitionPath(), "orphan.seq")))
                .as("nothing may be moved while the audit scan is incomplete")
                .isTrue();
        assertThat(fileSystem.exists(new Path(basePath + "/_quarantine/orphan.seq"))).isFalse();

        // Still reported, and still retried — withholding the action must not
        // withhold the finding.
        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> assertThat(d.getType())
                        .isEqualTo(DiscrepancyType.UNREADABLE_AUDIT));
        assertThat(report.isRetryLater()).isTrue();
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
                LONG_AFTER.atZone(ZoneOffset.UTC).toLocalDate()).records();
        assertThat(records)
                .anySatisfy(r -> assertThat(r.getFilename()).isEqualTo("solecopy.seq"));
    }

    @Test
    void aCorruptAuditWhoseDataFileIsAlsoMissingMustNotReconcileClean() throws Exception {
        // Both sides of the comparison disappear: the audit record is skipped
        // because it cannot be parsed, and its data file is not on disk, so
        // nothing is left to be missing. Before the scan reported what it
        // could not read, this returned zero discrepancies and
        // retryLater=false — a clean verdict over a partition nobody could
        // actually check.
        writeRawAudit("audit_corrupt.json", "{\"binding_id\":\"rms\",\"filename\":\"cut");

        var report = reconcile("rms", true, false);

        assertThat(report.getDiscrepancies())
                .as("an unreadable audit record is itself the finding")
                .isNotEmpty();
        assertThat(report.getDiscrepancies())
                .anySatisfy(d -> assertThat(d.getType())
                        .isEqualTo(PartitionReconciliationService.DiscrepancyType.UNREADABLE_AUDIT));
        assertThat(report.isRetryLater()).isTrue();
    }

    @Test
    void anUnreadableAuditNamesTheFileSoItCanBeLookedAt() throws Exception {
        writeRawAudit("audit_broken.json", "not json at all");

        var report = reconcile("rms", true, false);

        assertThat(report.getDiscrepancies())
                .filteredOn(d -> d.getType()
                        == PartitionReconciliationService.DiscrepancyType.UNREADABLE_AUDIT)
                .singleElement()
                .satisfies(d -> assertThat(d.getFilename()).contains("audit_broken.json"));
    }

    @Test
    void readableRecordsAreStillCheckedAlongsideACorruptOne() throws Exception {
        // The skip must stay a skip: one corrupt record used to abort the whole
        // binding's pass. Everything else is still reconciled — the corrupt one
        // is reported in addition, not instead.
        writeSeqFile("file1.seq", "guid-1", "guid-2");
        writeAudit("file1.seq", 2);
        writeRawAudit("audit_corrupt.json", "{\"binding_id\":\"rms\",\"filename\":\"cut");

        var report = reconcile("rms", true, false);

        assertThat(report.getDiscrepancies())
                .extracting(d -> d.getType())
                .containsOnly(PartitionReconciliationService.DiscrepancyType.UNREADABLE_AUDIT);
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

    /**
     * Writes a fixture in the ABANDONED Option A layout — {@code Text} keys
     * carrying {@code payload_guid=…}, {@code BytesWritable} values.
     *
     * <p><strong>This is not the production layout</strong>, which is a
     * {@code LongWritable} byte offset and a {@code Text} payload, pinned by
     * {@code ProductionLayoutFingerprintTest} at header length 129. It is kept
     * only because {@code SequenceFileIdentityReader} parses identity out of
     * the KEY, so it is the sole layout in which these identity-classification
     * tests can exercise anything at all.
     *
     * <p>The consequence, which is the point of this comment: every assertion
     * in this class about identity — SOLE_COPY, DUPLICATE, quarantine — is
     * proven against a file format production does not write. Those paths have
     * no production-layout coverage until identity can be read from the
     * payload rather than the key. {@code ReconciliationFactoryTest} covers
     * the production wiring and layout for the count path, which is the half
     * that has been fixed.
     */
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

    /** Drops an unparseable file into the audit directory the reader scans. */
    private void writeRawAudit(String name, String content) throws Exception {
        java.time.LocalDate date = PARTITION_INSTANT.atZone(ZoneOffset.UTC).toLocalDate();
        org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(
                com.hcsc.datalake.mqintake.core.audit.AuditPaths.dateDir(
                        auditBasePath, "rms", date));
        fileSystem.mkdirs(dir);
        try (org.apache.hadoop.fs.FSDataOutputStream out =
                     fileSystem.create(new org.apache.hadoop.fs.Path(dir, name), true)) {
            out.write(content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
