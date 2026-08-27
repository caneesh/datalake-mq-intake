package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.AuditRecord;
import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The audit reader — previously the only hand-rolled parser in the repo with
 * no dedicated test, which is where two review findings were hiding.
 *
 * <p>The properties under test are control properties: one unreadable file
 * must never stop the rest from being read, and a value the emitter escaped
 * must come back intact rather than truncated.
 */
class AuditRecordReaderTest {

    private static final Instant COMMIT = Instant.parse("2026-08-26T10:15:00Z");
    private static final LocalDate DATE = LocalDate.of(2026, 8, 26);

    private FileSystem fileSystem;
    private java.nio.file.Path auditDir;
    private HdfsAuditRecordEmitter emitter;
    private AuditRecordReader reader;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);
        auditDir = Files.createTempDirectory("audit-reader");
        emitter = new HdfsAuditRecordEmitter(fileSystem, auditDir.toString(), "it-instance",
                Clock.fixed(COMMIT, ZoneOffset.UTC));
        reader = new AuditRecordReader(fileSystem, auditDir.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.delete(new Path(auditDir.toString()), true);
    }

    @Test
    void readsBackWhatTheEmitterWrote() throws Exception {
        emitter.emit(record("rms_a_1.seq", "/data/raw/rms/year=2026", 42));

        List<AuditRecordReader.ParsedAuditRecord> records = reader.readForDate("rms", DATE);

        assertThat(records).hasSize(1);
        assertThat(records.get(0).getFilename()).isEqualTo("rms_a_1.seq");
        assertThat(records.get(0).getPartitionPath()).isEqualTo("/data/raw/rms/year=2026");
        assertThat(records.get(0).getRecordCount()).isEqualTo(42);
    }

    @Test
    void oneCorruptFileDoesNotAbortTheRestOfThePass() throws Exception {
        // The P0 finding: an unparseable record_count used to throw
        // NumberFormatException past an IOException-only catch, aborting the
        // ENTIRE binding's reconciliation — every window, every run, forever.
        emitter.emit(record("rms_good_1.seq", "/data/p", 5));
        writeRaw("audit_corrupt.json",
                "{\"binding_id\":\"rms\",\"filename\":\"rms_bad.seq\","
                        + "\"record_count\":99999999999999999999,\"partition_path\":\"/data/p\"}\n");
        emitter.emit(record("rms_good_2.seq", "/data/p", 7));

        List<AuditRecordReader.ParsedAuditRecord> records = reader.readForDate("rms", DATE);

        assertThat(records)
                .as("the two healthy records survive the corrupt one")
                .extracting(AuditRecordReader.ParsedAuditRecord::getFilename)
                .containsExactlyInAnyOrder("rms_good_1.seq", "rms_good_2.seq");
    }

    @Test
    void anEmptyFileIsSkippedNotFatal() throws Exception {
        emitter.emit(record("rms_ok.seq", "/data/p", 3));
        writeRaw("audit_empty.json", "");

        assertThat(reader.readForDate("rms", DATE)).hasSize(1);
    }

    @Test
    void aTruncatedFileIsSkippedNotFatal() throws Exception {
        emitter.emit(record("rms_ok.seq", "/data/p", 3));
        writeRaw("audit_truncated.json", "{\"binding_id\":\"rms\",\"filename\":\"cut");

        assertThat(reader.readForDate("rms", DATE)).hasSize(1);
    }

    @Test
    void missingRequiredFieldsAreSkipped() throws Exception {
        writeRaw("audit_nofields.json", "{\"binding_id\":\"rms\"}\n");

        assertThat(reader.readForDate("rms", DATE)).isEmpty();
    }

    @Test
    void escapedQuotesInValuesRoundTripInsteadOfTruncating() throws Exception {
        // The old regex readers stopped at the first escaped quote, so a
        // filename like this came back cut short — and the record then failed
        // partition matching, misreading an audited file as an orphan.
        emitter.emit(record("rms_\"quoted\"_1.seq", "/data/p\"q", 9));

        List<AuditRecordReader.ParsedAuditRecord> records = reader.readForDate("rms", DATE);

        assertThat(records).hasSize(1);
        assertThat(records.get(0).getFilename()).isEqualTo("rms_\"quoted\"_1.seq");
        assertThat(records.get(0).getPartitionPath()).isEqualTo("/data/p\"q");
    }

    @Test
    void missingDateDirectoryReadsAsEmpty() throws Exception {
        assertThat(reader.readForDate("rms", DATE.plusDays(30))).isEmpty();
    }

    // --- helpers ---

    private AuditRecord record(String filename, String partition, int count) {
        return AuditRecord.builder()
                .bindingId("rms")
                .partitionPath(partition)
                .filename(filename)
                .recordCount(count)
                .byteCount(count * 10L)
                .instanceId("it-instance")
                .commitTimestamp(COMMIT)
                .build();
    }

    private void writeRaw(String name, String content) throws Exception {
        java.nio.file.Path dir = auditDir.resolve("rms").resolve("20260826");
        Files.createDirectories(dir);
        Files.write(dir.resolve(name), content.getBytes(StandardCharsets.UTF_8));
    }
}
