package com.hcsc.datalake.mqintake.core.audit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for HdfsAuditRecordEmitter.
 *
 * <p>Audit files are now immutable per-batch:
 * {auditBasePath}/{bindingId}/{date}/audit_{datafilename}.json
 */
class HdfsAuditRecordEmitterTest {

    private FileSystem fileSystem;
    private String testBasePath;
    private HdfsAuditRecordEmitter emitter;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);

        testBasePath = "/tmp/audit-test-" + System.currentTimeMillis();
        emitter = new HdfsAuditRecordEmitter(fileSystem, testBasePath);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileSystem != null && testBasePath != null) {
            fileSystem.delete(new Path(testBasePath), true);
        }
    }

    @Test
    void auditWriteIsHsyncedNotJustFlushed() throws Exception {
        // An audit record certifies a commit, so it must not be less durable
        // than the data it accounts for. Backout-only records especially:
        // they have no data file to reconstruct a lost record from, so an
        // hflush-only write left a correlated-crash window in which a unit of
        // work vanished from the balance undetectably.
        AtomicBoolean hsynced = new AtomicBoolean(false);
        FileSystem recording = new FilterFileSystem(fileSystem) {
            @Override
            public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
                                             int bufferSize, short replication, long blockSize,
                                             Progressable progress) throws IOException {
                FSDataOutputStream real = super.create(f, permission, overwrite, bufferSize,
                        replication, blockSize, progress);
                return new FSDataOutputStream(new HsyncRecordingStream(real, hsynced), null);
            }
        };
        HdfsAuditRecordEmitter recordingEmitter =
                new HdfsAuditRecordEmitter(recording, testBasePath);

        recordingEmitter.emitBackoutOnly("rms", List.of(), 3);

        assertThat(hsynced).as("audit write must reach hsync, not stop at hflush").isTrue();
    }

    /** Delegates to the real stream, recording whether hsync was requested. */
    private static final class HsyncRecordingStream extends OutputStream implements Syncable {
        private final FSDataOutputStream delegate;
        private final AtomicBoolean hsynced;

        HsyncRecordingStream(FSDataOutputStream delegate, AtomicBoolean hsynced) {
            this.delegate = delegate;
            this.hsynced = hsynced;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void hflush() throws IOException {
            delegate.hflush();
        }

        @Override
        public void hsync() throws IOException {
            hsynced.set(true);
            delegate.hsync();
        }
    }

    @Test
    void emitsAuditRecordAsJson() throws Exception {
        AuditRecord record = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data/raw/rms/year=2026/month=08/day=22")
                .filename("rms_inst1_123456_1.seq")
                .recordCount(100)
                .byteCount(50000)
                .firstIdentity("guid-first")
                .lastIdentity("guid-last")
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T10:30:00Z"))
                .build();

        emitter.emit(record);

        // Audit file is named after the data file
        String auditFile = testBasePath + "/rms/20260822/audit_rms_inst1_123456_1.json";
        List<String> lines = readLines(auditFile);

        assertThat(lines).hasSize(1);

        String json = lines.get(0);
        assertThat(json).contains("\"binding_id\":\"rms\"");
        assertThat(json).contains("\"partition_path\":\"/data/raw/rms/year=2026/month=08/day=22\"");
        assertThat(json).contains("\"filename\":\"rms_inst1_123456_1.seq\"");
        assertThat(json).contains("\"record_count\":100");
        assertThat(json).contains("\"byte_count\":50000");
        assertThat(json).contains("\"first_identity\":\"guid-first\"");
        assertThat(json).contains("\"last_identity\":\"guid-last\"");
        assertThat(json).contains("\"instance_id\":\"inst1\"");
        assertThat(json).contains("\"commit_timestamp\":\"2026-08-22T10:30:00Z\"");
    }

    @Test
    void eachBatchGetsItsOwnAuditFile() throws Exception {
        AuditRecord record1 = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("file1.seq")
                .recordCount(10)
                .byteCount(1000)
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T10:00:00Z"))
                .build();

        AuditRecord record2 = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("file2.seq")
                .recordCount(20)
                .byteCount(2000)
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T10:15:00Z"))
                .build();

        emitter.emit(record1);
        emitter.emit(record2);

        // Each batch gets its own immutable audit file
        assertThat(fileSystem.exists(new Path(testBasePath + "/rms/20260822/audit_file1.json"))).isTrue();
        assertThat(fileSystem.exists(new Path(testBasePath + "/rms/20260822/audit_file2.json"))).isTrue();

        // Verify contents
        List<String> lines1 = readLines(testBasePath + "/rms/20260822/audit_file1.json");
        List<String> lines2 = readLines(testBasePath + "/rms/20260822/audit_file2.json");

        assertThat(lines1).hasSize(1);
        assertThat(lines2).hasSize(1);
        assertThat(lines1.get(0)).contains("file1.seq");
        assertThat(lines2.get(0)).contains("file2.seq");
    }

    @Test
    void separatesFilesByBindingId() throws Exception {
        AuditRecord rmsRecord = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("rms.seq")
                .recordCount(10)
                .byteCount(1000)
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T10:00:00Z"))
                .build();

        AuditRecord claimsRecord = AuditRecord.builder()
                .bindingId("claims")
                .partitionPath("/data")
                .filename("claims.seq")
                .recordCount(20)
                .byteCount(2000)
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T10:00:00Z"))
                .build();

        emitter.emit(rmsRecord);
        emitter.emit(claimsRecord);

        assertThat(fileSystem.exists(new Path(testBasePath + "/rms/20260822/audit_rms.json"))).isTrue();
        assertThat(fileSystem.exists(new Path(testBasePath + "/claims/20260822/audit_claims.json"))).isTrue();
    }

    @Test
    void separatesFilesByDate() throws Exception {
        AuditRecord day1 = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("day1.seq")
                .recordCount(10)
                .byteCount(1000)
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T23:59:00Z"))
                .build();

        AuditRecord day2 = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("day2.seq")
                .recordCount(20)
                .byteCount(2000)
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-23T00:01:00Z"))
                .build();

        emitter.emit(day1);
        emitter.emit(day2);

        assertThat(fileSystem.exists(new Path(testBasePath + "/rms/20260822/audit_day1.json"))).isTrue();
        assertThat(fileSystem.exists(new Path(testBasePath + "/rms/20260823/audit_day2.json"))).isTrue();
    }

    @Test
    void handlesNullIdentityValues() throws Exception {
        AuditRecord record = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("test.seq")
                .recordCount(10)
                .byteCount(1000)
                .firstIdentity(null)
                .lastIdentity(null)
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T10:00:00Z"))
                .build();

        emitter.emit(record);

        String auditFile = testBasePath + "/rms/20260822/audit_test.json";
        List<String> lines = readLines(auditFile);

        assertThat(lines.get(0)).contains("\"first_identity\":null");
        assertThat(lines.get(0)).contains("\"last_identity\":null");
    }

    @Test
    void escapesSpecialCharactersInJson() throws Exception {
        AuditRecord record = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data/with\"quotes")
                .filename("file_with_tabs.seq")
                .recordCount(10)
                .byteCount(1000)
                .firstIdentity("line\nbreak")
                .instanceId("inst1")
                .commitTimestamp(Instant.parse("2026-08-22T10:00:00Z"))
                .build();

        emitter.emit(record);

        String auditFile = testBasePath + "/rms/20260822/audit_file_with_tabs.json";
        List<String> lines = readLines(auditFile);

        // JSON should have escaped special characters
        assertThat(lines.get(0)).contains("\\\"");  // escaped quote
        assertThat(lines.get(0)).contains("\\n");   // escaped newline
    }

    private List<String> readLines(String path) throws Exception {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fileSystem.open(new Path(path))))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}
