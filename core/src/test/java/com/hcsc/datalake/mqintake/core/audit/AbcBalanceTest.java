package com.hcsc.datalake.mqintake.core.audit;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ABC balance equation: everything consumed is accounted for.
 *
 * <p>{@code consumed == recordCount + backoutCount} must hold for every unit
 * of work, or a control cannot tell a deliberately set-aside message from a
 * lost one.
 */
class AbcBalanceTest {

    private FileSystem fileSystem;
    private java.nio.file.Path auditDir;
    private HdfsAuditRecordEmitter emitter;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);
        auditDir = Files.createTempDirectory("abc-balance");
        emitter = new HdfsAuditRecordEmitter(fileSystem, auditDir.toString(), "abc-instance",
                Clock.fixed(Instant.parse("2026-08-26T10:15:00Z"), ZoneOffset.UTC));
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.delete(new Path(auditDir.toString()), true);
    }

    @Test
    void aCleanBatchBalancesExactly() throws Exception {
        emitter.emit(record(10, 0));

        AuditRecord read = onlyRecord();
        assertThat(read.getRecordCount()).isEqualTo(10);
        assertThat(read.getBackoutCount()).isZero();
        assertThat(read.getConsumedCount()).isEqualTo(10);
    }

    @Test
    void aBatchWithPoisonStillBalances() throws Exception {
        // 10 consumed, 9 landed, 1 set aside. Without backoutCount this reads
        // as a loss of one message.
        emitter.emit(record(9, 1));

        AuditRecord read = onlyRecord();
        assertThat(read.getRecordCount()).isEqualTo(9);
        assertThat(read.getBackoutCount()).isEqualTo(1);
        assertThat(read.getConsumedCount())
                .as("consumed = landed + set aside")
                .isEqualTo(10);
    }

    @Test
    void aBatchThatLandedNothingStillProducesARecord() throws Exception {
        // Every message was poison, so no file was written — but 5 messages
        // were consumed, and without a record of their own they appear in no
        // audit anywhere and the balance shows them as lost.
        emitter.emitBackoutOnly("rms", List.of(), 5);

        AuditRecord read = onlyRecord();
        assertThat(read.getRecordCount()).isZero();
        assertThat(read.getBackoutCount()).isEqualTo(5);
        assertThat(read.getConsumedCount()).isEqualTo(5);
    }

    @Test
    void totalsAcrossManyBatchesSumToWhatWasConsumed() throws Exception {
        emitter.emit(record(100, 0));
        emitter.emit(record(97, 3));
        emitter.emitBackoutOnly("rms", List.of(), 2);

        long landed = 0;
        long backedOut = 0;
        long consumed = 0;
        for (AuditRecord record : allRecords()) {
            landed += record.getRecordCount();
            backedOut += record.getBackoutCount();
            consumed += record.getConsumedCount();
        }

        assertThat(landed).isEqualTo(197);
        assertThat(backedOut).isEqualTo(5);
        assertThat(consumed)
                .as("every consumed message is accounted for as landed or set aside")
                .isEqualTo(202)
                .isEqualTo(landed + backedOut);
    }

    @Test
    void theRecordCarriesTheBalanceFieldsInItsJson() throws Exception {
        emitter.emit(record(9, 1));

        String json = rawJson();

        // A control reads this file, so the fields must be present by name
        assertThat(json).contains("\"record_count\":9");
        assertThat(json).contains("\"backout_count\":1");
        assertThat(json).contains("\"consumed_count\":10");
    }

    @Test
    void noPartialAuditFileIsLeftVisible() throws Exception {
        // Staged and renamed: a control must never read half a record.
        emitter.emit(record(5, 0));

        try (var stream = Files.walk(auditDir)) {
            assertThat(stream.filter(p -> p.getFileName().toString().endsWith(".tmp")))
                    .as("no staging file left behind")
                    .isEmpty();
        }
    }

    // --- helpers ---

    private AuditRecord record(int recordCount, int backoutCount) {
        return AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data/raw/rms/year=2026/month=08/day=26/hour=10/quarter=1")
                .filename("rms_abc_1_" + recordCount + "_" + backoutCount + ".seq")
                .recordCount(recordCount)
                .byteCount(recordCount * 100L)
                .backoutCount(backoutCount)
                .instanceId("abc-instance")
                .commitTimestamp(Instant.parse("2026-08-26T10:15:00Z"))
                .build();
    }

    private List<AuditRecord> allRecords() throws Exception {
        List<AuditRecord> records = new ArrayList<>();
        for (String json : rawJsonLines()) {
            records.add(AuditRecord.builder()
                    .bindingId("rms")
                    .partitionPath("p")
                    .filename("f")
                    .recordCount((int) longField(json, "record_count"))
                    .byteCount(longField(json, "byte_count"))
                    .backoutCount((int) longField(json, "backout_count"))
                    .instanceId("abc-instance")
                    .commitTimestamp(Instant.parse("2026-08-26T10:15:00Z"))
                    .build());
        }
        return records;
    }

    private AuditRecord onlyRecord() throws Exception {
        List<AuditRecord> records = allRecords();
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    private String rawJson() throws Exception {
        List<String> lines = rawJsonLines();
        assertThat(lines).hasSize(1);
        return lines.get(0);
    }

    private List<String> rawJsonLines() throws Exception {
        List<String> lines = new ArrayList<>();
        try (var stream = Files.walk(auditDir)) {
            for (java.nio.file.Path p : stream
                    .filter(p -> p.getFileName().toString().startsWith("audit_"))
                    .filter(p -> p.toString().endsWith(".json"))
                    .collect(java.util.stream.Collectors.toList())) {
                lines.add(new String(Files.readAllBytes(p), StandardCharsets.UTF_8).trim());
            }
        }
        return lines;
    }

    private long longField(String json, String field) {
        String marker = "\"" + field + "\":";
        int i = json.indexOf(marker) + marker.length();
        int end = i;
        while (end < json.length() && (Character.isDigit(json.charAt(end)))) {
            end++;
        }
        return Long.parseLong(json.substring(i, end));
    }
}
