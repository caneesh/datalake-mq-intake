package com.hcsc.datalake.mqintake.core.audit;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for AuditRecord.
 */
class AuditRecordTest {

    @Test
    void buildsValidAuditRecord() {
        Instant now = Instant.now();

        AuditRecord record = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data/raw/rms/year=2026/month=08/day=22/hour=10/quarter=2")
                .filename("rms_instance1_1724328000000_1.seq")
                .recordCount(4000)
                .byteCount(1048576)
                .firstIdentity("guid-001")
                .lastIdentity("guid-4000")
                .instanceId("instance1")
                .commitTimestamp(now)
                .build();

        assertThat(record.getBindingId()).isEqualTo("rms");
        assertThat(record.getPartitionPath()).isEqualTo("/data/raw/rms/year=2026/month=08/day=22/hour=10/quarter=2");
        assertThat(record.getFilename()).isEqualTo("rms_instance1_1724328000000_1.seq");
        assertThat(record.getRecordCount()).isEqualTo(4000);
        assertThat(record.getByteCount()).isEqualTo(1048576);
        assertThat(record.getFirstIdentity()).isEqualTo("guid-001");
        assertThat(record.getLastIdentity()).isEqualTo("guid-4000");
        assertThat(record.getInstanceId()).isEqualTo("instance1");
        assertThat(record.getCommitTimestamp()).isEqualTo(now);
    }

    @Test
    void computesFullFilePath() {
        AuditRecord record = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data/raw/rms/year=2026/month=08")
                .filename("test.seq")
                .recordCount(1)
                .byteCount(100)
                .instanceId("inst")
                .commitTimestamp(Instant.now())
                .build();

        assertThat(record.getFilePath()).isEqualTo("/data/raw/rms/year=2026/month=08/test.seq");
    }

    @Test
    void allowsNullIdentityValues() {
        AuditRecord record = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("test.seq")
                .recordCount(1)
                .byteCount(0)
                .firstIdentity(null)
                .lastIdentity(null)
                .instanceId("inst")
                .commitTimestamp(Instant.now())
                .build();

        assertThat(record.getFirstIdentity()).isNull();
        assertThat(record.getLastIdentity()).isNull();
    }

    @Test
    void rejectsNullBindingId() {
        assertThatThrownBy(() -> AuditRecord.builder()
                .bindingId(null)
                .partitionPath("/data")
                .filename("test.seq")
                .recordCount(1)
                .byteCount(0)
                .instanceId("inst")
                .commitTimestamp(Instant.now())
                .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("bindingId");
    }

    @Test
    void rejectsZeroRecordCount() {
        assertThatThrownBy(() -> AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("test.seq")
                .recordCount(0)
                .byteCount(0)
                .instanceId("inst")
                .commitTimestamp(Instant.now())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recordCount must be positive");
    }

    @Test
    void rejectsNegativeByteCount() {
        assertThatThrownBy(() -> AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("test.seq")
                .recordCount(1)
                .byteCount(-1)
                .instanceId("inst")
                .commitTimestamp(Instant.now())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("byteCount must be non-negative");
    }

    @Test
    void equalsAndHashCode() {
        Instant now = Instant.now();

        AuditRecord record1 = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("test.seq")
                .recordCount(100)
                .byteCount(5000)
                .firstIdentity("first")
                .lastIdentity("last")
                .instanceId("inst")
                .commitTimestamp(now)
                .build();

        AuditRecord record2 = AuditRecord.builder()
                .bindingId("rms")
                .partitionPath("/data")
                .filename("test.seq")
                .recordCount(100)
                .byteCount(5000)
                .firstIdentity("first")
                .lastIdentity("last")
                .instanceId("inst")
                .commitTimestamp(now)
                .build();

        assertThat(record1).isEqualTo(record2);
        assertThat(record1.hashCode()).isEqualTo(record2.hashCode());
    }
}
