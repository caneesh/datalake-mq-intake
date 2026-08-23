package com.hcsc.datalake.mqintake.core.hdfs;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PartitionPath.
 *
 * Verifies the path computation is:
 * - A pure function of (base_path, instant)
 * - Correct across hour and quarter boundaries
 * - Using UTC time
 */
class PartitionPathTest {

    @Test
    void computesCorrectPathForMidQuarter() {
        // 2025-08-22 14:23:45 UTC -> quarter = 23/15 = 1
        Instant instant = ZonedDateTime.of(2025, 8, 22, 14, 23, 45, 0, ZoneOffset.UTC).toInstant();

        String path = PartitionPath.compute("/data/raw/rms", instant);

        assertThat(path).isEqualTo("/data/raw/rms/year=2025/month=08/day=22/hour=14/quarter=1");
    }

    @Test
    void quarter0ForMinutes0To14() {
        Instant minute0 = ZonedDateTime.of(2025, 8, 22, 10, 0, 0, 0, ZoneOffset.UTC).toInstant();
        Instant minute14 = ZonedDateTime.of(2025, 8, 22, 10, 14, 59, 0, ZoneOffset.UTC).toInstant();

        assertThat(PartitionPath.compute("/base", minute0))
                .endsWith("hour=10/quarter=0");
        assertThat(PartitionPath.compute("/base", minute14))
                .endsWith("hour=10/quarter=0");
    }

    @Test
    void quarter1ForMinutes15To29() {
        Instant minute15 = ZonedDateTime.of(2025, 8, 22, 10, 15, 0, 0, ZoneOffset.UTC).toInstant();
        Instant minute29 = ZonedDateTime.of(2025, 8, 22, 10, 29, 59, 0, ZoneOffset.UTC).toInstant();

        assertThat(PartitionPath.compute("/base", minute15))
                .endsWith("hour=10/quarter=1");
        assertThat(PartitionPath.compute("/base", minute29))
                .endsWith("hour=10/quarter=1");
    }

    @Test
    void quarter2ForMinutes30To44() {
        Instant minute30 = ZonedDateTime.of(2025, 8, 22, 10, 30, 0, 0, ZoneOffset.UTC).toInstant();
        Instant minute44 = ZonedDateTime.of(2025, 8, 22, 10, 44, 59, 0, ZoneOffset.UTC).toInstant();

        assertThat(PartitionPath.compute("/base", minute30))
                .endsWith("hour=10/quarter=2");
        assertThat(PartitionPath.compute("/base", minute44))
                .endsWith("hour=10/quarter=2");
    }

    @Test
    void quarter3ForMinutes45To59() {
        Instant minute45 = ZonedDateTime.of(2025, 8, 22, 10, 45, 0, 0, ZoneOffset.UTC).toInstant();
        Instant minute59 = ZonedDateTime.of(2025, 8, 22, 10, 59, 59, 0, ZoneOffset.UTC).toInstant();

        assertThat(PartitionPath.compute("/base", minute45))
                .endsWith("hour=10/quarter=3");
        assertThat(PartitionPath.compute("/base", minute59))
                .endsWith("hour=10/quarter=3");
    }

    @Test
    void pathChangesAcrossQuarterBoundary() {
        // 10:14:59 -> quarter=0
        Instant beforeBoundary = ZonedDateTime.of(2025, 8, 22, 10, 14, 59, 0, ZoneOffset.UTC).toInstant();
        // 10:15:00 -> quarter=1
        Instant afterBoundary = ZonedDateTime.of(2025, 8, 22, 10, 15, 0, 0, ZoneOffset.UTC).toInstant();

        String pathBefore = PartitionPath.compute("/base", beforeBoundary);
        String pathAfter = PartitionPath.compute("/base", afterBoundary);

        assertThat(pathBefore).endsWith("hour=10/quarter=0");
        assertThat(pathAfter).endsWith("hour=10/quarter=1");
        assertThat(pathBefore).isNotEqualTo(pathAfter);
    }

    @Test
    void pathChangesAcrossHourBoundary() {
        // 10:59:59 -> hour=10, quarter=3
        Instant beforeBoundary = ZonedDateTime.of(2025, 8, 22, 10, 59, 59, 0, ZoneOffset.UTC).toInstant();
        // 11:00:00 -> hour=11, quarter=0
        Instant afterBoundary = ZonedDateTime.of(2025, 8, 22, 11, 0, 0, 0, ZoneOffset.UTC).toInstant();

        String pathBefore = PartitionPath.compute("/base", beforeBoundary);
        String pathAfter = PartitionPath.compute("/base", afterBoundary);

        assertThat(pathBefore).endsWith("hour=10/quarter=3");
        assertThat(pathAfter).endsWith("hour=11/quarter=0");
        assertThat(pathBefore).isNotEqualTo(pathAfter);
    }

    @Test
    void pathChangesAcrossDayBoundary() {
        // 2025-08-22 23:59:59 -> day=22, hour=23, quarter=3
        Instant beforeBoundary = ZonedDateTime.of(2025, 8, 22, 23, 59, 59, 0, ZoneOffset.UTC).toInstant();
        // 2025-08-23 00:00:00 -> day=23, hour=00, quarter=0
        Instant afterBoundary = ZonedDateTime.of(2025, 8, 23, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();

        String pathBefore = PartitionPath.compute("/base", beforeBoundary);
        String pathAfter = PartitionPath.compute("/base", afterBoundary);

        assertThat(pathBefore).contains("day=22").endsWith("hour=23/quarter=3");
        assertThat(pathAfter).contains("day=23").endsWith("hour=00/quarter=0");
    }

    @Test
    void usesUtcNotLocalTime() {
        // This test ensures we're using UTC, not the local timezone
        // Use a time that would be different in many timezones
        Instant instant = Instant.parse("2025-08-22T02:30:00Z");

        String path = PartitionPath.compute("/base", instant);

        // Should be hour=02, quarter=2 in UTC
        assertThat(path).endsWith("hour=02/quarter=2");
    }

    @Test
    void handlesTrailingSlashInBasePath() {
        Instant instant = ZonedDateTime.of(2025, 8, 22, 10, 20, 0, 0, ZoneOffset.UTC).toInstant();

        String pathWithSlash = PartitionPath.compute("/data/raw/rms/", instant);
        String pathWithoutSlash = PartitionPath.compute("/data/raw/rms", instant);

        assertThat(pathWithSlash).isEqualTo(pathWithoutSlash);
        assertThat(pathWithSlash).doesNotContain("//");
    }

    @Test
    void tempDirIncludesInstanceId() {
        String tempDir = PartitionPath.tempDir("/data/raw/rms", "instance-123");

        assertThat(tempDir).isEqualTo("/data/raw/rms/_tmp/instance-123");
    }

    @Test
    void filenameIncludesAllComponents() {
        String filename = PartitionPath.filename("rms", "instance-1", 1692700000000L, 42);

        assertThat(filename).isEqualTo("rms_instance-1_1692700000000_42.seq");
        assertThat(filename).endsWith(".seq");
    }

    @Test
    void sameInstantProducesSamePath() {
        Instant instant = Instant.parse("2025-08-22T14:23:45Z");

        String path1 = PartitionPath.compute("/base", instant);
        String path2 = PartitionPath.compute("/base", instant);

        assertThat(path1).isEqualTo(path2);
    }

    @Test
    void differentInstantsInSameQuarterProduceSamePath() {
        Instant instant1 = ZonedDateTime.of(2025, 8, 22, 10, 16, 0, 0, ZoneOffset.UTC).toInstant();
        Instant instant2 = ZonedDateTime.of(2025, 8, 22, 10, 28, 59, 0, ZoneOffset.UTC).toInstant();

        String path1 = PartitionPath.compute("/base", instant1);
        String path2 = PartitionPath.compute("/base", instant2);

        assertThat(path1).isEqualTo(path2);
    }

    @Test
    void designDocExample_0420_isQuarter1() {
        // From §7: "04:20 UTC → hour=04/quarter=1"
        Instant instant = ZonedDateTime.of(2025, 8, 22, 4, 20, 0, 0, ZoneOffset.UTC).toInstant();

        String path = PartitionPath.compute("/base", instant);

        assertThat(path).endsWith("hour=04/quarter=1");
    }

    @Test
    void designDocExample_0447_isQuarter3() {
        // From §7: "04:47 UTC → hour=04/quarter=3"
        Instant instant = ZonedDateTime.of(2025, 8, 22, 4, 47, 0, 0, ZoneOffset.UTC).toInstant();

        String path = PartitionPath.compute("/base", instant);

        assertThat(path).endsWith("hour=04/quarter=3");
    }

    // --- windowId must agree with compute() ---

    @Test
    void windowIdAgreesWithComputedPartitionAcrossADay() {
        // Sweep a full day at one-minute resolution: two instants must share a
        // window id exactly when they land in the same partition directory.
        // If these ever disagree, batches would be bounded to the wrong window.
        Instant start = Instant.parse("2026-08-22T00:00:00Z");
        for (long minute = 0; minute < 24 * 60; minute++) {
            Instant a = start.plusSeconds(minute * 60);
            Instant b = start.plusSeconds((minute + 1) * 60);

            boolean samePath = PartitionPath.compute("/base", a)
                    .equals(PartitionPath.compute("/base", b));
            boolean sameWindow = PartitionPath.windowId(a) == PartitionPath.windowId(b);

            assertThat(sameWindow)
                    .as("minute %d: path-equal=%s but window-equal=%s", minute, samePath, sameWindow)
                    .isEqualTo(samePath);
        }
    }

    @Test
    void windowIdChangesExactlyOnQuarterBoundaries() {
        Instant justBefore = Instant.parse("2026-08-22T10:14:59.999Z");
        Instant atBoundary = Instant.parse("2026-08-22T10:15:00Z");
        Instant withinNext = Instant.parse("2026-08-22T10:29:59.999Z");

        assertThat(PartitionPath.windowId(justBefore))
                .isNotEqualTo(PartitionPath.windowId(atBoundary));
        assertThat(PartitionPath.windowId(atBoundary))
                .isEqualTo(PartitionPath.windowId(withinNext));
        assertThat(PartitionPath.windowId(atBoundary))
                .isEqualTo(PartitionPath.windowId(justBefore) + 1);
    }
}
