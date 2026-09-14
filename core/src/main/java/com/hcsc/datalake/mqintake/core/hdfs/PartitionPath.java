package com.hcsc.datalake.mqintake.core.hdfs;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Computes partition paths from base path and timestamp.
 *
 * <p>This is a PURE, STATELESS function. It must be called fresh at every flush.
 * No partition path, hour, or quarter value may be stored in a field or cached
 * across batches.
 *
 * <p>A cached path is the confirmed root cause of production problem #3:
 * files landing in a partition ~87 minutes stale.
 *
 * <p>Path format: {base}/{YYYY}/{MM}/{DD}/{HH}/{Q}
 * <ul>
 *   <li>All components derived from UTC</li>
 *   <li>Quarter = minute / 15 (integer division): 0-14→0, 15-29→1, 30-44→2, 45-59→3</li>
 *   <li>Numeric format matches legacy MDB paths for migration compatibility</li>
 * </ul>
 */
public final class PartitionPath {

    /** Length of one partition window: a quarter hour. */
    private static final long WINDOW_MILLIS = 15L * 60L * 1000L;

    /**
     * The partition window, public because reconciliation must agree with it.
     * The 15-minute literal previously lived in three files; changing one and
     * missing another would silently break grace-period timing or window
     * enumeration with no compile-time signal.
     */
    public static final java.time.Duration WINDOW = java.time.Duration.ofMillis(WINDOW_MILLIS);

    private PartitionPath() {
        // Static utility class
    }

    /**
     * Returns an identifier for the partition window containing the instant.
     *
     * <p>Two instants share a window identifier exactly when {@link #compute}
     * would place them in the same partition directory. Epoch milliseconds
     * divide evenly into quarter hours, so this agrees with the UTC
     * {@code minute / 15} arithmetic in {@code compute} without repeating it.
     *
     * <p>Used to keep a batch from spanning a partition boundary: a batch that
     * accumulated in one window is flushed before messages from the next join
     * it, so each window produces its own file rather than everything landing
     * in whichever partition happened to be current at flush time.
     */
    public static long windowId(Instant instant) {
        return Math.floorDiv(instant.toEpochMilli(), WINDOW_MILLIS);
    }

    /**
     * Computes the partition path for the given base path and instant.
     *
     * <p>CRITICAL: Call this fresh at every flush. Never cache the result.
     *
     * @param basePath the HDFS base path for the binding
     * @param instant  the flush timestamp (typically Instant.now())
     * @return the full partition path
     */
    public static String compute(String basePath, Instant instant) {
        ZonedDateTime utc = instant.atZone(ZoneOffset.UTC);

        int year = utc.getYear();
        int month = utc.getMonthValue();
        int day = utc.getDayOfMonth();
        int hour = utc.getHour();
        int quarter = utc.getMinute() / 15;

        return String.format("%s/%04d/%02d/%02d/%02d/%d",
                normalizeBasePath(basePath), year, month, day, hour, quarter);
    }

    /**
     * Computes the temp directory path for an instance.
     *
     * @param basePath   the HDFS base path for the binding
     * @param instanceId the unique instance identifier
     * @return the temp directory path: {base}/_tmp/{instance_id}
     */
    public static String tempDir(String basePath, String instanceId) {
        return String.format("%s/_tmp/%s", normalizeBasePath(basePath), instanceId);
    }

    /**
     * Generates a unique filename for a batch.
     *
     * @param bindingId  the binding identifier
     * @param instanceId the unique instance identifier
     * @param epochMs    the timestamp in epoch milliseconds
     * @param batchSeq   the batch sequence number for this instance
     * @return filename: {binding_id}_{instance_id}_{epoch_millis}_{batch_seq} (no extension, matching legacy MDB)
     */
    public static String filename(String bindingId, String instanceId, long epochMs, long batchSeq) {
        return String.format("%s_%s_%d_%d", bindingId, instanceId, epochMs, batchSeq);
    }

    /**
     * True for a landed data file, as opposed to an audit record, sidecar
     * index, lease, or other metadata sharing the partition directory.
     *
     * <p>Data files carry no extension (legacy MDB contract); a {@code .seq}
     * suffix is accepted for files written before that change. Everything
     * reconciliation and orphan classification enumerate goes through here,
     * so the two cannot disagree about what counts as data.
     */
    public static boolean isDataFile(String filename) {
        if (filename == null || filename.isEmpty()) {
            return false;
        }
        if (filename.startsWith("_") || filename.startsWith(".")) {
            return false;
        }
        if (filename.endsWith(".json") || filename.endsWith(".jsonl")
                || filename.endsWith(".tmp") || filename.endsWith(".crc")) {
            return false;
        }
        return true;
    }

    /**
     * Removes trailing slash from base path if present.
     */
    private static String normalizeBasePath(String basePath) {
        if (basePath != null && basePath.endsWith("/")) {
            return basePath.substring(0, basePath.length() - 1);
        }
        return basePath;
    }
}
