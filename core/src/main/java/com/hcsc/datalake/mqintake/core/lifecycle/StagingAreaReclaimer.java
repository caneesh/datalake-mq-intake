package com.hcsc.datalake.mqintake.core.lifecycle;

import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Removes staging debris — this instance's own, and that of instances which
 * are gone.
 *
 * <p>Split out of {@code StartupValidator}, which had grown to do two
 * unrelated jobs and said so itself: it carried a constructor documented as
 * existing "for callers that have no audit destination to check — notably
 * cleanupInstanceTempFiles, which needs a validator but performs no
 * validation". A class that has to build a disabled copy of itself to reuse
 * its own file handling is two classes.
 *
 * <p><strong>This one deletes files.</strong> Both methods stay together
 * because they are two halves of one policy — the running instance's own
 * directory is swept by file age alone, everyone else's requires a stale lease
 * AND an expired file — and separating them would invite applying one rule
 * where the other belongs. The validator, by contrast, only ever reads and
 * creates.
 *
 * <p>Behaviour is unchanged from the validator: same rules, same order, same
 * logging, same failure handling. See {@code AbandonedInstanceReclamationTest},
 * whose two safety conditions were each verified to fail independently.
 */
public class StagingAreaReclaimer {

    private static final Logger log = LoggerFactory.getLogger(StagingAreaReclaimer.class);

    private final FileSystem fileSystem;
    private final String instanceId;

    /**
     * No argument validation, matching the validator this was split from: it
     * accepted whatever it was given, and an extraction must accept exactly
     * what the original accepted.
     */
    public StagingAreaReclaimer(FileSystem fileSystem, String instanceId) {
        this.fileSystem = fileSystem;
        this.instanceId = instanceId;
    }

    /**
     * Cleans up stale temp files for THIS instance only.
     * From §8.1: On startup, sweep ONLY this instance's own _tmp/{instance_id}/
     * subtree for crash debris — never another instance's.
     *
     * @param basePath  the HDFS base path
     * @param maxAgeMs  maximum age in milliseconds
     * @return number of files deleted
     */
    /**
     * Reclaims staging directories left behind by instances that are gone.
     *
     * <p>An instance id carries the PID, so an ordinary restart abandons the
     * previous directory and {@link #cleanupInstanceTempFiles} — which only
     * ever visits the CURRENT instance's directory — never returns to it.
     * Debris from every crash accumulated indefinitely across the data, index
     * and audit trees.
     *
     * <p>Deleting other instances' files cannot be done casually: two
     * instances may share a host, and removing one that is mid-write destroys
     * an in-flight batch. So reclamation requires TWO independent conditions,
     * and both must hold:
     *
     * <ol>
     *   <li>The directory's lease is stale — no refresh within
     *       {@code leaseTimeoutMs} — or it has no lease and nothing in it has
     *       been touched within that period either. A running instance
     *       refreshes its lease continuously.</li>
     *   <li>The individual file is older than {@code maxAgeMs}, the same rule
     *       an instance applies to its own directory.</li>
     * </ol>
     *
     * <p>The second condition is what makes this safe rather than merely
     * likely to be safe. If the first misjudges — a live peer whose lease
     * refresh has been failing against a struggling filesystem — a file being
     * written right now is still not an hour old, so it is not touched. Both
     * conditions have to agree before anything is deleted.
     *
     * @return the number of files removed
     */
    public int reclaimAbandonedInstances(String basePath, long maxAgeMs, long leaseTimeoutMs)
            throws IOException {
        // The parent of this instance's own staging directory is the root
        // every instance's directory sits under. Derived rather than rebuilt
        // from a literal, so the layout stays owned by PartitionPath.
        Path tempRoot = new Path(PartitionPath.tempDir(basePath, instanceId)).getParent();
        if (!fileSystem.exists(tempRoot)) {
            return 0;
        }

        long now = System.currentTimeMillis();
        int deleted = 0;

        for (var candidate : fileSystem.listStatus(tempRoot)) {
            if (!candidate.isDirectory()) {
                continue;
            }
            String owner = candidate.getPath().getName();
            if (owner.equals(instanceId)) {
                continue;   // our own; cleanupInstanceTempFiles owns it
            }
            if (!isAbandoned(candidate.getPath(), now, leaseTimeoutMs)) {
                log.debug("Staging directory {} still has a live lease — leaving it alone",
                        candidate.getPath());
                continue;
            }

            int removed = deleteExpiredFiles(candidate.getPath(), now - maxAgeMs);
            if (removed > 0) {
                log.info("Reclaimed {} expired file(s) from abandoned instance '{}' at {}",
                        removed, owner, candidate.getPath());
            }
            deleted += removed;
            removeIfEmpty(candidate.getPath());
        }

        return deleted;
    }
    /** True when nothing has claimed this directory recently. */
    private boolean isAbandoned(Path instanceDir, long now, long leaseTimeoutMs)
            throws IOException {
        Path lease = new Path(instanceDir, InstanceLease.LEASE_FILENAME);
        if (fileSystem.exists(lease)) {
            long age = now - fileSystem.getFileStatus(lease).getModificationTime();
            return age > leaseTimeoutMs;
        }

        // No lease at all: either an instance from before leases existed, or
        // one killed before it wrote the first. Fall back to the newest thing
        // in the directory — a running instance writes staging files
        // constantly, so silence for the whole timeout means nobody is home.
        long newest = 0;
        for (var status : fileSystem.listStatus(instanceDir)) {
            newest = Math.max(newest, status.getModificationTime());
        }
        return newest == 0 || (now - newest) > leaseTimeoutMs;
    }
    private int deleteExpiredFiles(Path instanceDir, long cutoff) throws IOException {
        int deleted = 0;
        for (var status : fileSystem.listStatus(instanceDir)) {
            if (!status.isFile()) {
                continue;
            }
            if (status.getPath().getName().equals(InstanceLease.LEASE_FILENAME)) {
                continue;   // removed with the directory, not by age
            }
            if (status.getModificationTime() < cutoff && fileSystem.delete(status.getPath(), false)) {
                deleted++;
            }
        }
        return deleted;
    }
    /** Removes a reclaimed directory once only its lease remains. */
    private void removeIfEmpty(Path instanceDir) {
        try {
            var remaining = fileSystem.listStatus(instanceDir);
            for (var status : remaining) {
                if (!status.getPath().getName().equals(InstanceLease.LEASE_FILENAME)) {
                    return;   // real files left; keep the directory and retry next start
                }
            }
            fileSystem.delete(instanceDir, true);
            log.info("Removed the emptied staging directory {}", instanceDir);
        } catch (IOException e) {
            log.debug("Could not remove {}: {}", instanceDir, e.getMessage());
        }
    }
    public int cleanupInstanceTempFiles(String basePath, long maxAgeMs) throws IOException {
        String tmpDir = PartitionPath.tempDir(basePath, instanceId);
        Path tmpPath = new Path(tmpDir);

        if (!fileSystem.exists(tmpPath)) {
            return 0;
        }

        int deleted = 0;
        long cutoff = System.currentTimeMillis() - maxAgeMs;

        for (var status : fileSystem.listStatus(tmpPath)) {
            if (status.isFile() && status.getModificationTime() < cutoff) {
                if (fileSystem.delete(status.getPath(), false)) {
                    log.info("Deleted stale temp file from crash debris: {}", status.getPath());
                    deleted++;
                }
            }
        }

        if (deleted > 0) {
            log.info("Cleaned up {} stale temp files for instance '{}' at {}",
                    deleted, instanceId, tmpDir);
        }

        return deleted;
    }
}
