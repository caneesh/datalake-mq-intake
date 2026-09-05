package com.hcsc.datalake.mqintake.core.lifecycle;

import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reclaiming staging directories left by instances that are gone.
 *
 * <p>An instance id carries the PID, so every restart abandons a directory
 * that nothing ever revisited — cleanup only swept the running instance's own.
 * Debris accumulated indefinitely.
 *
 * <p>The risk in fixing it is the reason it was not fixed: two instances can
 * share a host, and deleting a live peer's staging file destroys an in-flight
 * batch. Most of what follows is about NOT deleting things.
 */
class AbandonedInstanceReclamationTest {

    private static final long MAX_AGE = Duration.ofHours(1).toMillis();
    private static final long LEASE_TIMEOUT = Duration.ofHours(1).toMillis();

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem fs;
    private String basePath;
    private StagingAreaReclaimer reclaimer;   // acting as instance "current"

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fs = FileSystem.getLocal(conf);
        basePath = tempDir.resolve("data").toString();
        reclaimer = new StagingAreaReclaimer(fs, "current");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fs != null) {
            fs.close();
        }
    }

    // --- what must be reclaimed ---

    @Test
    void debrisFromADeadInstanceIsReclaimed() throws Exception {
        // The reported defect: an expired file under a previous instance's
        // directory survived startup cleanup for the new one, forever.
        Path stale = stagedFile("host-111", "batch.seq", ageMs(Duration.ofHours(9)));
        leaseAged("host-111", Duration.ofHours(9));

        int reclaimed = reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(reclaimed).isEqualTo(1);
        assertThat(fs.exists(stale)).isFalse();
    }

    @Test
    void anInstanceThatShutDownCleanlyIsReclaimedWithoutWaitingForTheTimeout() throws Exception {
        // A clean shutdown removes the lease, so the directory is abandoned
        // immediately rather than an hour later.
        Path stale = stagedFile("host-111", "batch.seq", ageMs(Duration.ofHours(2)));
        // no lease file at all

        reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(fs.exists(stale)).isFalse();
    }

    @Test
    void anEmptiedDirectoryIsRemovedRatherThanLeftBehind() throws Exception {
        stagedFile("host-111", "batch.seq", ageMs(Duration.ofHours(9)));
        leaseAged("host-111", Duration.ofHours(9));

        reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(fs.exists(new Path(PartitionPath.tempDir(basePath, "host-111")))).isFalse();
    }

    @Test
    void ownInstanceCleanupTouchesOnlyItsOwnDirectory() throws Exception {
        // Moved here from StartupValidatorTest, which is the point of the
        // split: this was never a validation test. The own-directory sweep
        // applies file age ALONE, with no lease check, so it must never reach
        // a sibling — reclaimAbandonedInstances is the only path that may,
        // and only with a stale lease as well.
        Path ours = stagedFile("current", "stale.seq", ageMs(Duration.ofHours(9)));
        Path theirs = stagedFile("other-instance", "stale.seq", ageMs(Duration.ofHours(9)));

        int deleted = reclaimer.cleanupInstanceTempFiles(basePath, 0);

        assertThat(deleted).isEqualTo(1);
        assertThat(fs.exists(ours)).isFalse();
        assertThat(fs.exists(theirs))
                .as("another instance's file is not this sweep's business")
                .isTrue();
    }

    @Test
    void ownInstanceCleanupKeepsFilesInsideTheMaxAge() throws Exception {
        Path recent = stagedFile("current", "recent.seq", ageMs(Duration.ofMinutes(1)));

        int deleted = reclaimer.cleanupInstanceTempFiles(basePath, Duration.ofHours(1).toMillis());

        assertThat(deleted).isZero();
        assertThat(fs.exists(recent)).isTrue();
    }

    // --- what must NOT be reclaimed ---

    @Test
    void aLivePeersFilesAreNeverTouched() throws Exception {
        // The condition that matters most. The peer's file is old enough to
        // expire, but its lease is fresh, so it is running.
        Path theirs = stagedFile("host-222", "in-flight.seq", ageMs(Duration.ofHours(9)));
        leaseAged("host-222", Duration.ZERO);

        int reclaimed = reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(reclaimed).isZero();
        assertThat(fs.exists(theirs))
                .as("a running instance's staging file must survive another instance's startup")
                .isTrue();
    }

    @Test
    void aRecentFileSurvivesEvenWhenTheLeaseLooksStale() throws Exception {
        // The second condition, and the reason this is safe rather than merely
        // likely to be safe. If the lease check misjudges — a live peer whose
        // refresh has been failing against a struggling filesystem — a file
        // being written right now is still not an hour old.
        Path fresh = stagedFile("host-333", "being-written.seq", ageMs(Duration.ofMinutes(2)));
        leaseAged("host-333", Duration.ofHours(9));

        int reclaimed = reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(reclaimed).isZero();
        assertThat(fs.exists(fresh)).isTrue();
    }

    @Test
    void aDirectoryWithNoLeaseButRecentActivityIsLeftAlone() throws Exception {
        // An instance from before leases existed, or one killed before it
        // wrote its first. Recent writes mean somebody is home.
        Path fresh = stagedFile("host-444", "recent.seq", ageMs(Duration.ofMinutes(5)));

        int reclaimed = reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(reclaimed).isZero();
        assertThat(fs.exists(fresh)).isTrue();
    }

    @Test
    void theRunningInstanceNeverReclaimsItsOwnDirectory() throws Exception {
        // Its own directory belongs to cleanupInstanceTempFiles, which applies
        // the age rule without any lease check. Reclaiming it here would apply
        // the rule twice and could delete a file this instance is writing.
        Path ours = stagedFile("current", "ours.seq", ageMs(Duration.ofHours(9)));

        int reclaimed = reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(reclaimed).isZero();
        assertThat(fs.exists(ours)).isTrue();
    }

    @Test
    void aPartlyExpiredDirectoryKeepsItsRecentFilesAndItself() throws Exception {
        Path old = stagedFile("host-555", "old.seq", ageMs(Duration.ofHours(9)));
        Path recent = stagedFile("host-555", "recent.seq", ageMs(Duration.ofMinutes(1)));
        leaseAged("host-555", Duration.ofHours(9));

        int reclaimed = reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT);

        assertThat(reclaimed).isEqualTo(1);
        assertThat(fs.exists(old)).isFalse();
        assertThat(fs.exists(recent)).isTrue();
        assertThat(fs.exists(new Path(PartitionPath.tempDir(basePath, "host-555"))))
                .as("files remain, so the directory stays for a later pass")
                .isTrue();
    }

    @Test
    void missingStagingRootIsNotAnError() throws Exception {
        assertThat(reclaimer.reclaimAbandonedInstances(
                tempDir.resolve("never-written").toString(), MAX_AGE, LEASE_TIMEOUT)).isZero();
    }

    // --- the lease itself ---

    @Test
    void aHeldLeaseProtectsTheDirectoryAndReleasingItExposesIt() throws Exception {
        Path theirs = stagedFile("host-666", "batch.seq", ageMs(Duration.ofHours(9)));

        InstanceLease lease = new InstanceLease(
                fs, "host-666", List.of(basePath), LEASE_TIMEOUT);
        lease.renew();

        assertThat(reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT))
                .as("held").isZero();
        assertThat(fs.exists(theirs)).isTrue();

        lease.close();   // a clean shutdown drops the lease

        assertThat(reclaimer.reclaimAbandonedInstances(basePath, MAX_AGE, LEASE_TIMEOUT))
                .as("released").isEqualTo(1);
        assertThat(fs.exists(theirs)).isFalse();
    }

    @Test
    void theLeaseIsWrittenIntoEveryStagingTreeTheInstanceUses() throws Exception {
        // Data and audit trees are separate roots; debris accumulates in both,
        // so both need a claim.
        String auditPath = tempDir.resolve("audit").toString();

        InstanceLease lease = new InstanceLease(
                fs, "host-777", List.of(basePath, auditPath), LEASE_TIMEOUT);
        lease.renew();

        for (String root : List.of(basePath, auditPath)) {
            assertThat(fs.exists(new Path(
                    PartitionPath.tempDir(root, "host-777"), InstanceLease.LEASE_FILENAME)))
                    .as(root).isTrue();
        }
        lease.close();
    }

    // --- helpers ---

    private long ageMs(Duration age) {
        return System.currentTimeMillis() - age.toMillis();
    }

    private Path stagedFile(String instanceId, String name, long modifiedAt) throws IOException {
        Path dir = new Path(PartitionPath.tempDir(basePath, instanceId));
        fs.mkdirs(dir);
        Path file = new Path(dir, name);
        try (FSDataOutputStream out = fs.create(file, true)) {
            out.write("staged".getBytes(StandardCharsets.UTF_8));
        }
        fs.setTimes(file, modifiedAt, -1);
        return file;
    }

    private void leaseAged(String instanceId, Duration age) throws IOException {
        Path lease = new Path(
                PartitionPath.tempDir(basePath, instanceId), InstanceLease.LEASE_FILENAME);
        fs.mkdirs(lease.getParent());
        try (FSDataOutputStream out = fs.create(lease, true)) {
            out.write(instanceId.getBytes(StandardCharsets.UTF_8));
        }
        fs.setTimes(lease, ageMs(age), -1);
    }
}
