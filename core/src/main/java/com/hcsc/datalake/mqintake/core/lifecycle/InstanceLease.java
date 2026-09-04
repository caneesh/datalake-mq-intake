package com.hcsc.datalake.mqintake.core.lifecycle;

import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The mark a running instance leaves in its own staging directory, saying it
 * is still here.
 *
 * <p>Staging directories are per process instance, and an instance id is
 * derived from hostname and PID — so an ordinary restart produces a new
 * directory and abandons the old one. Startup cleanup only ever swept the
 * instance's own directory, so debris from every previous crash accumulated
 * forever across the data, index and audit trees.
 *
 * <p>The reason cleanup was scoped that way is sound and has not changed: two
 * instances may share a host, and deleting another instance's staging files
 * while it is mid-write would destroy an in-flight batch. What was missing is
 * a way to tell a live peer from an abandoned directory. This is it.
 *
 * <p>The lease is a file whose modification time is refreshed periodically.
 * Nothing reads its contents to make a decision — the timestamp is the signal
 * — but it carries the instance id and a readable time so an operator looking
 * at an orphaned directory can tell whose it was and when it stopped.
 *
 * <p>On a clean shutdown the lease is removed, which marks the directory
 * abandoned immediately rather than after the timeout. A kill leaves it
 * behind, and the timeout covers that case.
 *
 * <p>Failures here are logged and swallowed throughout. A lease that cannot be
 * written costs cleanup, not correctness: the worst outcome is a directory
 * nobody reclaims, which is exactly the behaviour that existed before.
 */
public class InstanceLease implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InstanceLease.class);

    /** The lease file, inside the instance's own staging directory. */
    public static final String LEASE_FILENAME = ".instance-lease";

    /**
     * Refreshes per timeout. Frequent enough that a healthy instance never
     * approaches the deadline over a transient storage blip, rare enough that
     * it is invisible beside the batch writes happening in the same tree.
     */
    static final int REFRESHES_PER_TIMEOUT = 10;

    private final FileSystem fileSystem;
    private final String instanceId;
    private final List<String> basePaths;
    private final long refreshIntervalMs;
    private final Clock clock;

    private volatile ScheduledExecutorService refresher;

    public InstanceLease(FileSystem fileSystem, String instanceId, List<String> basePaths,
                         long leaseTimeoutMs) {
        this(fileSystem, instanceId, basePaths, leaseTimeoutMs, Clock.systemUTC());
    }

    InstanceLease(FileSystem fileSystem, String instanceId, List<String> basePaths,
                  long leaseTimeoutMs, Clock clock) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId required");
        this.basePaths = List.copyOf(Objects.requireNonNull(basePaths, "basePaths required"));
        this.refreshIntervalMs = Math.max(1_000, leaseTimeoutMs / REFRESHES_PER_TIMEOUT);
        this.clock = Objects.requireNonNull(clock, "clock required");
    }

    /** Writes the lease once, then keeps refreshing it until closed. */
    public void start() {
        renew();

        refresher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "instance-lease");
            // Daemon: a lease refresh must never hold up JVM shutdown, and a
            // missed one costs nothing but a later reclamation.
            t.setDaemon(true);
            return t;
        });
        refresher.scheduleWithFixedDelay(
                this::renew, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);

        log.info("Instance lease held for '{}' across {} staging tree(s), refreshed every {}ms",
                instanceId, basePaths.size(), refreshIntervalMs);
    }

    /** Rewrites the lease in every staging tree this instance writes to. */
    void renew() {
        for (String basePath : basePaths) {
            Path lease = leasePath(basePath);
            try {
                fileSystem.mkdirs(lease.getParent());
                try (FSDataOutputStream out = fileSystem.create(lease, true)) {
                    out.write((instanceId + "\n" + Instant.now(clock) + "\n")
                            .getBytes(StandardCharsets.UTF_8));
                }
            } catch (IOException e) {
                // Not fatal. A lease that cannot be written means this
                // instance's directory may be reclaimed once its files are
                // also older than the max age — which is the pre-existing
                // behaviour, not a regression.
                log.warn("Could not refresh the instance lease at {} — this instance's staging "
                        + "directory may be treated as abandoned: {}", lease, e.getMessage());
            }
        }
    }

    /**
     * Stops refreshing and drops the lease.
     *
     * <p>Removing it on a clean shutdown is the point: the directory becomes
     * reclaimable straight away rather than after the timeout, so an ordinary
     * restart cleans up after its predecessor immediately.
     */
    @Override
    public void close() {
        ScheduledExecutorService r = refresher;
        if (r != null) {
            r.shutdownNow();
            refresher = null;
        }
        for (String basePath : basePaths) {
            Path lease = leasePath(basePath);
            try {
                fileSystem.delete(lease, false);
            } catch (IOException e) {
                log.debug("Could not remove the instance lease at {}: {}", lease, e.getMessage());
            }
        }
        log.info("Instance lease released for '{}'", instanceId);
    }

    private Path leasePath(String basePath) {
        return new Path(PartitionPath.tempDir(basePath, instanceId), LEASE_FILENAME);
    }
}
