package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.AuditPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A backlog being read must not be visible, or writable, half-loaded.
 *
 * <p>The first caller reads the file into the set. A second caller used to
 * find {@code loaded.add} already false and receive the set mid-read — and if
 * it then retained a window, {@code persist} rewrote the whole file from that
 * partial set, deleting from HDFS the entries the read had not reached. The
 * backlog exists so unresolved partitions keep being examined; truncating it
 * is how they stop being examined, silently.
 *
 * <p>Latent in production today: the reconciliation runner's in-progress flag
 * serialises passes per binding, so two threads do not currently reach one
 * binding at once. That invariant lives in another class and is not this
 * class's to assume.
 */
class PendingPartitionsLoadRaceTest {

    private static final String BINDING = "rms";
    private static final long W1 = Instant.parse("2026-09-05T10:00:00Z").toEpochMilli();
    private static final long W2 = Instant.parse("2026-09-05T10:15:00Z").toEpochMilli();
    private static final long W3 = Instant.parse("2026-09-05T10:30:00Z").toEpochMilli();
    private static final Instant ADDED = Instant.parse("2026-09-05T10:45:00Z");

    private LatchingFileSystem fileSystem;
    private java.nio.file.Path auditDir;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = new LatchingFileSystem(FileSystem.get(conf));
        auditDir = Files.createTempDirectory("pending-race");
        writeBacklog(W1, W2, W3);
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.release.countDown();
        fileSystem.delete(new Path(auditDir.toString()), true);
    }

    @Test
    void aRetainDuringTheLoadCannotTruncateTheBacklog() throws Exception {
        PendingPartitions pending = new PendingPartitions(fileSystem, auditDir.toString());

        // One thread starts the load and is held inside it, holding the set's
        // monitor.
        Thread loader = new Thread(() -> pending.pending(BINDING), "loader");
        loader.start();
        assertThat(fileSystem.insideOpen.await(5, TimeUnit.SECONDS))
                .as("the load reached the file read").isTrue();

        // A second thread retains a window while that read is in flight.
        AtomicBoolean retained = new AtomicBoolean(false);
        Thread retainer = new Thread(() -> {
            pending.retain(BINDING, ADDED);
            retained.set(true);
        }, "retainer");
        retainer.start();
        retainer.join(300);

        assertThat(retained.get())
                .as("retain must wait for the load rather than act on a half-read set")
                .isFalse();

        fileSystem.release.countDown();
        loader.join(5_000);
        retainer.join(5_000);
        assertThat(retained.get()).as("and then complete").isTrue();

        // The file must still hold everything it did, plus the new window.
        assertThat(backlogOnDisk())
                .as("a concurrent retain must never delete entries it never saw")
                .containsExactlyInAnyOrder(W1, W2, W3, ADDED.toEpochMilli());
        assertThat(pending.pending(BINDING)).hasSize(4);
    }

    @Test
    void theBacklogIsReadOncePerBindingAndNotOnEveryCall() throws Exception {
        // The load-once guard had nothing holding it: deleting it left the
        // suite green. Without it every pending/retain/resolved/size call
        // re-reads the backlog file from the cluster — four HDFS reads per
        // partition per pass instead of one per binding per process — and
        // re-logs "resuming N pending partitions" each time, which reads as a
        // restart loop to anyone watching the logs.
        fileSystem.release.countDown();   // nothing needs to block here
        PendingPartitions pending = new PendingPartitions(fileSystem, auditDir.toString());

        pending.pending(BINDING);
        pending.size(BINDING);
        pending.retain(BINDING, ADDED);
        pending.resolved(BINDING, ADDED);
        pending.pending(BINDING);

        assertThat(fileSystem.opens.get())
                .as("read once, then held in memory").isEqualTo(1);
    }

    // --- harness ---

    private void writeBacklog(long... windows) throws IOException {
        Path path = new Path(AuditPaths.pendingFile(auditDir.toString(), BINDING));
        fileSystem.mkdirs(path.getParent());
        StringBuilder content = new StringBuilder();
        for (long window : windows) {
            content.append(window).append('\n');
        }
        try (FSDataOutputStream out = fileSystem.create(path, true)) {
            out.write(content.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    private List<Long> backlogOnDisk() throws IOException {
        Path path = new Path(AuditPaths.pendingFile(auditDir.toString(), BINDING));
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(fileSystem.open(path), StandardCharsets.UTF_8))) {
            return reader.lines()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .map(Long::parseLong)
                    .collect(java.util.stream.Collectors.toList());
        }
    }

    /**
     * Holds the first read open until released, so "during the load" is a real
     * moment rather than a hoped-for interleaving.
     */
    private static class LatchingFileSystem extends FilterFileSystem {
        final CountDownLatch insideOpen = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicInteger opens =
                new java.util.concurrent.atomic.AtomicInteger();
        private final AtomicBoolean latchNextOpen = new AtomicBoolean(true);

        LatchingFileSystem(FileSystem delegate) {
            super(delegate);
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize) throws IOException {
            opens.incrementAndGet();
            if (latchNextOpen.compareAndSet(true, false)) {
                insideOpen.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.open(f, bufferSize);
        }
    }
}
