package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.AuditPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The backlog that keeps an unresolved partition on the schedule.
 *
 * <p>Without it, reconciliation rebuilt its work from the last few closed
 * windows only, so anything still unresolved when it aged past that range
 * stopped being checked — the discrepancy stayed, the checking stopped.
 */
class PendingPartitionsTest {

    private static final String BINDING = "rms";
    private static final Instant W1 = Instant.parse("2026-03-04T10:00:00Z");
    private static final Instant W2 = Instant.parse("2026-03-04T10:15:00Z");
    private static final Instant W3 = Instant.parse("2026-03-04T10:30:00Z");

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem fs;
    private String auditBasePath;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fs = FileSystem.getLocal(conf);
        auditBasePath = tempDir.resolve("audit").toString();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fs != null) {
            fs.close();
        }
    }

    @Test
    void anUnresolvedPartitionIsRememberedAndAResolvedOneIsNot() {
        PendingPartitions pending = new PendingPartitions(fs, auditBasePath);

        pending.retain(BINDING, W1);
        pending.retain(BINDING, W2);
        assertThat(pending.pending(BINDING)).containsExactly(W1, W2);

        pending.resolved(BINDING, W1);
        assertThat(pending.pending(BINDING)).containsExactly(W2);
    }

    @Test
    void theBacklogSurvivesARestart() {
        // The whole point: the outages that produce a backlog are the ones
        // that end in a restart, so an in-memory list would be emptied by
        // exactly the event that filled it.
        new PendingPartitions(fs, auditBasePath).retain(BINDING, W1);

        PendingPartitions afterRestart = new PendingPartitions(fs, auditBasePath);

        assertThat(afterRestart.pending(BINDING)).containsExactly(W1);
    }

    @Test
    void resolvingAPartitionSurvivesARestartToo() {
        PendingPartitions before = new PendingPartitions(fs, auditBasePath);
        before.retain(BINDING, W1);
        before.resolved(BINDING, W1);

        assertThat(new PendingPartitions(fs, auditBasePath).pending(BINDING)).isEmpty();
    }

    @Test
    void partitionsComeBackOldestFirst() {
        PendingPartitions pending = new PendingPartitions(fs, auditBasePath);
        pending.retain(BINDING, W3);
        pending.retain(BINDING, W1);
        pending.retain(BINDING, W2);

        assertThat(pending.pending(BINDING)).containsExactly(W1, W2, W3);
    }

    @Test
    void oneBindingsBacklogIsNotAnothers() {
        PendingPartitions pending = new PendingPartitions(fs, auditBasePath);
        pending.retain("rms", W1);

        assertThat(pending.pending("claims")).isEmpty();
    }

    @Test
    void theBacklogIsBoundedAndSaysWhatItDropped() {
        // An unresolvable partition retries forever by design, so the list of
        // them has to be capped. The oldest goes, and it is logged at ERROR —
        // a silently truncated to-do list reads as "nothing left to do".
        PendingPartitions pending = new PendingPartitions(fs, auditBasePath, 2);

        pending.retain(BINDING, W1);
        pending.retain(BINDING, W2);
        pending.retain(BINDING, W3);

        assertThat(pending.pending(BINDING))
                .as("oldest dropped, newest kept")
                .containsExactly(W2, W3);
    }

    @Test
    void aTornBacklogFileKeepsTheEntriesThatParse() throws Exception {
        // A crash mid-write can leave a partial last line. Discarding the
        // whole backlog for one bad entry would lose the windows it was
        // written to protect.
        Path path = new Path(AuditPaths.pendingFile(auditBasePath, BINDING));
        fs.mkdirs(path.getParent());
        try (FSDataOutputStream out = fs.create(path, true)) {
            out.write((W1.toEpochMilli() + "\n" + W2.toEpochMilli() + "\n17760")
                    .getBytes(StandardCharsets.UTF_8));
        }
        // "17760" parses as a long, so make the tail genuinely unreadable.
        try (FSDataOutputStream out = fs.create(path, true)) {
            out.write((W1.toEpochMilli() + "\n" + W2.toEpochMilli() + "\n17760177x")
                    .getBytes(StandardCharsets.UTF_8));
        }

        assertThat(new PendingPartitions(fs, auditBasePath).pending(BINDING))
                .containsExactly(W1, W2);
    }

    @Test
    void anUnreadableBacklogDoesNotStopReconciliation() throws Exception {
        // Losing this file costs coverage of some old windows, not
        // correctness. Failing here would let a bookkeeping problem stop the
        // check itself.
        Path path = new Path(AuditPaths.pendingFile(auditBasePath, BINDING));
        fs.mkdirs(path);   // a DIRECTORY where the file should be

        PendingPartitions pending = new PendingPartitions(fs, auditBasePath);

        assertThat(pending.pending(BINDING)).isEmpty();
        pending.retain(BINDING, W1);   // must not throw either
        assertThat(pending.pending(BINDING)).containsExactly(W1);
    }
}
