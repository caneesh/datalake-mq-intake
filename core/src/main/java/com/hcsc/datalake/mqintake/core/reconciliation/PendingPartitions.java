package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.AuditPaths;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Partitions that were examined but could not be resolved, kept until they are.
 *
 * <p>Reconciliation used to rebuild its work solely from the last
 * {@code lookbackWindows} closed partitions. Anything still unresolved when it
 * aged past that range simply stopped being checked — a long outage, or one
 * stubborn file, and the partition left the schedule silently with its
 * discrepancy unresolved. {@code ReconciliationReport.isRetryLater()} was set
 * for exactly this and had no consumer at all.
 *
 * <p>Held on HDFS under the binding's audit directory rather than in memory
 * alone, because the outages that produce a backlog are also the ones that end
 * in a restart. The audit tree is already per binding, already on the cluster,
 * and already swept for staging debris at startup.
 *
 * <p><strong>Not a control.</strong> Losing this file costs coverage of some
 * old windows, not correctness, so every failure here is logged and swallowed:
 * reconciliation continues against whatever the in-memory set holds. A file
 * torn by a crash mid-write is read line by line, keeping what parses. The
 * alternative — failing reconciliation because its to-do list was unreadable —
 * would let a bookkeeping problem stop the check entirely.
 */
public class PendingPartitions {

    private static final Logger log = LoggerFactory.getLogger(PendingPartitions.class);

    /**
     * The most partitions one binding may carry.
     *
     * <p>At a quarter-hour window this is more than five days of continuously
     * unresolved partitions — far past the point where the backlog is the
     * problem rather than the symptom. Bounded because an unresolvable
     * partition retries forever by design, and an unbounded list of them would
     * grow without limit; the oldest is dropped, and dropping is logged at
     * ERROR rather than done quietly.
     */
    static final int DEFAULT_MAX_ENTRIES = 512;

    private final FileSystem fileSystem;
    private final String auditBasePath;
    private final int maxEntries;

    /** Epoch millis of each pending window, ordered oldest first. */
    private final Map<String, TreeSet<Long>> byBinding = new ConcurrentHashMap<>();
    private final Set<String> loaded = ConcurrentHashMap.newKeySet();

    public PendingPartitions(FileSystem fileSystem, String auditBasePath) {
        this(fileSystem, auditBasePath, DEFAULT_MAX_ENTRIES);
    }

    PendingPartitions(FileSystem fileSystem, String auditBasePath, int maxEntries) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.auditBasePath = Objects.requireNonNull(auditBasePath, "auditBasePath required");
        this.maxEntries = maxEntries;
    }

    /**
     * The partitions still awaiting resolution for a binding, oldest first.
     *
     * <p>Reads from HDFS the first time a binding is asked about, so a restart
     * resumes the backlog rather than starting empty.
     */
    public List<Instant> pending(String bindingId) {
        TreeSet<Long> entries = entriesFor(bindingId);
        List<Instant> windows = new ArrayList<>(entries.size());
        synchronized (entries) {
            for (Long millis : entries) {
                windows.add(Instant.ofEpochMilli(millis));
            }
        }
        return windows;
    }

    /** Records that a partition still needs another look. */
    public void retain(String bindingId, Instant window) {
        TreeSet<Long> entries = entriesFor(bindingId);
        boolean changed;
        synchronized (entries) {
            changed = entries.add(window.toEpochMilli());
            while (entries.size() > maxEntries) {
                Long dropped = entries.pollFirst();
                log.error("Binding '{}': pending-partition backlog is full at {} entries — "
                                + "dropping the oldest, {}. It will not be re-examined. A "
                                + "backlog this size means partitions are not being resolved, "
                                + "not that the limit is too low.",
                        bindingId, maxEntries, Instant.ofEpochMilli(dropped));
                changed = true;
            }
        }
        if (changed) {
            persist(bindingId, entries);
        }
    }

    /** Records that a partition is resolved and needs no further passes. */
    public void resolved(String bindingId, Instant window) {
        TreeSet<Long> entries = entriesFor(bindingId);
        boolean changed;
        synchronized (entries) {
            changed = entries.remove(window.toEpochMilli());
        }
        if (changed) {
            persist(bindingId, entries);
        }
    }

    /** How many partitions a binding is carrying; published for alerting. */
    public int size(String bindingId) {
        TreeSet<Long> entries = entriesFor(bindingId);
        synchronized (entries) {
            return entries.size();
        }
    }

    private TreeSet<Long> entriesFor(String bindingId) {
        TreeSet<Long> entries = byBinding.computeIfAbsent(bindingId, id -> new TreeSet<>());
        if (loaded.add(bindingId)) {
            load(bindingId, entries);
        }
        return entries;
    }

    private void load(String bindingId, TreeSet<Long> entries) {
        Path path = new Path(AuditPaths.pendingFile(auditBasePath, bindingId));
        try {
            if (!fileSystem.exists(path)) {
                return;
            }
            int unreadableLines = 0;
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(path), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    try {
                        synchronized (entries) {
                            entries.add(Long.parseLong(trimmed));
                        }
                    } catch (NumberFormatException e) {
                        // A crash mid-write can leave a torn last line. Keep
                        // every window that parses rather than discarding the
                        // whole backlog for one bad entry.
                        unreadableLines++;
                    }
                }
            }
            if (unreadableLines > 0) {
                log.warn("Binding '{}': {} unreadable line(s) in the pending-partition backlog "
                        + "at {} — kept the {} that parsed", bindingId, unreadableLines, path,
                        entries.size());
            }
            if (!entries.isEmpty()) {
                log.info("Binding '{}': resuming {} pending partition(s) from a previous run",
                        bindingId, entries.size());
            }
        } catch (IOException e) {
            log.warn("Binding '{}': could not read the pending-partition backlog at {} — "
                            + "continuing without it, so only the recent windows are checked "
                            + "until something is added: {}", bindingId, path, e.getMessage());
        }
    }

    private void persist(String bindingId, TreeSet<Long> entries) {
        Path path = new Path(AuditPaths.pendingFile(auditBasePath, bindingId));
        StringBuilder content = new StringBuilder();
        synchronized (entries) {
            for (Long millis : entries) {
                content.append(millis).append('\n');
            }
        }
        try {
            fileSystem.mkdirs(path.getParent());
            // Written in place rather than staged and renamed. The file is
            // rewritten whole on every change and read only at startup, so the
            // worst a torn write costs is some of the backlog — which load()
            // recovers around. Staging would need an instance id here purely
            // to protect a recovery aid.
            try (FSDataOutputStream out = fileSystem.create(path, true)) {
                out.write(content.toString().getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            log.warn("Binding '{}': could not write the pending-partition backlog to {} — the "
                            + "in-memory list is unaffected, but it will not survive a restart: "
                            + "{}", bindingId, path, e.getMessage());
        }
    }
}
