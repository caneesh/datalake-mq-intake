package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.AuditRecord;
import com.hcsc.datalake.mqintake.core.audit.AuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.audit.FileClassification;
import com.hcsc.datalake.mqintake.core.audit.IdentityExtractor;
import com.hcsc.datalake.mqintake.core.audit.OrphanFileClassifier;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.reconciliation.AuditRecordReader.ParsedAuditRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Per-binding partition reconciliation (DESIGN §7.1, §10, §12).
 *
 * <p>Flow: partition close + grace period G → enumerate landed SequenceFiles →
 * enumerate audit records → compare counts → classify suspect files by
 * identity-set comparison → alert / quarantine / retrospectively reconcile.
 *
 * <p>Safety rules:
 * <ul>
 *   <li><strong>This service has NO delete path.</strong> A suspect data file
 *       is never deleted automatically. Quarantine is a rename into
 *       {@code {base}/_quarantine/}, and only DUPLICATE-classified files are
 *       eligible. Retention-controlled cleanup, if ever wanted, is a separate
 *       concern outside this service.</li>
 *   <li>SOLE_COPY and INCONCLUSIVE files are always kept in place;
 *       INCONCLUSIVE partitions are retried on a later run.</li>
 *   <li>A partition is not reconciled before close + grace period.</li>
 *   <li>Bindings are reconciled independently — one binding's failure never
 *       affects another's report.</li>
 *   <li>A binding without an approved stable identity (claims, open item #17)
 *       reports NOT_READY and its files are not touched (§12).</li>
 * </ul>
 *
 * <p>Crash-window recovery (§12.1): a file with an audit record is state ≥5;
 * a file without one is state 3 or 4 (externally indistinguishable). A
 * SOLE_COPY file without an audit record gets a <em>retrospective</em> audit
 * record reconstructed from the file itself, closing the commit→audit crash
 * window without touching data.
 */
public class PartitionReconciliationService implements PartitionReconciler {

    private static final Logger log = LoggerFactory.getLogger(PartitionReconciliationService.class);
    private static final Duration PARTITION_LENGTH = PartitionPath.WINDOW;

    private final FileSystem fileSystem;
    private final IdentityExtractor identityReader;
    private final AuditRecordReader auditReader;
    private final OrphanFileClassifier orphanClassifier;
    private final AuditRecordEmitter retrospectiveAuditEmitter; // may be null
    private final Duration gracePeriod;
    private final Clock clock;
    private final String instanceId;

    public PartitionReconciliationService(FileSystem fileSystem,
                                          IdentityExtractor identityReader,
                                          AuditRecordReader auditReader,
                                          AuditRecordEmitter retrospectiveAuditEmitter,
                                          Duration gracePeriod,
                                          Clock clock,
                                          String instanceId) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.identityReader = Objects.requireNonNull(identityReader, "identityReader required");
        this.auditReader = Objects.requireNonNull(auditReader, "auditReader required");
        this.retrospectiveAuditEmitter = retrospectiveAuditEmitter;
        this.gracePeriod = Objects.requireNonNull(gracePeriod, "gracePeriod required");
        this.clock = Objects.requireNonNull(clock, "clock required");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId required");
        this.orphanClassifier = new OrphanFileClassifier(fileSystem, identityReader);
    }

    /**
     * Reconciles the quarter-hour partition containing {@code partitionInstant}.
     *
     * @param bindingId          the binding to reconcile
     * @param basePath           the binding's HDFS base path
     * @param partitionInstant   any instant within the target partition window
     * @param identityApproved   whether this binding has an approved stable
     *                           identity (claims must pass false until open
     *                           item #17 is resolved)
     * @param quarantineDuplicates when true, DUPLICATE-classified orphans are
     *                           moved (never deleted) to {base}/_quarantine/
     * @param metrics            optional metrics sink for discrepancies
     */
    @Override
    public ReconciliationReport reconcilePartition(String bindingId,
                                                   String basePath,
                                                   Instant partitionInstant,
                                                   boolean identityApproved,
                                                   boolean quarantineDuplicates,
                                                   BindingMetrics metrics) {
        String partitionPath = PartitionPath.compute(basePath, partitionInstant);

        if (!identityApproved) {
            log.warn("Binding '{}': reconciliation NOT READY — stable identity unresolved; " +
                    "partition {} untouched", bindingId, partitionPath);
            return ReconciliationReport.notReady(bindingId, partitionPath,
                    "Stable identity unresolved (open item #17) — reconciliation refused");
        }

        Instant partitionClose = partitionCloseInstant(partitionInstant);
        Instant reconcileAfter = partitionClose.plus(gracePeriod);
        Instant now = clock.instant();
        if (now.isBefore(reconcileAfter)) {
            log.debug("Binding '{}': partition {} inside grace period until {}",
                    bindingId, partitionPath, reconcileAfter);
            return ReconciliationReport.skippedGracePeriod(bindingId, partitionPath, reconcileAfter);
        }

        try {
            return doReconcile(bindingId, basePath, partitionPath, partitionInstant,
                    quarantineDuplicates, metrics);
        } catch (IOException e) {
            log.error("Binding '{}': reconciliation of {} failed: {}",
                    bindingId, partitionPath, e.getMessage(), e);
            return ReconciliationReport.error(bindingId, partitionPath, e.getMessage());
        }
    }

    private ReconciliationReport doReconcile(String bindingId,
                                             String basePath,
                                             String partitionPath,
                                             Instant partitionInstant,
                                             boolean quarantineDuplicates,
                                             BindingMetrics metrics) throws IOException {
        List<Discrepancy> discrepancies = new ArrayList<>();
        boolean retryLater = false;

        // Enumerate landed files
        Map<String, FileStatus> filesByName = new HashMap<>();
        Path partition = new Path(partitionPath);
        if (fileSystem.exists(partition)) {
            for (FileStatus status : fileSystem.listStatus(partition)) {
                if (status.isFile() && status.getPath().getName().endsWith(".seq")) {
                    filesByName.put(status.getPath().getName(), status);
                }
            }
        }

        // Enumerate audit records for this partition (commit may land just
        // after midnight for a late partition, so read both days)
        LocalDate date = partitionInstant.atZone(ZoneOffset.UTC).toLocalDate();
        List<ParsedAuditRecord> auditRecords = new ArrayList<>();
        auditRecords.addAll(auditReader.readForDate(bindingId, date));
        auditRecords.addAll(auditReader.readForDate(bindingId, date.plusDays(1)));
        auditRecords.removeIf(r -> !matchesPartition(r.getPartitionPath(), partitionPath));

        int auditedRecordSum = auditRecords.stream().mapToInt(ParsedAuditRecord::getRecordCount).sum();

        // Audit record with no data file → potential loss, loudest alert
        Map<String, ParsedAuditRecord> auditByFilename = new HashMap<>();
        for (ParsedAuditRecord audit : auditRecords) {
            auditByFilename.put(audit.getFilename(), audit);
            if (!filesByName.containsKey(audit.getFilename())) {
                discrepancies.add(new Discrepancy(DiscrepancyType.MISSING_FILE,
                        audit.getFilename(),
                        "Audit record exists but data file is missing from " + partitionPath));
            }
        }

        // Files: count check when audited, identity classification when not.
        // Sorted iteration plus a snapshot the quarantines update keeps a
        // multi-orphan pass deterministic: two mutually-duplicate orphans used
        // to split DUPLICATE/SOLE_COPY by HashMap iteration order racing the
        // per-call re-listing. Now the first (by name) is quarantined and the
        // second — its match gone from the snapshot — is kept as SOLE_COPY
        // with a retrospective audit: exactly one copy survives, always the
        // same one.
        List<String> partitionSnapshot = new ArrayList<>();
        for (FileStatus status : filesByName.values()) {
            partitionSnapshot.add(status.getPath().toString());
        }
        long actualRecordSum = 0;
        for (Map.Entry<String, FileStatus> entry : new java.util.TreeMap<>(filesByName).entrySet()) {
            String filename = entry.getKey();
            String filePath = entry.getValue().getPath().toString();
            ParsedAuditRecord audit = auditByFilename.get(filename);

            int actualCount;
            try {
                actualCount = identityReader.countRecords(filePath);
            } catch (IOException e) {
                discrepancies.add(new Discrepancy(DiscrepancyType.UNREADABLE_FILE,
                        filename, "Cannot read file: " + e.getMessage()));
                retryLater = true;
                continue;
            }
            actualRecordSum += actualCount;

            if (audit != null) {
                if (audit.getRecordCount() != actualCount) {
                    discrepancies.add(new Discrepancy(DiscrepancyType.COUNT_MISMATCH,
                            filename,
                            "Audit says " + audit.getRecordCount() +
                            " records, file contains " + actualCount));
                }
                continue;
            }

            // File with no audit record: crash window state 3/4 — classify
            OrphanFileClassifier.ClassificationResult result =
                    orphanClassifier.classify(filePath, partitionSnapshot);

            if (result.getClassification() == FileClassification.DUPLICATE) {
                String detail = "All identities present in audited files";
                if (quarantineDuplicates) {
                    // Contained like the unreadable-file case above: one failed
                    // quarantine rename (permission gap, stale target) used to
                    // escape as an IOException that aborted the whole
                    // partition's report — discarding every discrepancy
                    // already found in this pass, including MISSING_FILE and
                    // COUNT_MISMATCH findings that had nothing to do with it.
                    try {
                        Path quarantined = quarantine(basePath, entry.getValue().getPath());
                        partitionSnapshot.remove(filePath);
                        detail += " — quarantined (moved) to " + quarantined;
                    } catch (IOException e) {
                        detail += " — quarantine FAILED (" + e.getMessage()
                                + "), file left in place, retrying next pass";
                        retryLater = true;
                    }
                } else {
                    detail += " — quarantine candidate (no action taken)";
                }
                discrepancies.add(new Discrepancy(DiscrepancyType.ORPHAN_DUPLICATE,
                        filename, detail));

            } else if (result.getClassification() == FileClassification.SOLE_COPY) {
                String detail = "Sole copy of " + result.getRelevantIdentities().size() +
                        " identities — KEPT";
                detail += emitRetrospectiveAudit(bindingId, filePath, filename,
                        partitionPath, actualCount);
                discrepancies.add(new Discrepancy(DiscrepancyType.ORPHAN_SOLE_COPY,
                        filename, detail));

            } else {
                discrepancies.add(new Discrepancy(DiscrepancyType.ORPHAN_INCONCLUSIVE,
                        filename, "Classification inconclusive (" + result.getReason() +
                        ") — KEPT, retry on next run"));
                retryLater = true;
            }
        }

        if (metrics != null) {
            for (int i = 0; i < discrepancies.size(); i++) {
                metrics.recordReconciliationDiscrepancy();
            }
        }

        ReconciliationReport report = new ReconciliationReport(
                bindingId, partitionPath,
                discrepancies.isEmpty()
                        ? ReconciliationStatus.CLEAN
                        : ReconciliationStatus.DISCREPANCIES,
                discrepancies, filesByName.size(), auditRecords.size(),
                actualRecordSum, auditedRecordSum, retryLater, null);

        log.info("Binding '{}': reconciled {} — {} files, {} audit records, {} discrepancies",
                bindingId, partitionPath, filesByName.size(), auditRecords.size(),
                discrepancies.size());
        return report;
    }

    /**
     * Quarantine is a MOVE, never a delete. The file lands intact under
     * {base}/_quarantine/ and can be restored by moving it back.
     */
    private Path quarantine(String basePath, Path file) throws IOException {
        Path quarantineDir = new Path(basePath.endsWith("/")
                ? basePath + "_quarantine" : basePath + "/_quarantine");
        fileSystem.mkdirs(quarantineDir);
        Path target = new Path(quarantineDir, file.getName());
        if (!fileSystem.rename(file, target)) {
            throw new IOException("Quarantine move failed: " + file + " -> " + target);
        }
        log.warn("Quarantined duplicate file (moved, not deleted): {} -> {}", file, target);
        return target;
    }

    /**
     * Reconstructs an audit record from the landed file itself, closing the
     * commit→audit crash window (§12.1 state 5 without state 6).
     */
    private String emitRetrospectiveAudit(String bindingId, String filePath,
                                          String filename, String partitionPath,
                                          int recordCount) {
        if (retrospectiveAuditEmitter == null) {
            return "; no audit emitter configured for retrospective record";
        }
        try {
            Set<String> identities = identityReader.extractIdentities(filePath);
            String anyIdentity = identities.isEmpty() ? null : identities.iterator().next();
            AuditRecord record = AuditRecord.builder()
                    .bindingId(bindingId)
                    .partitionPath(partitionPath)
                    .filename(filename)
                    .recordCount(recordCount)
                    .byteCount(fileSystem.getFileStatus(new Path(filePath)).getLen())
                    .firstIdentity(anyIdentity)
                    .lastIdentity(anyIdentity)
                    .instanceId(instanceId + "-reconciliation")
                    .commitTimestamp(clock.instant())
                    .build();
            retrospectiveAuditEmitter.emit(record);
            return "; retrospective audit record emitted";
        } catch (IOException e) {
            log.warn("Failed to emit retrospective audit for {}: {}", filename, e.getMessage());
            return "; retrospective audit emission FAILED: " + e.getMessage();
        }
    }

    private Instant partitionCloseInstant(Instant partitionInstant) {
        // Boundary via PartitionPath.windowId, not re-derived arithmetic: the
        // window math was centralised there precisely so grace-period timing
        // and window enumeration cannot silently disagree.
        long windowStart = Math.multiplyExact(
                PartitionPath.windowId(partitionInstant),
                PARTITION_LENGTH.toMillis());
        return Instant.ofEpochMilli(windowStart).plus(PARTITION_LENGTH);
    }

    private boolean matchesPartition(String auditPartitionPath, String partitionPath) {
        if (auditPartitionPath == null) {
            return false;
        }
        return normalize(auditPartitionPath).equals(normalize(partitionPath));
    }

    private String normalize(String path) {
        String p = path;
        if (p.startsWith("file:")) {
            p = p.substring(5);
        }
        while (p.startsWith("//")) {
            p = p.substring(1);
        }
        return p.endsWith("/") ? p.substring(0, p.length() - 1) : p;
    }

    // --- Report types ---

    public enum ReconciliationStatus {
        CLEAN, DISCREPANCIES, SKIPPED_GRACE_PERIOD, NOT_READY, ERROR
    }

    public enum DiscrepancyType {
        MISSING_FILE, COUNT_MISMATCH, ORPHAN_DUPLICATE,
        ORPHAN_SOLE_COPY, ORPHAN_INCONCLUSIVE, UNREADABLE_FILE
    }

    public static final class Discrepancy {
        private final DiscrepancyType type;
        private final String filename;
        private final String detail;

        public Discrepancy(DiscrepancyType type, String filename, String detail) {
            this.type = type;
            this.filename = filename;
            this.detail = detail;
        }

        public DiscrepancyType getType() { return type; }
        public String getFilename() { return filename; }
        public String getDetail() { return detail; }

        @Override
        public String toString() {
            return type + " " + filename + ": " + detail;
        }
    }

    public static final class ReconciliationReport {
        private final String bindingId;
        private final String partitionPath;
        private final ReconciliationStatus status;
        private final List<Discrepancy> discrepancies;
        private final int fileCount;
        private final int auditRecordCount;
        private final long actualRecordSum;
        private final long auditedRecordSum;
        private final boolean retryLater;
        private final String message;

        ReconciliationReport(String bindingId, String partitionPath,
                             ReconciliationStatus status, List<Discrepancy> discrepancies,
                             int fileCount, int auditRecordCount,
                             long actualRecordSum, long auditedRecordSum,
                             boolean retryLater, String message) {
            this.bindingId = bindingId;
            this.partitionPath = partitionPath;
            this.status = status;
            this.discrepancies = List.copyOf(discrepancies);
            this.fileCount = fileCount;
            this.auditRecordCount = auditRecordCount;
            this.actualRecordSum = actualRecordSum;
            this.auditedRecordSum = auditedRecordSum;
            this.retryLater = retryLater;
            this.message = message;
        }

        static ReconciliationReport skippedGracePeriod(String bindingId, String partitionPath,
                                                       Instant reconcileAfter) {
            return new ReconciliationReport(bindingId, partitionPath,
                    ReconciliationStatus.SKIPPED_GRACE_PERIOD, List.of(), 0, 0, 0, 0, true,
                    "Partition inside grace period until " + reconcileAfter);
        }

        static ReconciliationReport notReady(String bindingId, String partitionPath, String reason) {
            return new ReconciliationReport(bindingId, partitionPath,
                    ReconciliationStatus.NOT_READY, List.of(), 0, 0, 0, 0, true, reason);
        }

        static ReconciliationReport error(String bindingId, String partitionPath, String message) {
            return new ReconciliationReport(bindingId, partitionPath,
                    ReconciliationStatus.ERROR, List.of(), 0, 0, 0, 0, true, message);
        }

        public String getBindingId() { return bindingId; }
        public String getPartitionPath() { return partitionPath; }
        public ReconciliationStatus getStatus() { return status; }
        public List<Discrepancy> getDiscrepancies() { return discrepancies; }
        public int getFileCount() { return fileCount; }
        public int getAuditRecordCount() { return auditRecordCount; }
        public long getActualRecordSum() { return actualRecordSum; }
        public long getAuditedRecordSum() { return auditedRecordSum; }
        public boolean isRetryLater() { return retryLater; }
        public String getMessage() { return message; }
    }
}
