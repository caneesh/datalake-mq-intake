package com.hcsc.datalake.mqintake.core.audit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Classifies files with no audit record by comparing identity values.
 *
 * <p>From DESIGN.md §10: Given a data file with no audit record, classify it
 * by comparing its identity values (payload_guid, mq_message_id fallback)
 * against other files in the partition:
 * <ul>
 *   <li>DUPLICATE: All identity values present elsewhere</li>
 *   <li>SOLE_COPY: Any identity value present nowhere else</li>
 *   <li>INCONCLUSIVE: Otherwise (partition still open, etc.)</li>
 * </ul>
 *
 * <p><strong>CRITICAL: This classifier NEVER deletes a file.</strong>
 * It returns a classification only. Quarantine is a move performed by a
 * separate aged step. A file without an audit record may be the only copy
 * of committed payment data — §12.1 states that states 3 (LANDED) and 4
 * (MQ_COMMITTED) are externally indistinguishable.
 *
 * <p>On a payment-bearing path, an extra duplicate is an annoyance;
 * a deleted sole copy is an incident.
 */
public final class OrphanFileClassifier {

    private static final Logger log = LoggerFactory.getLogger(OrphanFileClassifier.class);

    private final FileSystem fileSystem;
    private final IdentityExtractor identityExtractor;

    public OrphanFileClassifier(FileSystem fileSystem, IdentityExtractor identityExtractor) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.identityExtractor = Objects.requireNonNull(identityExtractor, "identityExtractor required");
    }

    /**
     * Classifies a file that has no corresponding audit record.
     *
     * <p>This method NEVER deletes the file. It only returns a classification
     * that tells the caller what the file is. The caller is responsible for
     * deciding what action to take based on the classification.
     *
     * @param suspectFilePath  path to the file with no audit record
     * @param partitionPath    path to the partition containing the file
     * @return classification result
     * @throws IOException if file comparison fails
     */
    public ClassificationResult classify(String suspectFilePath, String partitionPath) throws IOException {
        return classify(suspectFilePath, listSeqFiles(new Path(partitionPath)));
    }

    /**
     * Classifies against a caller-supplied snapshot of the partition's files
     * instead of re-listing HDFS per call.
     *
     * <p>This is what makes a multi-orphan reconciliation pass deterministic:
     * the caller lists the partition once, updates the snapshot itself when it
     * quarantines a file, and every later classification sees exactly that
     * state. With per-call re-listing, the outcome for two mutually-duplicate
     * files depended on map iteration order racing the quarantine renames.
     *
     * @param partitionFilePaths paths of the partition's {@code .seq} files;
     *                           may include the suspect itself (it is excluded
     *                           from the comparison set here)
     */
    public ClassificationResult classify(String suspectFilePath,
                                         java.util.Collection<String> partitionFilePaths)
            throws IOException {
        Path suspect = new Path(suspectFilePath);

        if (!fileSystem.exists(suspect)) {
            log.warn("Suspect file does not exist: {}", suspectFilePath);
            return new ClassificationResult(FileClassification.INCONCLUSIVE,
                    "File does not exist", Set.of(), 0);
        }

        // Extract identities from the suspect file
        Set<String> suspectIdentities;
        try {
            suspectIdentities = identityExtractor.extractIdentities(suspectFilePath);
        } catch (IOException e) {
            log.warn("Failed to extract identities from suspect file {}: {}",
                    suspectFilePath, e.getMessage());
            return new ClassificationResult(FileClassification.INCONCLUSIVE,
                    "Cannot read suspect file: " + e.getMessage(), Set.of(), 0);
        }

        if (suspectIdentities.isEmpty()) {
            log.warn("No identities extracted from suspect file: {}", suspectFilePath);
            return new ClassificationResult(FileClassification.INCONCLUSIVE,
                    "No identities in file", Set.of(), 0);
        }

        // Collect identities from all OTHER files in the snapshot
        Set<String> otherIdentities = collectOtherIdentities(partitionFilePaths, suspect);

        // Compare: check which suspect identities appear elsewhere
        Set<String> foundElsewhere = new HashSet<>();
        Set<String> uniqueToSuspect = new HashSet<>();

        for (String identity : suspectIdentities) {
            if (otherIdentities.contains(identity)) {
                foundElsewhere.add(identity);
            } else {
                uniqueToSuspect.add(identity);
            }
        }

        // Classification logic from §10
        if (uniqueToSuspect.isEmpty() && !foundElsewhere.isEmpty()) {
            // All GUIDs present elsewhere → genuine duplicate
            log.info("File classified as DUPLICATE: {} ({} identities all found elsewhere)",
                    suspectFilePath, foundElsewhere.size());
            return new ClassificationResult(FileClassification.DUPLICATE,
                    "All " + foundElsewhere.size() + " identities found in other files",
                    foundElsewhere, suspectIdentities.size());
        }

        if (!uniqueToSuspect.isEmpty()) {
            // Any GUID present nowhere else → sole copy
            log.warn("File classified as SOLE_COPY: {} ({} unique identities not found elsewhere)",
                    suspectFilePath, uniqueToSuspect.size());
            return new ClassificationResult(FileClassification.SOLE_COPY,
                    uniqueToSuspect.size() + " identities found nowhere else — MUST keep",
                    uniqueToSuspect, suspectIdentities.size());
        }

        // Edge case: empty suspect identities already handled above
        log.info("File classification INCONCLUSIVE: {}", suspectFilePath);
        return new ClassificationResult(FileClassification.INCONCLUSIVE,
                "Cannot determine classification", Set.of(), suspectIdentities.size());
    }

    /**
     * Collects all identities from the snapshot's files EXCEPT the suspect.
     */
    private Set<String> collectOtherIdentities(java.util.Collection<String> partitionFilePaths,
                                               Path excludeFile) {
        Set<String> identities = new HashSet<>();

        // Normalize exclude path for comparison (remove file: prefix and extra slashes)
        String excludeNormalized = normalizePath(excludeFile.toString());

        for (String filePath : partitionFilePaths) {
            // Skip the suspect file by comparing normalized paths
            if (normalizePath(filePath).equals(excludeNormalized)) {
                continue;
            }

            try {
                Set<String> fileIdentities = identityExtractor.extractIdentities(filePath);
                identities.addAll(fileIdentities);
            } catch (IOException e) {
                log.warn("Failed to extract identities from {}: {}",
                        filePath, e.getMessage());
                // Continue with other files — don't fail the whole classification
            }
        }

        return identities;
    }

    /** The 2-arg convenience path: one fresh listing of the partition. */
    private java.util.List<String> listSeqFiles(Path partitionPath) throws IOException {
        java.util.List<String> paths = new java.util.ArrayList<>();
        if (!fileSystem.exists(partitionPath)) {
            return paths;
        }
        for (FileStatus file : fileSystem.listStatus(partitionPath)) {
            if (file.isFile() && file.getPath().toString().endsWith(".seq")) {
                paths.add(file.getPath().toString());
            }
        }
        return paths;
    }

    /**
     * Normalizes a path by removing file: prefix and extra slashes.
     */
    private String normalizePath(String path) {
        if (path.startsWith("file:")) {
            path = path.substring(5);
        }
        while (path.startsWith("//")) {
            path = path.substring(1);
        }
        return path;
    }

    /**
     * Result of classifying an orphan file.
     */
    public static final class ClassificationResult {
        private final FileClassification classification;
        private final String reason;
        private final Set<String> relevantIdentities;
        private final int totalIdentities;

        public ClassificationResult(FileClassification classification, String reason,
                                     Set<String> relevantIdentities, int totalIdentities) {
            this.classification = Objects.requireNonNull(classification);
            this.reason = Objects.requireNonNull(reason);
            this.relevantIdentities = Set.copyOf(relevantIdentities);
            this.totalIdentities = totalIdentities;
        }

        public FileClassification getClassification() {
            return classification;
        }

        public String getReason() {
            return reason;
        }

        /**
         * For DUPLICATE: the identities found elsewhere.
         * For SOLE_COPY: the identities unique to this file.
         * For INCONCLUSIVE: empty.
         */
        public Set<String> getRelevantIdentities() {
            return relevantIdentities;
        }

        public int getTotalIdentities() {
            return totalIdentities;
        }

        /**
         * Returns true if this file is safe to quarantine (move, not delete).
         */
        public boolean isSafeToQuarantine() {
            return classification.isSafeToQuarantine();
        }

        /**
         * Returns true if this file must be kept.
         */
        public boolean mustKeep() {
            return classification.mustKeep();
        }

        @Override
        public String toString() {
            return classification + ": " + reason;
        }
    }
}
