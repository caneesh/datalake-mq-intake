package com.hcsc.datalake.mqintake.core.audit;

/**
 * Classification result for a file with no audit record.
 *
 * <p>From DESIGN.md §10: A file without an audit record has two possible causes
 * that look identical from the file system:
 * <ol>
 *   <li>Crash after rename, before MQ commit — messages rolled back and were
 *       re-landed elsewhere. This file is a DUPLICATE.</li>
 *   <li>Crash after MQ commit, before audit write — messages were delivered.
 *       This file is the SOLE_COPY.</li>
 * </ol>
 *
 * <p>Classification is determined by message-ID set comparison against other
 * files in the partition. The classifier NEVER deletes — it only returns a
 * classification. Quarantine is a move performed by a separate aged step.
 *
 * <p>States 3 (LANDED) and 4 (MQ_COMMITTED) in §12.1 are externally
 * indistinguishable — that is the whole reason for this classification logic.
 */
public enum FileClassification {

    /**
     * All identity values in this file are present in other files in the partition.
     * This file is a genuine duplicate and can be safely quarantined.
     *
     * <p>Safe action: move to quarantine (not delete), then remove after retention window.
     */
    DUPLICATE("All identities present elsewhere — safe to quarantine"),

    /**
     * At least one identity value in this file is present nowhere else.
     * This file is the sole copy of that data and MUST be kept.
     *
     * <p>Required action: KEEP. Reconcile into the audit store retrospectively.
     * Deleting this file would destroy the only copy of committed payment data.
     */
    SOLE_COPY("Contains unique identities — MUST keep"),

    /**
     * Cannot determine classification conclusively. The partition may still be open,
     * downstream processing may not have completed, or comparison data is incomplete.
     *
     * <p>Required action: leave in place and re-evaluate after grace period G (§7.1).
     * Never delete on an inconclusive result.
     */
    INCONCLUSIVE("Cannot determine — leave in place");

    private final String description;

    FileClassification(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Returns true if this classification indicates the file is safe to quarantine.
     * Only DUPLICATE files may be quarantined.
     */
    public boolean isSafeToQuarantine() {
        return this == DUPLICATE;
    }

    /**
     * Returns true if this classification requires keeping the file.
     * SOLE_COPY files must be kept; INCONCLUSIVE files should also be kept
     * until a conclusive classification is reached.
     */
    public boolean mustKeep() {
        return this == SOLE_COPY || this == INCONCLUSIVE;
    }
}
