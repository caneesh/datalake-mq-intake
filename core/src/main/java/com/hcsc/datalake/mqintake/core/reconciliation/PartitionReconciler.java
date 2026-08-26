package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;

import java.time.Instant;

/**
 * Checks one partition's landed data against the audit trail.
 *
 * <p>The scheduler needs a thing that reconciles a partition, not a particular
 * way of doing it. Separating the two also means the schedule — intervals,
 * overlap, failure containment — can be tested without a filesystem, which is
 * where the behaviour that protects ingestion actually lives.
 */
public interface PartitionReconciler {

    /**
     * @param identityApproved whether this binding records a trustworthy
     *        per-message identity. False makes the reconciler refuse rather
     *        than compare against identities it does not have.
     * @param quarantineDuplicates when true, duplicate-classified orphans are
     *        moved — never deleted — to {@code {base}/_quarantine/}
     */
    PartitionReconciliationService.ReconciliationReport reconcilePartition(
            String bindingId,
            String basePath,
            Instant partitionInstant,
            boolean identityApproved,
            boolean quarantineDuplicates,
            BindingMetrics metrics);
}
