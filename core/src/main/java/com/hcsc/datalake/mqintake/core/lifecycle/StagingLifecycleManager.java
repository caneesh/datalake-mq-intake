package com.hcsc.datalake.mqintake.core.lifecycle;

import com.hcsc.datalake.mqintake.core.audit.AuditPaths;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This instance's claim on its staging directories, and the sweep of what
 * earlier runs left in them.
 *
 * <p>Extracted from {@code IntakeRuntimeManager} unchanged. The two halves live
 * together because their ORDER is the invariant: the lease is written before
 * anything is swept, so that two instances starting at the same moment each see
 * the other's claim before either decides a directory is abandoned. As two
 * adjacent calls in a sixty-line startup method that ordering was a comment; as
 * one method it is the only way to call this class.
 *
 * <p>The claim is held for the life of the process and dropped on a clean
 * shutdown, which is what lets an ordinary restart reclaim its predecessor's
 * directory immediately rather than after the lease timeout. A kill leaves the
 * lease behind, and the timeout is what covers that case.
 *
 * <p>Every sweep failure is logged and swallowed, exactly as before: debris
 * that cannot be removed is a housekeeping problem, never a reason to refuse to
 * start consuming.
 */
public class StagingLifecycleManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StagingLifecycleManager.class);

    private final FileSystem fileSystem;
    private final String instanceId;
    private final IntakeProperties.HdfsProperties hdfs;

    /** Null until {@link #claim} takes one, and again once it is released. */
    private volatile InstanceLease lease;

    public StagingLifecycleManager(FileSystem fileSystem, String instanceId,
                                   IntakeProperties.HdfsProperties hdfs) {
        this.fileSystem = fileSystem;
        this.instanceId = instanceId;
        this.hdfs = hdfs;
    }

    /**
     * Claims this instance's staging trees, then sweeps them — in that order,
     * which is the whole reason the two are one call.
     */
    public void claim(List<BindingConfig> bindings) {
        takeInstanceLease(bindings);
        cleanupTempFiles(bindings);
    }

    /**
     * Drops the claim.
     *
     * <p>Idempotent, because more than one shutdown path reaches it: a failed
     * startup rolls back, and Spring may still close the context afterwards.
     */
    @Override
    public void close() {
        InstanceLease held = lease;
        if (held != null) {
            held.close();
            lease = null;
        }
    }

    /** Claims this instance's staging directories for as long as it runs. */
    private void takeInstanceLease(List<BindingConfig> bindings) {
        List<String> roots = stagingRoots(bindings);
        if (roots.isEmpty()) {
            return;
        }
        lease = new InstanceLease(fileSystem, instanceId, roots,
                hdfs.getInstanceLeaseTimeoutMs());
        lease.start();
    }

    /** Every tree this instance stages files under, data and audit alike. */
    private List<String> stagingRoots(List<BindingConfig> bindings) {
        List<String> roots = new ArrayList<>();
        String auditBasePath = hdfs.getAuditBasePath();
        for (BindingConfig binding : bindings) {
            roots.add(binding.getHdfs().getBasePath());
            if (auditBasePath != null && !auditBasePath.isBlank()) {
                roots.add(AuditPaths.bindingDir(auditBasePath, binding.getId()));
            }
        }
        return roots;
    }

    private void cleanupTempFiles(List<BindingConfig> bindings) {
        String resolvedInstanceId = this.instanceId;
        long maxAge = hdfs.getTempFileMaxAgeMs();
        long leaseTimeout = hdfs.getInstanceLeaseTimeoutMs();
        String auditBasePath = hdfs.getAuditBasePath();

        for (BindingConfig binding : bindings) {
            StagingAreaReclaimer reclaimer = new StagingAreaReclaimer(fileSystem, resolvedInstanceId);
            try {
                int deleted = reclaimer.cleanupInstanceTempFiles(binding.getHdfs().getBasePath(), maxAge);
                if (deleted > 0) {
                    log.info("Cleaned up {} stale temp files for binding '{}'", deleted, binding.getId());
                }
            } catch (IOException e) {
                log.warn("Failed to cleanup temp files for binding '{}': {}",
                        binding.getId(), e.getMessage());
            }

            // Directories belonging to instances that are gone. Own directory
            // above, everyone else's here, and only where a stale lease and an
            // expired file agree.
            try {
                int reclaimed = reclaimer.reclaimAbandonedInstances(
                        binding.getHdfs().getBasePath(), maxAge, leaseTimeout);
                if (reclaimed > 0) {
                    log.info("Reclaimed {} file(s) from abandoned instances for binding '{}'",
                            reclaimed, binding.getId());
                }
            } catch (IOException e) {
                log.warn("Failed to reclaim abandoned instance directories for binding '{}': {}",
                        binding.getId(), e.getMessage());
            }

            // The audit emitter stages under the same _tmp/{instanceId}
            // convention in its own tree; sweep that too, or a crash between
            // stage and rename leaves debris nothing ever removes.
            if (auditBasePath != null && !auditBasePath.isBlank()) {
                try {
                    int deleted = reclaimer.cleanupInstanceTempFiles(
                            AuditPaths.bindingDir(auditBasePath, binding.getId()),
                            maxAge);
                    if (deleted > 0) {
                        log.info("Cleaned up {} stale audit temp files for binding '{}'",
                                deleted, binding.getId());
                    }
                } catch (IOException e) {
                    log.warn("Failed to cleanup audit temp files for binding '{}': {}",
                            binding.getId(), e.getMessage());
                }
                try {
                    reclaimer.reclaimAbandonedInstances(
                            AuditPaths.bindingDir(auditBasePath, binding.getId()),
                            maxAge, leaseTimeout);
                } catch (IOException e) {
                    log.warn("Failed to reclaim abandoned audit staging for binding '{}': {}",
                            binding.getId(), e.getMessage());
                }
            }
        }
    }
}
