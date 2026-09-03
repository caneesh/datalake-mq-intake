package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.index.RecordIndexIdentityExtractor;
import com.hcsc.datalake.mqintake.core.index.RecordIndexReader;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Function;

/**
 * Assembles the reconciliation object graph: audit reader, identity reader
 * chain, reconciliation service, and the scheduler that drives them.
 *
 * <p>Construction lives here, next to the classes being constructed, so that
 * {@code IntakeRuntimeManager} stays what it is — a lifecycle orchestrator
 * that starts and stops things — rather than also being the one place that
 * knows how five reconciliation collaborators fit together.
 */
public final class ReconciliationFactory {

    private ReconciliationFactory() {
    }

    /**
     * Builds the identity reader the scheduler runs with.
     *
     * <p>Named separately from {@link #createScheduler} so it can be tested on
     * its own. This chain is what production reconciliation actually uses, and
     * it was previously reachable only through the scheduler — so the
     * reconciliation service tests wired a different chain by hand and the two
     * drifted apart unnoticed. Whatever else changes here, a test must be able
     * to ask this method what production gets.
     *
     * <p>Identity comes from the sidecar index where a binding writes one,
     * falling back to the file reader — which, under the production key, finds
     * nothing and says so. The record COUNT always comes from reading the
     * file; see {@link RecordIndexIdentityExtractor}.
     */
    static RecordIndexIdentityExtractor createIdentityReader(FileSystem fileSystem,
                                                             Configuration hadoopConf) {
        return new RecordIndexIdentityExtractor(
                new RecordIndexReader(fileSystem),
                new SequenceFileIdentityReader(hadoopConf));
    }

    /**
     * Builds a scheduler ready to {@code start()}.
     *
     * @param metricsLookup resolves a binding's metrics so discrepancies can
     *                      be counted against the binding they belong to
     */
    public static ReconciliationScheduler createScheduler(FileSystem fileSystem,
                                                          Configuration hadoopConf,
                                                          IntakeProperties properties,
                                                          String instanceId,
                                                          Function<String, BindingMetrics> metricsLookup,
                                                          Clock clock) {
        String auditBasePath = properties.getHdfs().getAuditBasePath();

        AuditRecordReader auditReader = new AuditRecordReader(fileSystem, auditBasePath);

        RecordIndexIdentityExtractor identityReader =
                createIdentityReader(fileSystem, hadoopConf);

        PartitionReconciliationService service = new PartitionReconciliationService(
                fileSystem,
                identityReader,
                auditReader,
                new HdfsAuditRecordEmitter(fileSystem, auditBasePath, instanceId, clock),
                Duration.ofMillis(properties.getReconciliation().getGracePeriodMs()),
                clock,
                instanceId);

        return new ReconciliationScheduler(service, properties, metricsLookup, clock);
    }
}
