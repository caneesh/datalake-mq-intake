package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.audit.IdentityExtractor;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.index.RecordIndexIdentityExtractor;
import com.hcsc.datalake.mqintake.core.index.RecordIndexReader;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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
     * <p>Named separately from {@link #createScheduler}, and public, so tests
     * — including integration tests in the binding modules — bind against the
     * chain production runs rather than assembling a lookalike. This chain is
     * what production reconciliation actually uses, and it was previously
     * reachable only through the scheduler — so the reconciliation service
     * tests wired a different chain by hand and the two drifted apart
     * unnoticed. Whatever else changes here, a test must be able to ask this
     * method what production gets.
     *
     * <p>Identity comes from the sidecar index where a binding writes one,
     * falling back to the file reader. The record COUNT always comes from
     * reading the file; see {@link RecordIndexIdentityExtractor}.
     */
    public static RecordIndexIdentityExtractor createIdentityReader(FileSystem fileSystem,
                                                                    Configuration hadoopConf) {
        return createIdentityReader(fileSystem, hadoopConf, null);
    }

    /**
     * The identity chain for one binding: sidecar index first, then the file
     * itself read through the binding's own value extractor.
     *
     * @param serializer the binding's serializer, whose {@code identityOf}
     *                   recovers identity from a landed value; null selects the
     *                   key-based fallback that finds nothing in production files
     */
    public static RecordIndexIdentityExtractor createIdentityReader(FileSystem fileSystem,
                                                                    Configuration hadoopConf,
                                                                    RecordSerializer serializer) {
        Function<String, String> valueIdentity =
                serializer != null && serializer.providesIdentity() ? serializer::identityOf : null;
        return new RecordIndexIdentityExtractor(
                new RecordIndexReader(fileSystem),
                new SequenceFileIdentityReader(hadoopConf, valueIdentity));
    }

    /**
     * Builds a scheduler ready to {@code start()}.
     *
     * @param metricsLookup    resolves a binding's metrics so discrepancies can
     *                         be counted against the binding they belong to
     * @param serializerLookup the binding's serializer, or null when it cannot
     *                         be built; decides whether identity can be
     *                         recovered from landed files
     */
    public static ReconciliationScheduler createScheduler(FileSystem fileSystem,
                                                          Configuration hadoopConf,
                                                          IntakeProperties properties,
                                                          String instanceId,
                                                          Function<String, BindingMetrics> metricsLookup,
                                                          Function<BindingConfig, RecordSerializer> serializerLookup,
                                                          Clock clock) {
        String auditBasePath = properties.getHdfs().getAuditBasePath();

        AuditRecordReader auditReader = new AuditRecordReader(fileSystem, auditBasePath);

        Map<String, IdentityExtractor> readers = new HashMap<>();
        Map<String, Boolean> identityAvailable = new HashMap<>();
        for (BindingConfig binding : properties.getBindings()) {
            RecordSerializer serializer = serializerLookup.apply(binding);
            readers.put(binding.getId(), createIdentityReader(fileSystem, hadoopConf, serializer));
            identityAvailable.put(binding.getId(),
                    serializer != null && serializer.providesIdentity());
        }
        IdentityExtractor fallback = createIdentityReader(fileSystem, hadoopConf);

        PartitionReconciliationService service = new PartitionReconciliationService(
                fileSystem,
                id -> readers.getOrDefault(id, fallback),
                auditReader,
                new HdfsAuditRecordEmitter(fileSystem, auditBasePath, instanceId, clock),
                Duration.ofMillis(properties.getReconciliation().getGracePeriodMs()),
                clock,
                instanceId);

        return new ReconciliationScheduler(service, properties, metricsLookup, clock,
                new PendingPartitions(fileSystem, auditBasePath),
                id -> identityAvailable.getOrDefault(id, false));
    }
}
