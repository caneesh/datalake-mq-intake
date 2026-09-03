package com.hcsc.datalake.mqintake.rms.integration;

import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import com.hcsc.datalake.mqintake.core.index.HdfsRecordIndexWriter;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.reconciliation.AuditRecordReader;
import com.hcsc.datalake.mqintake.core.reconciliation.PartitionReconciliationService;
import com.hcsc.datalake.mqintake.core.reconciliation.ReconciliationFactory;
import com.hcsc.datalake.mqintake.rms.serializer.RmsRecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Session;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reconciliation end to end on the RMS production path.
 *
 * <p>Everything here is the real thing: the real {@link RmsRecordSerializer}
 * (so files carry a {@code LongWritable} byte offset and a {@code Text}
 * payload), the real {@link SequenceFileBatchWriter} with its sidecar index,
 * the real {@link HdfsAuditRecordEmitter}, and the real
 * {@code ReconciliationFactory} wiring. Nothing is stubbed but the filesystem,
 * which is a local one.
 *
 * <p>It exists because the unit-level reconciliation suite proves less than it
 * appears to: it builds its own identity chain by hand, over fixtures using
 * {@code Text} metadata keys — the abandoned Option A layout that
 * {@code ProductionLayoutFingerprintTest} forbids. So the count check it
 * exercised was one production could not perform. This drives the production
 * wiring over production files instead.
 */
class RmsReconciliationIntegrationTest {

    private static final String BINDING = "rms";
    private static final Instant PARTITION_INSTANT =
            Instant.parse("2026-03-04T10:05:00Z");

    @TempDir
    java.nio.file.Path tempDir;

    private Configuration conf;
    private FileSystem fs;
    private Connection jmsConnection;
    private Session jmsSession;

    private String basePath;
    private String auditBasePath;
    private HdfsAuditRecordEmitter auditEmitter;
    private PartitionReconciliationService reconciler;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fs = FileSystem.getLocal(conf);

        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        jmsConnection = factory.createConnection();
        jmsConnection.start();
        jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        basePath = tempDir.resolve("data").toString();
        auditBasePath = tempDir.resolve("audit").toString();

        // Reconcile well after the partition closed, so the grace period never
        // decides the outcome of a test about counts.
        Clock afterGrace = Clock.fixed(PARTITION_INSTANT.plus(Duration.ofHours(2)), ZoneOffset.UTC);

        auditEmitter = new HdfsAuditRecordEmitter(fs, auditBasePath, "it-instance", afterGrace);
        reconciler = new PartitionReconciliationService(
                fs,
                ReconciliationFactory.createIdentityReader(fs, conf),
                new AuditRecordReader(fs, auditBasePath),
                auditEmitter,
                Duration.ofMinutes(5),
                afterGrace,
                "it-instance");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (jmsSession != null) jmsSession.close();
        if (jmsConnection != null) jmsConnection.close();
        if (fs != null) fs.close();
    }

    @Test
    void aLandedAndAuditedPartitionReconcilesClean() throws Exception {
        landBatch(5);

        PartitionReconciliationService.ReconciliationReport report = reconcile();

        assertThat(report.getDiscrepancies())
                .as("a partition written and audited by the production path balances")
                .isEmpty();
    }

    @Test
    void aFileTruncatedAfterLandingIsReportedAsCountMismatch() throws Exception {
        // The failure reconciliation exists for, and the one it could not see
        // while countRecords() answered from the sidecar: the audit record and
        // the index are both built from the writer's single indexEntries list,
        // so they agreed with each other no matter what the file held.
        BatchWriter.BatchWriteResult landed = landBatch(6);

        rewriteWithFewerRecords(new Path(landed.getFilePath()), 2);

        PartitionReconciliationService.ReconciliationReport report = reconcile();

        assertThat(report.getDiscrepancies()).hasSize(1);
        assertThat(report.getDiscrepancies().get(0).getType())
                .isEqualTo(PartitionReconciliationService.DiscrepancyType.COUNT_MISMATCH);
        assertThat(report.getDiscrepancies().get(0).getDetail())
                .contains("Audit says 6")
                .contains("file contains 2");
    }

    @Test
    void aFileThatBecameUnreadableIsReportedRatherThanIgnored() throws Exception {
        // Same blind spot, different symptom: UNREADABLE_FILE is raised only
        // from the read that was being skipped, so an audited file that lost
        // its content reconciled clean.
        BatchWriter.BatchWriteResult landed = landBatch(3);

        try (org.apache.hadoop.fs.FSDataOutputStream out =
                     fs.create(new Path(landed.getFilePath()), true)) {
            out.write("not a sequence file".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        PartitionReconciliationService.ReconciliationReport report = reconcile();

        assertThat(report.getDiscrepancies()).hasSize(1);
        assertThat(report.getDiscrepancies().get(0).getType())
                .isEqualTo(PartitionReconciliationService.DiscrepancyType.UNREADABLE_FILE);
    }

    @Test
    void anAuditRecordWhoseFileVanishedIsTheLoudestFinding() throws Exception {
        BatchWriter.BatchWriteResult landed = landBatch(4);

        fs.delete(new Path(landed.getFilePath()), false);

        PartitionReconciliationService.ReconciliationReport report = reconcile();

        assertThat(report.getDiscrepancies()).hasSize(1);
        assertThat(report.getDiscrepancies().get(0).getType())
                .isEqualTo(PartitionReconciliationService.DiscrepancyType.MISSING_FILE);
    }

    @Test
    void identitiesComeFromTheSidecarTheRealWriterProduced() throws Exception {
        // The other half of the split: the count is read from the file, the
        // identities from the index — and the index here was written by the
        // production writer from the production serializer, not by a fixture.
        BatchWriter.BatchWriteResult landed = landBatch(3);

        java.util.Set<String> identities =
                ReconciliationFactory.createIdentityReader(fs, conf)
                        .extractIdentities(landed.getFilePath());

        assertThat(identities).containsExactlyInAnyOrder("guid-0", "guid-1", "guid-2");
    }

    /** Writes one batch through the production writer, then audits it. */
    private BatchWriter.BatchWriteResult landBatch(int messages) throws Exception {
        Clock atPartition = Clock.fixed(PARTITION_INSTANT, ZoneOffset.UTC);

        SequenceFileBatchWriter writer = new SequenceFileBatchWriter(
                fs, conf, new RmsRecordSerializer(), "it-instance", atPartition,
                SequenceFile.CompressionType.RECORD,
                java.util.Map.of(BINDING, basePath),
                new HdfsRecordIndexWriter(fs, "it-instance"),
                false);   // hflush: a local filesystem has no DataNode to hsync

        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < messages; i++) {
            batch.add(jmsSession.createTextMessage(
                    "<Msg><MessageID>guid-" + i + "</MessageID><Body>row " + i + "</Body></Msg>"));
        }

        BatchWriter.BatchWriteResult result = writer.write(BINDING, batch);
        auditEmitter.emit(BINDING, result, batch, 0, messages);
        return result;
    }

    /** Rewrites a landed file with fewer records, keeping its name and index. */
    private void rewriteWithFewerRecords(Path file, int keep) throws Exception {
        List<org.apache.hadoop.io.LongWritable> keys = new ArrayList<>();
        List<org.apache.hadoop.io.Text> values = new ArrayList<>();
        try (SequenceFile.Reader reader =
                     new SequenceFile.Reader(conf, SequenceFile.Reader.file(file))) {
            org.apache.hadoop.io.LongWritable k = new org.apache.hadoop.io.LongWritable();
            org.apache.hadoop.io.Text v = new org.apache.hadoop.io.Text();
            while (reader.next(k, v) && keys.size() < keep) {
                keys.add(new org.apache.hadoop.io.LongWritable(k.get()));
                values.add(new org.apache.hadoop.io.Text(v.toString()));
            }
        }
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(org.apache.hadoop.io.LongWritable.class),
                SequenceFile.Writer.valueClass(org.apache.hadoop.io.Text.class))) {
            for (int i = 0; i < keys.size(); i++) {
                writer.append(keys.get(i), values.get(i));
            }
        }
    }

    private PartitionReconciliationService.ReconciliationReport reconcile() {
        return reconciler.reconcilePartition(
                BINDING, basePath, PARTITION_INSTANT,
                true,    // RMS has an approved identity
                false,   // never quarantine in a test
                new BindingMetrics(BINDING));
    }
}
