package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.index.HdfsRecordIndexWriter;
import com.hcsc.datalake.mqintake.core.index.RecordIndex;
import com.hcsc.datalake.mqintake.core.index.RecordIndexEntry;
import com.hcsc.datalake.mqintake.core.index.RecordIndexIdentityExtractor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the identity chain that PRODUCTION reconciliation runs with.
 *
 * <p>This class exists because its absence was the mechanism behind a real
 * defect. {@code PartitionReconciliationServiceTest} builds its own chain by
 * hand — and builds its fixtures with {@code Text} metadata keys, the
 * abandoned Option A layout that {@code ProductionLayoutFingerprintTest}
 * exists to forbid. Production wires a different chain through
 * {@link ReconciliationFactory} over files with a {@code LongWritable} byte
 * offset and a {@code Text} payload. The two drifted, and the drift was
 * invisible: the service suite proved a count check that production could not
 * perform.
 *
 * <p>Everything here therefore uses the production file layout and the
 * production wiring. Identity assertions stay out of it deliberately: under
 * that layout the file reader finds none, which is the open half of this work
 * (a binding-aware extractor that reads the payload rather than the key).
 */
class ReconciliationFactoryTest {

    @TempDir
    java.nio.file.Path tempDir;

    private Configuration conf;
    private FileSystem fileSystem;
    private RecordIndexIdentityExtractor identityReader;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(conf);
        identityReader = ReconciliationFactory.createIdentityReader(fileSystem, conf);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    @Test
    void theCountProductionUsesIsReadFromTheFileNotTheSidecar() throws Exception {
        // A file holding four records, beside an index claiming nine.
        //
        // Nine is not an arbitrary wrong number: the sidecar and the audit
        // record are both built from the writer's single indexEntries list, so
        // whatever the sidecar says, the audit says too. Counting from the
        // sidecar made reconciliation compare that list's size with itself,
        // and COUNT_MISMATCH could not fire on any indexed file.
        Path dataFile = writeDataFile("rms_1.seq", 4);
        writeIndexClaiming(dataFile, 9);

        assertThat(identityReader.countRecords(dataFile.toString()))
                .as("production counts what is in the file, whatever the sidecar claims")
                .isEqualTo(4);
    }

    @Test
    void aTruncatedFileIsVisibleToProductionEvenWithAnIntactSidecar() throws Exception {
        // The failure the count check exists for: the file loses records after
        // landing while its metadata still describes what was written.
        Path dataFile = writeDataFile("rms_2.seq", 6);
        writeIndexClaiming(dataFile, 6);
        assertThat(identityReader.countRecords(dataFile.toString())).isEqualTo(6);

        Path truncated = writeDataFile("rms_2.seq", 2);   // same name, fewer records
        writeIndexClaiming(truncated, 6);                 // sidecar still says six

        assertThat(identityReader.countRecords(truncated.toString()))
                .as("the shortfall is what reconciliation reports as COUNT_MISMATCH")
                .isEqualTo(2);
    }

    @Test
    void identityStillPrefersTheSidecar() throws Exception {
        // Unchanged by this work, and stated so the split is explicit: only
        // the COUNT moved to the file. Identity cannot come from the file
        // until something reads the payload instead of the byte-offset key.
        Path dataFile = writeDataFile("rms_3.seq", 2);
        writeIndex(dataFile, List.of(
                new RecordIndexEntry(129, "guid-a"),
                new RecordIndexEntry(240, "guid-b")));

        assertThat(identityReader.extractIdentities(dataFile.toString()))
                .containsExactlyInAnyOrder("guid-a", "guid-b");
    }

    @Test
    void aFileWithNoSidecarIsStillCountedFromTheFile() throws Exception {
        // Bindings with indexing off, and files that landed before it existed.
        Path dataFile = writeDataFile("rms_4.seq", 3);

        assertThat(identityReader.countRecords(dataFile.toString())).isEqualTo(3);
    }

    /** A SequenceFile in the production layout: LongWritable offset, Text payload. */
    private Path writeDataFile(String filename, int records) throws Exception {
        Path file = new Path(tempDir.resolve(filename).toString());
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(Text.class))) {
            for (int i = 0; i < records; i++) {
                writer.append(new LongWritable(writer.getLength()),
                        new Text("<Msg><MessageID>guid-" + i + "</MessageID></Msg>"));
            }
        }
        return file;
    }

    private void writeIndexClaiming(Path dataFile, int records) throws Exception {
        List<RecordIndexEntry> entries = new ArrayList<>();
        for (int i = 0; i < records; i++) {
            entries.add(new RecordIndexEntry(i * 100L, "guid-" + i));
        }
        writeIndex(dataFile, entries);
    }

    private void writeIndex(Path dataFile, List<RecordIndexEntry> entries) throws Exception {
        new HdfsRecordIndexWriter(fileSystem, "test-instance").write(new RecordIndex(
                "rms", dataFile.getName(), dataFile.getParent().toString(),
                "test-instance", entries));
    }
}
