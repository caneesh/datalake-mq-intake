package com.hcsc.datalake.mqintake.core.index;

import com.hcsc.datalake.mqintake.core.audit.IdentityExtractor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reconciliation reading identities from the sidecar rather than from the file.
 */
class RecordIndexIdentityExtractorTest {

    private FileSystem fileSystem;
    private String partition;
    private String root;
    private HdfsRecordIndexWriter writer;
    private RecordingFallback fallback;
    private RecordIndexIdentityExtractor extractor;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);
        root = "/tmp/index-extractor-" + System.nanoTime();
        partition = root + "/year=2026/month=08/day=25/hour=10/quarter=2";
        fileSystem.mkdirs(new Path(partition));
        writer = new HdfsRecordIndexWriter(fileSystem, "test-instance");
        fallback = new RecordingFallback();
        extractor = new RecordIndexIdentityExtractor(new RecordIndexReader(fileSystem), fallback);
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.delete(new Path(root), true);
    }

    @Test
    void identitiesComeFromTheIndex() throws Exception {
        write(List.of(new RecordIndexEntry(129, "id-a"), new RecordIndexEntry(412, "id-b")));

        assertThat(extractor.extractIdentities(dataFile())).containsExactly("id-a", "id-b");
        assertThat(fallback.calls.get()).isZero();
    }

    @Test
    void recordCountComesFromTheIndexWithoutReadingTheFile() throws Exception {
        write(List.of(new RecordIndexEntry(1, "a"), new RecordIndexEntry(2, "b"),
                new RecordIndexEntry(3, "c")));

        assertThat(extractor.countRecords(dataFile())).isEqualTo(3);
        assertThat(fallback.calls.get()).isZero();
    }

    @Test
    void filesWithoutAnIndexFallBackToTheFileReader() throws Exception {
        // Everything landed before indexing was enabled, and every binding
        // that has it off.
        assertThat(extractor.extractIdentities(dataFile())).isEmpty();
        assertThat(fallback.calls.get()).isEqualTo(1);

        assertThat(extractor.countRecords(dataFile())).isEqualTo(7);
        assertThat(fallback.calls.get()).isEqualTo(2);
    }

    @Test
    void aPartiallyIdentifiedIndexYieldsNoIdentitiesRatherThanSome() throws Exception {
        // Returning the identified subset would make the rest look like losses.
        write(List.of(new RecordIndexEntry(129, "id-a"), new RecordIndexEntry(412, null)));

        assertThat(extractor.extractIdentities(dataFile())).isEmpty();
        assertThat(extractor.hasUsableIndex(dataFile())).isFalse();
    }

    @Test
    void aFullyIdentifiedIndexIsReportedAsUsable() throws Exception {
        write(List.of(new RecordIndexEntry(129, "id-a")));

        assertThat(extractor.hasUsableIndex(dataFile())).isTrue();
    }

    private void write(List<RecordIndexEntry> entries) throws IOException {
        writer.write(new RecordIndex("rms", "rms_1.seq", partition, "test-instance", entries));
    }

    private String dataFile() {
        return partition + "/rms_1.seq";
    }

    /** Stands in for the SequenceFile-based reader. */
    private static class RecordingFallback implements IdentityExtractor {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public Set<String> extractIdentities(String filePath) {
            calls.incrementAndGet();
            return Set.of();   // what the production key actually yields
        }

        @Override
        public int countRecords(String filePath) {
            calls.incrementAndGet();
            return 7;
        }
    }
}
