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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void recordCountIsReadFromTheFileEvenWhenAnIndexExists() throws Exception {
        // The index claims three records; the file reader says seven. The
        // count must come from the file.
        //
        // The sidecar and the audit record are both built from the writer's
        // single indexEntries list, so answering from the index made
        // reconciliation compare that list's size with itself — COUNT_MISMATCH
        // could not fire on any indexed file, and because nothing then opened
        // the file, a truncated or unreadable one reconciled clean.
        write(List.of(new RecordIndexEntry(1, "a"), new RecordIndexEntry(2, "b"),
                new RecordIndexEntry(3, "c")));

        assertThat(extractor.countRecords(dataFile()))
                .as("counted from the file, not from the index")
                .isEqualTo(7);
        assertThat(fallback.calls.get()).isEqualTo(1);
    }

    @Test
    void anUnreadableFileStillSurfacesEvenWithAnIndexPresent() throws Exception {
        // UNREADABLE_FILE is raised from this read alone. While the count came
        // from the index there was no read, so the condition was undetectable.
        write(List.of(new RecordIndexEntry(1, "a")));
        fallback.failWith(new IOException("block missing"));

        assertThatThrownBy(() -> extractor.countRecords(dataFile()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("block missing");
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

        private volatile IOException failure;

        void failWith(IOException e) {
            this.failure = e;
        }

        @Override
        public int countRecords(String filePath) throws IOException {
            calls.incrementAndGet();
            if (failure != null) {
                throw failure;
            }
            return 7;
        }
    }
}
