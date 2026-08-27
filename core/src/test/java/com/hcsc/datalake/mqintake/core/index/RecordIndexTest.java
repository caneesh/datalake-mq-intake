package com.hcsc.datalake.mqintake.core.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sidecar index: writing it, reading it back, and refusing to trust it
 * when it is damaged.
 */
class RecordIndexTest {

    private FileSystem fileSystem;
    private String root;
    private String partition;
    private HdfsRecordIndexWriter writer;
    private RecordIndexReader reader;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);
        root = "/tmp/record-index-test-" + System.nanoTime();
        partition = root + "/year=2026/month=08/day=25/hour=10/quarter=2";
        fileSystem.mkdirs(new Path(partition));
        writer = new HdfsRecordIndexWriter(fileSystem, "test-instance");
        reader = new RecordIndexReader(fileSystem);
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.delete(new Path(root), true);
    }

    @Test
    void anIndexRoundTrips() throws Exception {
        writer.write(index(List.of(
                new RecordIndexEntry(129, "id-a"),
                new RecordIndexEntry(412, "id-b"),
                new RecordIndexEntry(701, "id-c"))));

        Optional<RecordIndex> read = reader.read(dataFile());

        assertThat(read).isPresent();
        assertThat(read.get().getBindingId()).isEqualTo("rms");
        assertThat(read.get().getFilename()).isEqualTo("rms_h1_169_1.seq");
        assertThat(read.get().getRecordCount()).isEqualTo(3);
        assertThat(read.get().getEntries()).containsExactly(
                new RecordIndexEntry(129, "id-a"),
                new RecordIndexEntry(412, "id-b"),
                new RecordIndexEntry(701, "id-c"));
        assertThat(read.get().isFullyIdentified()).isTrue();
    }

    @Test
    void identitiesCanBeReadAsASetForComparison() throws Exception {
        writer.write(index(List.of(
                new RecordIndexEntry(129, "id-a"),
                new RecordIndexEntry(412, "id-b"))));

        assertThat(reader.readIdentities(dataFile())).containsExactly("id-a", "id-b");
    }

    @Test
    void aSingleRecordBatchWorks() throws Exception {
        writer.write(index(List.of(new RecordIndexEntry(129, "only"))));

        assertThat(reader.read(dataFile()).orElseThrow().getEntries()).hasSize(1);
    }

    @Test
    void aFileWithNoIndexReadsAsEmptyRatherThanFailing() throws Exception {
        // The expected state for anything landed before indexing was enabled,
        // and after a crash between the data rename and the index rename.
        assertThat(reader.read(dataFile())).isEmpty();
        assertThat(reader.readIdentities(dataFile())).isEmpty();
        assertThat(reader.hasIndex(dataFile())).isFalse();
    }

    @Test
    void missingIdentitiesAreRecordedAndTheIndexIsMarkedNotFullyIdentified() throws Exception {
        // A binding whose payload has no identity: the index is honest about
        // it rather than pretending, so reconciliation can decline to use it.
        writer.write(index(List.of(
                new RecordIndexEntry(129, "id-a"),
                new RecordIndexEntry(412, null))));

        RecordIndex read = reader.read(dataFile()).orElseThrow();

        assertThat(read.getRecordCount()).isEqualTo(2);
        assertThat(read.isFullyIdentified()).isFalse();
        assertThat(reader.readIdentities(dataFile())).containsExactly("id-a");
    }

    @Test
    void aTruncatedIndexIsRejectedRatherThanReadAsShort() throws Exception {
        // The dangerous case: a half-written index that parses cleanly would
        // make reconciliation report the missing records as losses.
        writer.write(index(List.of(
                new RecordIndexEntry(129, "id-a"),
                new RecordIndexEntry(412, "id-b"),
                new RecordIndexEntry(701, "id-c"))));

        Path indexPath = RecordIndexReader.indexPathFor(dataFile());
        String content = new String(java.nio.file.Files.readAllBytes(
                java.nio.file.Paths.get(indexPath.toUri().getPath())),
                java.nio.charset.StandardCharsets.UTF_8);
        String truncated = content.substring(0, content.indexOf("id-c"));
        writeRaw(indexPath, truncated.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertThat(reader.read(dataFile()))
                .as("a truncated index must be ignored, not believed")
                .isEmpty();
    }

    @Test
    void aTruncatedHeaderIsRejectedNotReadAsAnEmptyIndex() throws Exception {
        // Without the header guard, a cut-off header still yielded schema=1,
        // declaredCount=-1 (check skipped), zero entries — a damaged index
        // reading as an authoritative claim of ZERO records for a full file.
        Path indexPath = RecordIndexReader.indexPathFor(dataFile());
        writeRaw(indexPath, "{\"schema\":1,\"binding\":\"rms\",\"file\":\"rms_h1"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertThat(reader.read(dataFile())).isEmpty();
    }

    @Test
    void aHeaderMissingTheRecordCountIsRejected() throws Exception {
        Path indexPath = RecordIndexReader.indexPathFor(dataFile());
        writeRaw(indexPath, "{\"schema\":1,\"binding\":\"rms\",\"file\":\"f.seq\"}\n"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertThat(reader.read(dataFile())).isEmpty();
    }

    @Test
    void controlCharacterIdentitiesSurviveTheRoundTrip() throws Exception {
        // The old unescape switch dropped the u-escape sequences the writer
        // emits for control characters, silently corrupting the identity.
        String identity = "ctl" + (char) 0x01 + "id";
        writer.write(index(List.of(new RecordIndexEntry(1, identity))));

        RecordIndex read = reader.read(dataFile()).orElseThrow();

        assertThat(read.getEntries().get(0).getIdentity()).isEqualTo(identity);
    }

    @Test
    void anUnknownSchemaVersionIsIgnored() throws Exception {
        Path indexPath = RecordIndexReader.indexPathFor(dataFile());
        writeRaw(indexPath, "{\"schema\":99,\"binding\":\"rms\",\"records\":0}\n"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertThat(reader.read(dataFile())).isEmpty();
    }

    @Test
    void anEmptyIndexFileIsIgnored() throws Exception {
        Path indexPath = RecordIndexReader.indexPathFor(dataFile());
        writeRaw(indexPath, new byte[0]);

        assertThat(reader.read(dataFile())).isEmpty();
    }

    @Test
    void identitiesContainingJsonMetacharactersSurviveTheRoundTrip() throws Exception {
        // The identity comes from a payload, so it is not guaranteed tame.
        writer.write(index(List.of(
                new RecordIndexEntry(1, "has \"quotes\" and \\backslash"),
                new RecordIndexEntry(2, "has\nnewline\ttab"))));

        RecordIndex read = reader.read(dataFile()).orElseThrow();

        assertThat(read.getEntries().get(0).getIdentity())
                .isEqualTo("has \"quotes\" and \\backslash");
        assertThat(read.getEntries().get(1).getIdentity()).isEqualTo("has\nnewline\ttab");
    }

    @Test
    void indexIsWrittenBesideItsDataFileWithAPredictableName() throws Exception {
        writer.write(index(List.of(new RecordIndexEntry(129, "id"))));

        Path expected = new Path(partition, "rms_h1_169_1.seq.index.jsonl");
        assertThat(fileSystem.exists(expected)).isTrue();
        assertThat(RecordIndexReader.indexPathFor(dataFile())).isEqualTo(expected);
    }

    @Test
    void noTempFileIsLeftBehindOnSuccess() throws Exception {
        writer.write(index(List.of(new RecordIndexEntry(129, "id"))));

        Path temp = new Path(root + "/_tmp/test-instance", "rms_h1_169_1.seq.index.jsonl");
        assertThat(fileSystem.exists(temp)).isFalse();
    }

    @Test
    void theDisabledWriterPersistsNothing() throws Exception {
        RecordIndexWriter disabled = RecordIndexWriter.disabled();

        assertThat(disabled.isEnabled()).isFalse();
        disabled.write(index(List.of(new RecordIndexEntry(1, "id"))));

        assertThat(reader.read(dataFile())).isEmpty();
    }

    @Test
    void anIndexRequiresItsIdentifyingFields() {
        assertThatThrownBy(() -> new RecordIndex(null, "f", "p", "i", List.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RecordIndex("b", null, "p", "i", List.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RecordIndex("b", "f", "p", "i", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void anEmptyIndexIsFullyIdentifiedVacuously() {
        assertThat(new RecordIndex("b", "f", "p", "i", List.of()).isFullyIdentified()).isTrue();
    }

    // --- helpers ---

    /**
     * Replaces a file's bytes. The local FileSystem keeps a .crc sidecar, so
     * the checksum must be dropped or the next read fails verification —
     * which would mask the condition under test.
     */
    private void writeRaw(Path path, byte[] bytes) throws IOException {
        java.nio.file.Path local = java.nio.file.Paths.get(path.toUri().getPath());
        java.nio.file.Files.write(local, bytes);
        java.nio.file.Path crc = local.getParent().resolve("." + local.getFileName() + ".crc");
        java.nio.file.Files.deleteIfExists(crc);
    }

    private RecordIndex index(List<RecordIndexEntry> entries) {
        return new RecordIndex("rms", "rms_h1_169_1.seq", partition, "test-instance", entries);
    }

    private Path dataFile() {
        return new Path(partition, "rms_h1_169_1.seq");
    }
}
