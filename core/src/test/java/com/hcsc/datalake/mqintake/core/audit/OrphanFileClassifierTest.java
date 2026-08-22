package com.hcsc.datalake.mqintake.core.audit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for OrphanFileClassifier.
 *
 * <p>From DESIGN.md §10: The classifier MUST NEVER delete a file.
 * It returns a classification only. Quarantine is a move performed
 * by a separate aged step.
 *
 * <p>A file without an audit record may be the only copy of committed
 * payment data — §12.1 states that states 3 (LANDED) and 4 (MQ_COMMITTED)
 * are externally indistinguishable.
 */
class OrphanFileClassifierTest {

    private FileSystem fileSystem;
    private TestIdentityExtractor identityExtractor;
    private OrphanFileClassifier classifier;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);

        identityExtractor = new TestIdentityExtractor();
        classifier = new OrphanFileClassifier(fileSystem, identityExtractor);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clean up any test files
        identityExtractor.clear();
    }

    // --- DUPLICATE classification tests ---

    @Test
    void classifiesAsDuplicateWhenAllIdentitiesPresentElsewhere() throws Exception {
        String partition = "/tmp/test-partition-dup";
        String suspectFile = partition + "/suspect.seq";
        String otherFile = partition + "/other.seq";

        // Create test files
        createTestDirectory(partition);
        createTestFile(suspectFile);
        createTestFile(otherFile);

        // Suspect file has identities that are all present in other file
        identityExtractor.setIdentities(suspectFile, Set.of("guid-001", "guid-002", "guid-003"));
        identityExtractor.setIdentities(otherFile, Set.of("guid-001", "guid-002", "guid-003", "guid-004"));

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getClassification()).isEqualTo(FileClassification.DUPLICATE);
        assertThat(result.isSafeToQuarantine()).isTrue();
        assertThat(result.mustKeep()).isFalse();
        assertThat(result.getRelevantIdentities()).containsExactlyInAnyOrder(
                "guid-001", "guid-002", "guid-003");

        cleanup(partition);
    }

    @Test
    void duplicateReasonIndicatesAllIdentitiesFoundElsewhere() throws Exception {
        String partition = "/tmp/test-partition-dup-reason";
        String suspectFile = partition + "/suspect.seq";
        String otherFile = partition + "/other.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);
        createTestFile(otherFile);

        identityExtractor.setIdentities(suspectFile, Set.of("a", "b"));
        identityExtractor.setIdentities(otherFile, Set.of("a", "b", "c"));

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getReason()).contains("2").contains("found in other files");

        cleanup(partition);
    }

    // --- SOLE_COPY classification tests ---

    @Test
    void classifiesAsSoleCopyWhenAnyIdentityPresentNowhereElse() throws Exception {
        String partition = "/tmp/test-partition-sole";
        String suspectFile = partition + "/suspect.seq";
        String otherFile = partition + "/other.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);
        createTestFile(otherFile);

        // Suspect file has guid-003 which is NOT in other files
        identityExtractor.setIdentities(suspectFile, Set.of("guid-001", "guid-002", "guid-003"));
        identityExtractor.setIdentities(otherFile, Set.of("guid-001", "guid-002"));

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getClassification()).isEqualTo(FileClassification.SOLE_COPY);
        assertThat(result.isSafeToQuarantine()).isFalse();
        assertThat(result.mustKeep()).isTrue();
        assertThat(result.getRelevantIdentities()).containsExactly("guid-003");

        cleanup(partition);
    }

    @Test
    void soleCopyWhenNoOtherFilesInPartition() throws Exception {
        String partition = "/tmp/test-partition-alone";
        String suspectFile = partition + "/suspect.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);

        // Only file in partition - all its identities are unique
        identityExtractor.setIdentities(suspectFile, Set.of("guid-001", "guid-002"));

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getClassification()).isEqualTo(FileClassification.SOLE_COPY);
        assertThat(result.mustKeep()).isTrue();
        assertThat(result.getRelevantIdentities()).containsExactlyInAnyOrder("guid-001", "guid-002");

        cleanup(partition);
    }

    @Test
    void soleCopyReasonIndicatesUniqueIdentities() throws Exception {
        String partition = "/tmp/test-partition-sole-reason";
        String suspectFile = partition + "/suspect.seq";
        String otherFile = partition + "/other.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);
        createTestFile(otherFile);

        identityExtractor.setIdentities(suspectFile, Set.of("a", "b", "unique"));
        identityExtractor.setIdentities(otherFile, Set.of("a", "b"));

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getReason()).contains("1").contains("nowhere else").contains("MUST keep");

        cleanup(partition);
    }

    // --- INCONCLUSIVE classification tests ---

    @Test
    void classifiesAsInconclusiveWhenSuspectFileNotFound() throws Exception {
        String partition = "/tmp/test-partition-missing";
        String suspectFile = partition + "/nonexistent.seq";

        createTestDirectory(partition);

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getClassification()).isEqualTo(FileClassification.INCONCLUSIVE);
        assertThat(result.mustKeep()).isTrue();
        assertThat(result.getReason()).contains("does not exist");

        cleanup(partition);
    }

    @Test
    void classifiesAsInconclusiveWhenNoIdentitiesExtracted() throws Exception {
        String partition = "/tmp/test-partition-empty";
        String suspectFile = partition + "/suspect.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);

        // Empty identities
        identityExtractor.setIdentities(suspectFile, Set.of());

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getClassification()).isEqualTo(FileClassification.INCONCLUSIVE);
        assertThat(result.mustKeep()).isTrue();
        assertThat(result.getReason()).contains("No identities");

        cleanup(partition);
    }

    @Test
    void classifiesAsInconclusiveWhenCannotReadSuspectFile() throws Exception {
        String partition = "/tmp/test-partition-unreadable";
        String suspectFile = partition + "/suspect.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);

        // Make identity extractor throw for this file
        identityExtractor.setThrowOnExtract(suspectFile, true);

        OrphanFileClassifier.ClassificationResult result =
                classifier.classify(suspectFile, partition);

        assertThat(result.getClassification()).isEqualTo(FileClassification.INCONCLUSIVE);
        assertThat(result.mustKeep()).isTrue();
        assertThat(result.getReason()).contains("Cannot read");

        cleanup(partition);
    }

    // --- CRITICAL: No delete path exists ---

    @Test
    void classifierHasNoDeleteMethod() {
        // Verify the OrphanFileClassifier class has NO methods that could delete files
        Method[] methods = OrphanFileClassifier.class.getDeclaredMethods();

        for (Method method : methods) {
            String name = method.getName().toLowerCase();
            assertThat(name)
                    .as("Method %s should not contain 'delete'", method.getName())
                    .doesNotContain("delete");
            assertThat(name)
                    .as("Method %s should not contain 'remove'", method.getName())
                    .doesNotContain("remove");
        }
    }

    @Test
    void classifierNeverCallsFileSystemDelete() throws Exception {
        // Use a tracking FileSystem wrapper to verify no deletes are called
        TrackingFileSystem trackingFs = new TrackingFileSystem(fileSystem);
        OrphanFileClassifier trackingClassifier =
                new OrphanFileClassifier(trackingFs, identityExtractor);

        String partition = "/tmp/test-partition-nodelete";
        String suspectFile = partition + "/suspect.seq";
        String otherFile = partition + "/other.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);
        createTestFile(otherFile);

        // Set up for DUPLICATE classification
        identityExtractor.setIdentities(suspectFile, Set.of("a", "b"));
        identityExtractor.setIdentities(otherFile, Set.of("a", "b", "c"));

        // Classify - even as DUPLICATE, should NOT delete
        OrphanFileClassifier.ClassificationResult result =
                trackingClassifier.classify(suspectFile, partition);

        assertThat(result.getClassification()).isEqualTo(FileClassification.DUPLICATE);
        assertThat(trackingFs.getDeleteCallCount())
                .as("Classifier should NEVER call delete")
                .isEqualTo(0);

        // File should still exist
        assertThat(fileSystem.exists(new Path(suspectFile))).isTrue();

        cleanup(partition);
    }

    @Test
    void classificationResultIndicatesSafeActionsOnly() {
        // Verify that ClassificationResult methods only indicate quarantine (move), never delete
        OrphanFileClassifier.ClassificationResult duplicateResult =
                new OrphanFileClassifier.ClassificationResult(
                        FileClassification.DUPLICATE, "test", Set.of("a"), 1);

        // "isSafeToQuarantine" is correct - quarantine is a MOVE, not delete
        assertThat(duplicateResult.isSafeToQuarantine()).isTrue();

        // Verify via reflection that no "delete" method exists
        Method[] methods = OrphanFileClassifier.ClassificationResult.class.getDeclaredMethods();
        for (Method method : methods) {
            String name = method.getName().toLowerCase();
            assertThat(name)
                    .as("ClassificationResult method %s should not suggest delete", method.getName())
                    .doesNotContain("delete");
        }
    }

    @Test
    void allClassificationPathsPreserveFile() throws Exception {
        String partition = "/tmp/test-partition-preserve";
        String suspectFile = partition + "/suspect.seq";
        String otherFile = partition + "/other.seq";

        createTestDirectory(partition);
        createTestFile(suspectFile);
        createTestFile(otherFile);

        // Test DUPLICATE path
        identityExtractor.setIdentities(suspectFile, Set.of("a"));
        identityExtractor.setIdentities(otherFile, Set.of("a", "b"));
        classifier.classify(suspectFile, partition);
        assertThat(fileSystem.exists(new Path(suspectFile)))
                .as("DUPLICATE classification should NOT delete file").isTrue();

        // Test SOLE_COPY path
        identityExtractor.setIdentities(suspectFile, Set.of("unique"));
        identityExtractor.setIdentities(otherFile, Set.of("a", "b"));
        classifier.classify(suspectFile, partition);
        assertThat(fileSystem.exists(new Path(suspectFile)))
                .as("SOLE_COPY classification should NOT delete file").isTrue();

        // Test INCONCLUSIVE path
        identityExtractor.setIdentities(suspectFile, Set.of());
        classifier.classify(suspectFile, partition);
        assertThat(fileSystem.exists(new Path(suspectFile)))
                .as("INCONCLUSIVE classification should NOT delete file").isTrue();

        cleanup(partition);
    }

    // --- Helper methods ---

    private void createTestDirectory(String path) throws IOException {
        fileSystem.mkdirs(new Path(path));
    }

    private void createTestFile(String path) throws IOException {
        fileSystem.create(new Path(path)).close();
    }

    private void cleanup(String path) throws IOException {
        fileSystem.delete(new Path(path), true);
    }

    /**
     * Test identity extractor with configurable responses.
     * Normalizes paths to handle file:/ prefix differences.
     */
    private static class TestIdentityExtractor implements IdentityExtractor {
        private final Map<String, Set<String>> identities = new HashMap<>();
        private final Set<String> throwOnExtract = new HashSet<>();

        void setIdentities(String filePath, Set<String> ids) {
            identities.put(normalizePath(filePath), ids);
        }

        void setThrowOnExtract(String filePath, boolean shouldThrow) {
            if (shouldThrow) {
                throwOnExtract.add(normalizePath(filePath));
            } else {
                throwOnExtract.remove(normalizePath(filePath));
            }
        }

        void clear() {
            identities.clear();
            throwOnExtract.clear();
        }

        @Override
        public Set<String> extractIdentities(String filePath) throws IOException {
            String normalized = normalizePath(filePath);
            if (throwOnExtract.contains(normalized)) {
                throw new IOException("Simulated read error");
            }
            return identities.getOrDefault(normalized, Set.of());
        }

        @Override
        public int countRecords(String filePath) throws IOException {
            return extractIdentities(filePath).size();
        }

        private String normalizePath(String path) {
            // Remove file: prefix if present
            if (path.startsWith("file:")) {
                path = path.substring(5);
            }
            // Remove leading slashes after file: (e.g., file:///tmp -> /tmp)
            while (path.startsWith("//")) {
                path = path.substring(1);
            }
            return path;
        }
    }

    /**
     * FileSystem wrapper that tracks delete calls.
     */
    private static class TrackingFileSystem extends FileSystem {
        private final FileSystem delegate;
        private int deleteCallCount = 0;

        TrackingFileSystem(FileSystem delegate) {
            this.delegate = delegate;
        }

        int getDeleteCallCount() {
            return deleteCallCount;
        }

        @Override
        public boolean delete(Path f, boolean recursive) throws IOException {
            deleteCallCount++;
            return delegate.delete(f, recursive);
        }

        // Delegate all other methods
        @Override
        public java.net.URI getUri() { return delegate.getUri(); }

        @Override
        public org.apache.hadoop.fs.FSDataInputStream open(Path f, int bufferSize) throws IOException {
            return delegate.open(f, bufferSize);
        }

        @Override
        public org.apache.hadoop.fs.FSDataOutputStream create(Path f, org.apache.hadoop.fs.permission.FsPermission permission,
                                                                boolean overwrite, int bufferSize, short replication,
                                                                long blockSize, org.apache.hadoop.util.Progressable progress) throws IOException {
            return delegate.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
        }

        @Override
        public org.apache.hadoop.fs.FSDataOutputStream append(Path f, int bufferSize,
                                                               org.apache.hadoop.util.Progressable progress) throws IOException {
            return delegate.append(f, bufferSize, progress);
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            return delegate.rename(src, dst);
        }

        @Override
        public boolean exists(Path f) throws IOException {
            return delegate.exists(f);
        }

        @Override
        public org.apache.hadoop.fs.FileStatus[] listStatus(Path f) throws IOException {
            return delegate.listStatus(f);
        }

        @Override
        public org.apache.hadoop.fs.FileStatus[] listStatus(Path f, org.apache.hadoop.fs.PathFilter filter) throws IOException {
            return delegate.listStatus(f, filter);
        }

        @Override
        public void setWorkingDirectory(Path new_dir) {
            delegate.setWorkingDirectory(new_dir);
        }

        @Override
        public Path getWorkingDirectory() {
            return delegate.getWorkingDirectory();
        }

        @Override
        public boolean mkdirs(Path f, org.apache.hadoop.fs.permission.FsPermission permission) throws IOException {
            return delegate.mkdirs(f, permission);
        }

        @Override
        public org.apache.hadoop.fs.FileStatus getFileStatus(Path f) throws IOException {
            return delegate.getFileStatus(f);
        }
    }
}
