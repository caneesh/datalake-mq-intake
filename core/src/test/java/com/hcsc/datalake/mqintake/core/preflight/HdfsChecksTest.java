package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The storage probes, against a real (local) filesystem.
 *
 * <p>The durability probe is the one that matters: it performs the exact
 * write → hsync → close → rename → read-back sequence a batch performs, so a
 * cluster that cannot do it is caught before any message is consumed. Equally
 * important is what it must NOT do — leave anything behind, or write into a
 * data partition where reconciliation would later find it and call it an
 * orphan.
 */
class HdfsChecksTest {

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem fileSystem;
    private String basePath;
    private String auditPath;
    private IntakeProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);

        basePath = tempDir.resolve("data").toString();
        auditPath = tempDir.resolve("audit").toString();
        Files.createDirectories(tempDir.resolve("data"));
        Files.createDirectories(tempDir.resolve("audit/rms"));

        BindingConfig binding = new BindingConfig();
        binding.setId("rms");
        binding.setMode(BindingMode.LAND_ONLY);
        binding.setSourceQueue("Q.IN");
        binding.getHdfs().setBasePath(basePath);

        properties = new IntakeProperties();
        properties.setBindings(List.of(binding));
        properties.getHdfs().setAuditBasePath(auditPath);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    private PreflightReport run(FileSystem fs) {
        return new PreflightRunner(HdfsChecks.forAllBindings(properties, fs, "inst-1"))
                .run(Set.of());
    }

    private CheckOutcome outcomeOf(PreflightReport report, String nameSuffix) {
        return report.getEntries().stream()
                .filter(e -> e.getCheck().name().endsWith(nameSuffix))
                .findFirst().orElseThrow()
                .getOutcome();
    }

    @Test
    void aHealthyFilesystemPassesEveryCheck() {
        PreflightReport report = run(fileSystem);

        assertThat(report.hasFailures())
                .as(report.render())
                .isFalse();
        assertThat(outcomeOf(report, "durability-roundtrip").getDetail())
                .contains("read-back verified");
    }

    @Test
    void theDurabilityProbeLeavesNothingBehind() throws Exception {
        run(fileSystem);

        Path tempTree = new Path(PartitionPath.tempDir(basePath, "inst-1"));
        FileStatus[] leftovers = fileSystem.exists(tempTree)
                ? fileSystem.listStatus(tempTree) : new FileStatus[0];

        assertThat(leftovers)
                .as("a probe file left in place would be swept eventually, but preflight "
                        + "should not need the sweep")
                .isEmpty();
    }

    @Test
    void theDurabilityProbeNeverWritesIntoADataPartition() throws Exception {
        run(fileSystem);

        // Anything outside _tmp would be read by reconciliation on its next
        // pass and classified as an orphan — preflight would manufacture the
        // incident it exists to prevent.
        for (FileStatus entry : fileSystem.listStatus(new Path(basePath))) {
            assertThat(entry.getPath().getName())
                    .as("only the temp tree may appear under the landing path")
                    .isEqualTo("_tmp");
        }
    }

    @Test
    void aMissingLandingPathFailsWithAnActionableRemedy() throws Exception {
        properties.getBindings().get(0).getHdfs()
                .setBasePath(tempDir.resolve("nonexistent").toString());

        CheckOutcome outcome = outcomeOf(run(fileSystem), "landing-path");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getDetail()).contains("does not exist");
        assertThat(outcome.getRemedy()).contains("write to the service principal");
    }

    @Test
    void aFailedRenameIsReportedAsTheDurabilityFailureItIs() {
        FileSystem renameRefuses = new FilterFileSystem(fileSystem) {
            @Override
            public boolean rename(Path src, Path dst) {
                return false;
            }
        };

        CheckOutcome outcome = outcomeOf(run(renameRefuses), "durability-roundtrip");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getDetail()).contains("rename returned false");
        assertThat(outcome.getRemedy()).contains("Rename is how a batch becomes visible");
    }

    @Test
    void aClusterThatDoesNotStoreWhatItAcknowledgedIsCaught() {
        // Substitutes different content after the rename succeeds, standing in
        // for a filesystem that acknowledges a write it did not store
        // faithfully. The probe must compare the bytes, not merely read them.
        FileSystem corruptsAfterRename = new FilterFileSystem(fileSystem) {
            @Override
            public boolean rename(Path src, Path dst) throws IOException {
                boolean renamed = super.rename(src, dst);
                if (renamed) {
                    byte[] wrong = new byte[4096];
                    java.util.Arrays.fill(wrong, (byte) 'x');
                    try (org.apache.hadoop.fs.FSDataOutputStream out = super.create(dst, true)) {
                        out.write(wrong);
                    }
                }
                return renamed;
            }
        };

        CheckOutcome outcome = outcomeOf(run(corruptsAfterRename), "durability-roundtrip");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getDetail()).contains("read-back did not match");
        assertThat(outcome.getRemedy()).contains("Escalate to the platform team");
    }

    @Test
    void anUnconfiguredAuditPathIsSkippedNotFailed() {
        properties.getHdfs().setAuditBasePath("");

        assertThat(outcomeOf(run(fileSystem), "audit-path").getStatus())
                .isEqualTo(CheckOutcome.Status.SKIP);
    }
}
