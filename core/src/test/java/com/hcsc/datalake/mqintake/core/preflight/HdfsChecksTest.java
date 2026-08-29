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
        // These probes run against a local filesystem on purpose; the service
        // itself demands the same explicit acknowledgement in production mode.
        properties.getHdfs().setAllowLocalFilesystem(true);
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
    void aFilesystemNamingADifferentClusterIsReportedAsAFailure() {
        // Every other check in this class passes against the wrong cluster —
        // it connects, the paths exist, the durability sequence works. This is
        // the only one that can tell the operator they are about to land data
        // somewhere nobody is looking for it.
        properties.getHdfs().setExpectedNameservice("target-ns");

        CheckOutcome outcome = outcomeOf(run(fileSystem), "filesystem.nameservice");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getDetail()).contains("does not name 'target-ns'");
        assertThat(outcome.getRemedy()).contains("DIFFERENT cluster");
    }

    @Test
    void anUncheckedNameserviceIsSkippedRatherThanAssumedCorrect() {
        CheckOutcome outcome = outcomeOf(run(fileSystem), "filesystem.nameservice");

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.SKIP);
        assertThat(outcome.getDetail()).contains("expected-nameservice not configured");
    }

    @Test
    void aClusterConfigDirectoryThatCannotBeReadIsReported() {
        properties.getHdfs().setConfigResources(
                List.of(tempDir.resolve("no-such-conf").toString()));

        CheckOutcome outcome = outcomeOf(run(fileSystem), "cluster-config.resources");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getDetail()).contains("cannot read");
        assertThat(outcome.getRemedy()).contains("HDFS_CONFIG_RESOURCES");
    }

    @Test
    void theLoadedClusterConfigurationIsNamedInTheReport() throws Exception {
        // "Which configuration did it actually read" is the first question on
        // a host that carries more than one Hadoop client's config.
        java.nio.file.Path conf = tempDir.resolve("target-conf");
        Files.createDirectories(conf);
        Files.writeString(conf.resolve("core-site.xml"),
                "<?xml version=\"1.0\"?><configuration/>");
        properties.getHdfs().setConfigResources(List.of(conf.toString()));

        CheckOutcome outcome = outcomeOf(run(fileSystem), "cluster-config.resources");

        assertThat(outcome.isFailure()).isFalse();
        assertThat(outcome.getDetail()).contains("core-site.xml");
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
    void aMissingAuditBindingDirectoryIsCreatedRatherThanFailed() throws Exception {
        // StartupValidator mkdirs this directory, so a fresh environment where
        // only the audit BASE exists is deployable. Preflight must predict the
        // service, not be stricter than it.
        fileSystem.delete(new Path(auditPath + "/rms"), true);

        CheckOutcome outcome = outcomeOf(run(fileSystem), "audit-path");

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
        assertThat(outcome.getDetail()).contains("created and writable");
        assertThat(fileSystem.exists(new Path(auditPath + "/rms"))).isTrue();
    }

    @Test
    void anUnacknowledgedLocalFilesystemIsAFailure() {
        // A fat jar with no cluster config resolves to file:/// and lands data
        // on the server's own disk, successfully and silently. A connectivity
        // check that called that a pass would certify the wrong destination.
        properties.getHdfs().setAllowLocalFilesystem(false);

        CheckOutcome outcome = outcomeOf(run(fileSystem), "filesystem.connect");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getDetail()).contains("LOCAL filesystem");
        assertThat(outcome.getRemedy()).contains("intake.hdfs.config-resources");
    }

    @Test
    void anUnconfiguredAuditPathIsSkippedNotFailed() {
        properties.getHdfs().setAuditBasePath("");

        assertThat(outcomeOf(run(fileSystem), "audit-path").getStatus())
                .isEqualTo(CheckOutcome.Status.SKIP);
    }
}
