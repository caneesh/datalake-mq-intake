package com.hcsc.datalake.mqintake.core.lifecycle;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Startup validation of the audit destination.
 *
 * <p>Audit records are written <em>after</em> the MQ commit, so an unusable
 * audit path does not stop the service starting, landing data and
 * acknowledging messages — it surfaces only when the first batch tries to
 * record what it has already done. By then the data is committed and the
 * audit trail for it is permanently missing.
 */
class AuditPathStartupValidationTest {

    private FileSystem fileSystem;
    private String root;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);
        root = "/tmp/audit-validation-test-" + System.nanoTime();
        fileSystem.mkdirs(new Path(root + "/data"));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileSystem != null && root != null) {
            // Restore permissions first or the delete itself fails
            new File(root + "/audit").setWritable(true, false);
            fileSystem.delete(new Path(root), true);
        }
    }

    @Test
    void validAuditLocationPasses() throws Exception {
        fileSystem.mkdirs(new Path(root + "/audit"));
        StartupValidator validator =
                new StartupValidator(fileSystem, "test-instance", root + "/audit");

        assertThat(validator.validateBindings(List.of(binding("rms")))).isEmpty();
        assertThatCode(() -> validator.validateOrFail(List.of(binding("rms"))))
                .doesNotThrowAnyException();
    }

    @Test
    void missingButCreatableAuditLocationIsCreated() throws Exception {
        // Mirrors HdfsAuditRecordEmitter, which mkdirs the parent before
        // writing — a missing directory is not an error, an uncreatable one is.
        String auditBase = root + "/audit";
        assertThat(fileSystem.exists(new Path(auditBase + "/rms"))).isFalse();

        StartupValidator validator =
                new StartupValidator(fileSystem, "test-instance", auditBase);

        assertThat(validator.validateBindings(List.of(binding("rms")))).isEmpty();
        assertThat(fileSystem.exists(new Path(auditBase + "/rms"))).isTrue();
    }

    @Test
    void inaccessibleAuditLocationFailsStartup() throws Exception {
        assumeFalse(isRunningAsRoot(), "root bypasses directory permissions");

        File auditBase = new File(root + "/audit");
        assertThat(auditBase.mkdirs()).isTrue();
        assertThat(auditBase.setWritable(false, false)).isTrue();

        StartupValidator validator =
                new StartupValidator(fileSystem, "test-instance", auditBase.getPath());

        assertThatThrownBy(() -> validator.validateOrFail(List.of(binding("rms"))))
                .isInstanceOf(StartupValidator.StartupValidationException.class)
                .hasMessageContaining("rms")
                .hasMessageContaining("audit");
    }

    @Test
    void errorNamesTheBindingThePathAndTheReason() throws Exception {
        assumeFalse(isRunningAsRoot(), "root bypasses directory permissions");

        File auditBase = new File(root + "/audit");
        assertThat(auditBase.mkdirs()).isTrue();
        assertThat(auditBase.setWritable(false, false)).isTrue();

        StartupValidator validator =
                new StartupValidator(fileSystem, "test-instance", auditBase.getPath());

        List<String> errors = validator.validateBindings(List.of(binding("claims")));

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0))
                .contains("claims")                       // which binding
                .contains(auditBase.getPath())            // which path
                .containsPattern("(?i)not writable|could not be created|failed");  // why
    }

    @Test
    void oneFailingAuditPathAmongSeveralBindingsIsReported() throws Exception {
        assumeFalse(isRunningAsRoot(), "root bypasses directory permissions");

        // Shared audit base, with only one binding's subdirectory unusable
        File auditBase = new File(root + "/audit");
        assertThat(auditBase.mkdirs()).isTrue();
        File healthy = new File(auditBase, "rms");
        assertThat(healthy.mkdirs()).isTrue();
        File broken = new File(auditBase, "claims");
        assertThat(broken.mkdirs()).isTrue();
        assertThat(broken.setWritable(false, false)).isTrue();

        StartupValidator validator =
                new StartupValidator(fileSystem, "test-instance", auditBase.getPath());

        List<String> errors =
                validator.validateBindings(List.of(binding("rms"), binding("claims")));

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("claims");
        assertThat(errors.get(0)).doesNotContain("'rms'");

        broken.setWritable(true, false);
    }

    @Test
    void unconfiguredAuditBasePathIsAnError() {
        // Every committed batch writes an audit record, so a blank audit base
        // path is a misconfiguration rather than a way to opt out.
        StartupValidator blank = new StartupValidator(fileSystem, "test-instance", "  ");
        assertThat(blank.validateBindings(List.of(binding("rms"))))
                .singleElement().asString()
                .contains("audit-base-path");

        StartupValidator missing = new StartupValidator(fileSystem, "test-instance", null);
        assertThat(missing.validateBindings(List.of(binding("rms"))))
                .singleElement().asString()
                .contains("audit-base-path");
    }

    @Test
    void validatorWithoutAuditConfiguredStillChecksLandingPaths() {
        // The two-arg constructor is used where there is no audit destination
        // to check; it must not start reporting audit errors.
        StartupValidator validator = new StartupValidator(fileSystem, "test-instance");

        assertThat(validator.validateBindings(List.of(binding("rms")))).isEmpty();
    }

    private BindingConfig binding(String id) {
        BindingConfig config = new BindingConfig();
        config.setId(id);
        config.setSourceQueue("TEST.QUEUE");
        config.getHdfs().setBasePath(root + "/data");
        config.setMode(BindingMode.LAND_ONLY);
        config.getBatch().setSize(100);
        config.getBatch().setBytes(1024 * 1024);
        config.getBatch().setIntervalMs(30000);
        return config;
    }

    private boolean isRunningAsRoot() {
        return "root".equals(System.getProperty("user.name"));
    }
}
