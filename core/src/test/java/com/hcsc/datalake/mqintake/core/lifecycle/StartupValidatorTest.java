package com.hcsc.datalake.mqintake.core.lifecycle;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for StartupValidator.
 *
 * <p>From DESIGN.md §13 and §14: Verify write access to every configured
 * binding base path at startup and fail fast with a clear error.
 */
class StartupValidatorTest {

    private FileSystem fileSystem;
    private StartupValidator validator;
    private String testBasePath;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.get(conf);

        testBasePath = "/tmp/startup-validator-test-" + System.currentTimeMillis();
        validator = new StartupValidator(fileSystem, "test-instance");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileSystem != null && testBasePath != null) {
            fileSystem.delete(new Path(testBasePath), true);
        }
    }

    private BindingConfig createBinding(String id, String hdfsBasePath) {
        BindingConfig config = new BindingConfig();
        config.setId(id);
        config.setSourceQueue("TEST.QUEUE");
        config.getHdfs().setBasePath(hdfsBasePath);
        config.setMode(BindingMode.LAND_ONLY);
        config.getBatch().setSize(100);
        config.getBatch().setBytes(1024 * 1024);
        config.getBatch().setIntervalMs(30000);
        return config;
    }

    @Test
    void validatesExistingWritablePath() throws Exception {
        fileSystem.mkdirs(new Path(testBasePath));

        BindingConfig binding = createBinding("test-binding", testBasePath);

        List<String> errors = validator.validateBindings(List.of(binding));

        assertThat(errors).isEmpty();
    }

    @Test
    void failsIfBasePathDoesNotExist() {
        String nonExistentPath = "/tmp/nonexistent-" + System.currentTimeMillis();

        BindingConfig binding = createBinding("test-binding", nonExistentPath);

        List<String> errors = validator.validateBindings(List.of(binding));

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("does not exist");
        assertThat(errors.get(0)).contains("test-binding");
    }

    @Test
    void validateOrFailThrowsWithClearMessage() {
        String nonExistentPath = "/tmp/nonexistent-" + System.currentTimeMillis();

        BindingConfig binding = createBinding("test-binding", nonExistentPath);

        assertThatThrownBy(() -> validator.validateOrFail(List.of(binding)))
                .isInstanceOf(StartupValidator.StartupValidationException.class)
                .hasMessageContaining("Startup validation failed")
                .hasMessageContaining("test-binding");
    }

    @Test
    void validatesMultipleBindings() throws Exception {
        String validPath = testBasePath + "/rms";
        fileSystem.mkdirs(new Path(validPath));

        // Use a path guaranteed not to exist
        String badPath = "/tmp/nonexistent-" + System.currentTimeMillis() + "-" + System.nanoTime();

        BindingConfig goodBinding = createBinding("rms", validPath);
        BindingConfig badBinding = createBinding("claims", badPath);

        List<String> errors = validator.validateBindings(List.of(goodBinding, badBinding));

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("claims");
        assertThat(errors.get(0)).doesNotContain("rms");
    }

    @Test
    void createsTmpDirectoryIfNotExists() throws Exception {
        fileSystem.mkdirs(new Path(testBasePath));

        BindingConfig binding = createBinding("test-binding", testBasePath);

        List<String> errors = validator.validateBindings(List.of(binding));

        assertThat(errors).isEmpty();

        Path tmpPath = new Path(testBasePath + "/_tmp/test-instance");
        assertThat(fileSystem.exists(tmpPath)).isTrue();
    }

    @Test
    void cleanupRemovesStaleFilesForOwnInstanceOnly() throws Exception {
        String ownTmpDir = testBasePath + "/_tmp/test-instance";
        String otherTmpDir = testBasePath + "/_tmp/other-instance";
        fileSystem.mkdirs(new Path(ownTmpDir));
        fileSystem.mkdirs(new Path(otherTmpDir));

        Path ownFile = new Path(ownTmpDir + "/stale.seq");
        Path otherFile = new Path(otherTmpDir + "/stale.seq");
        fileSystem.create(ownFile).close();
        fileSystem.create(otherFile).close();

        int deleted = validator.cleanupInstanceTempFiles(testBasePath, 0);

        assertThat(deleted).isEqualTo(1);
        assertThat(fileSystem.exists(ownFile)).isFalse();
        assertThat(fileSystem.exists(otherFile)).isTrue();
    }

    @Test
    void cleanupDoesNotDeleteRecentFiles() throws Exception {
        String tmpDir = testBasePath + "/_tmp/test-instance";
        fileSystem.mkdirs(new Path(tmpDir));

        Path recentFile = new Path(tmpDir + "/recent.seq");
        fileSystem.create(recentFile).close();

        int deleted = validator.cleanupInstanceTempFiles(testBasePath, 3600000);

        assertThat(deleted).isEqualTo(0);
        assertThat(fileSystem.exists(recentFile)).isTrue();
    }

    @Test
    void validateOrFailPassesWithAllValidBindings() throws Exception {
        fileSystem.mkdirs(new Path(testBasePath + "/binding1"));
        fileSystem.mkdirs(new Path(testBasePath + "/binding2"));

        BindingConfig binding1 = createBinding("binding1", testBasePath + "/binding1");
        BindingConfig binding2 = createBinding("binding2", testBasePath + "/binding2");

        assertThatNoException().isThrownBy(() ->
                validator.validateOrFail(List.of(binding1, binding2)));
    }
}
