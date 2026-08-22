package com.hcsc.datalake.mqintake.core.hdfs;

import com.hcsc.datalake.mqintake.core.config.HdfsPathValidator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

class FileSystemHdfsPathValidatorTest {

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem fileSystem;
    private FileSystemHdfsPathValidator validator;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(conf);
        validator = new FileSystemHdfsPathValidator(fileSystem);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    @Test
    void validatesExistingWritablePath() {
        String path = tempDir.resolve("data").toString();

        HdfsPathValidator.PathValidationResult result = validator.validatePath(path);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getError()).isNull();
    }

    @Test
    void createsPathIfNotExists() {
        String path = tempDir.resolve("new/nested/path").toString();

        HdfsPathValidator.PathValidationResult result = validator.validatePath(path);

        assertThat(result.isValid()).isTrue();
    }

    @Test
    void createsTmpSubdirectory() throws IOException {
        String basePath = tempDir.resolve("data").toString();

        validator.validatePath(basePath);

        Path tmpPath = new Path(basePath, "_tmp");
        assertThat(fileSystem.exists(tmpPath)).isTrue();
    }

    @Test
    void failsForNullPath() {
        HdfsPathValidator.PathValidationResult result = validator.validatePath(null);

        assertThat(result.isValid()).isFalse();
        assertThat(result.getError()).contains("null or blank");
    }

    @Test
    void failsForBlankPath() {
        HdfsPathValidator.PathValidationResult result = validator.validatePath("   ");

        assertThat(result.isValid()).isFalse();
        assertThat(result.getError()).contains("null or blank");
    }

    @Test
    void pathValidationResultSuccessFactory() {
        HdfsPathValidator.PathValidationResult result = HdfsPathValidator.PathValidationResult.success();

        assertThat(result.isValid()).isTrue();
        assertThat(result.getError()).isNull();
    }

    @Test
    void pathValidationResultFailureFactory() {
        HdfsPathValidator.PathValidationResult result =
            HdfsPathValidator.PathValidationResult.failure("test error");

        assertThat(result.isValid()).isFalse();
        assertThat(result.getError()).isEqualTo("test error");
    }
}
