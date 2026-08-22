package com.hcsc.datalake.mqintake.core.hdfs;

import com.hcsc.datalake.mqintake.core.config.HdfsPathValidator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * Production implementation of HdfsPathValidator that checks actual HDFS paths.
 *
 * <p>Validates:
 * <ul>
 *   <li>Base path is writable (can create directories)</li>
 *   <li>_tmp subdirectory can be created</li>
 *   <li>Files can be created in the path</li>
 * </ul>
 */
@Component
@ConditionalOnBean(FileSystem.class)
public class FileSystemHdfsPathValidator implements HdfsPathValidator {

    private static final Logger log = LoggerFactory.getLogger(FileSystemHdfsPathValidator.class);

    private final FileSystem fileSystem;

    public FileSystemHdfsPathValidator(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public PathValidationResult validatePath(String hdfsBasePath) {
        if (hdfsBasePath == null || hdfsBasePath.isBlank()) {
            return PathValidationResult.failure("Path is null or blank");
        }

        Path basePath = new Path(hdfsBasePath);

        try {
            if (!fileSystem.exists(basePath)) {
                return tryCreatePath(basePath);
            }

            return checkWriteAccess(basePath);

        } catch (AccessControlException e) {
            return PathValidationResult.failure("Permission denied: " + hdfsBasePath);
        } catch (IOException e) {
            return PathValidationResult.failure("IO error checking path: " + e.getMessage());
        }
    }

    private PathValidationResult tryCreatePath(Path basePath) throws IOException {
        try {
            boolean created = fileSystem.mkdirs(basePath);
            if (!created) {
                return PathValidationResult.failure("Cannot create directory: " + basePath);
            }
            return checkWriteAccess(basePath);
        } catch (AccessControlException e) {
            return PathValidationResult.failure("Permission denied: " + basePath);
        }
    }

    private PathValidationResult checkWriteAccess(Path basePath) throws IOException {
        Path tmpPath = new Path(basePath, "_tmp");

        try {
            if (!fileSystem.exists(tmpPath)) {
                fileSystem.mkdirs(tmpPath);
            }

            Path testFile = new Path(tmpPath, ".write_test_" + UUID.randomUUID());
            try {
                fileSystem.create(testFile, false).close();
                fileSystem.delete(testFile, false);
            } catch (AccessControlException e) {
                return PathValidationResult.failure("Cannot write to _tmp directory: " + tmpPath);
            }

            log.debug("HDFS path validation successful: {}", basePath);
            return PathValidationResult.success();

        } catch (AccessControlException e) {
            return PathValidationResult.failure("Cannot create _tmp directory: " + tmpPath);
        }
    }
}
