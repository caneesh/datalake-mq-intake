package com.hcsc.datalake.mqintake.core.lifecycle;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Validates startup prerequisites before any listener threads start.
 *
 * <p>From DESIGN.md §13 and §14: Verify write access to every configured
 * binding base path (including its _tmp directory) at startup, and fail
 * fast with a clear error if not. A permission gap discovered at first
 * flush means a rollback loop on a payment queue.
 *
 * <p>Validates the ENTIRE binding set before starting ANY listener —
 * fail fast on a bad config rather than starting half the bindings.
 */
public class StartupValidator {

    private static final Logger log = LoggerFactory.getLogger(StartupValidator.class);

    private final FileSystem fileSystem;
    private final String instanceId;

    public StartupValidator(FileSystem fileSystem, String instanceId) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId required");
    }

    /**
     * Validates all bindings and returns any validation errors.
     * Returns an empty list if all validations pass.
     *
     * @param bindings the bindings to validate
     * @return list of validation errors (empty if all pass)
     */
    public List<String> validateBindings(List<BindingConfig> bindings) {
        List<String> errors = new ArrayList<>();

        for (BindingConfig binding : bindings) {
            errors.addAll(validateBinding(binding));
        }

        return errors;
    }

    /**
     * Validates a single binding's HDFS paths.
     */
    private List<String> validateBinding(BindingConfig binding) {
        List<String> errors = new ArrayList<>();
        String basePath = binding.getHdfsBasePath();
        String bindingId = binding.getId();

        // Validate base path exists and is writable
        boolean basePathValid = false;
        try {
            validatePathWritable(basePath, bindingId, "base path");
            basePathValid = true;
        } catch (ValidationException e) {
            errors.add(e.getMessage());
        }

        // Only validate _tmp if base path is valid
        // (validateOrCreateTmpPath uses mkdirs which would create the base path)
        if (basePathValid) {
            String tmpPath = PartitionPath.tempDir(basePath, instanceId);
            try {
                validateOrCreateTmpPath(tmpPath, bindingId);
            } catch (ValidationException e) {
                errors.add(e.getMessage());
            }
        }

        return errors;
    }

    /**
     * Validates that a path is writable.
     */
    private void validatePathWritable(String pathStr, String bindingId, String description)
            throws ValidationException {
        Path path = new Path(pathStr);

        try {
            if (!fileSystem.exists(path)) {
                throw new ValidationException(String.format(
                        "Binding '%s' %s does not exist: %s", bindingId, description, pathStr));
            }

            // Check write permission
            fileSystem.access(path, FsAction.WRITE);

            log.debug("Validated write access to {} for binding '{}': {}",
                    description, bindingId, pathStr);

        } catch (org.apache.hadoop.security.AccessControlException e) {
            throw new ValidationException(String.format(
                    "Binding '%s' %s is not writable: %s — %s",
                    bindingId, description, pathStr, e.getMessage()));
        } catch (IOException e) {
            throw new ValidationException(String.format(
                    "Binding '%s' %s validation failed: %s — %s",
                    bindingId, description, pathStr, e.getMessage()));
        }
    }

    /**
     * Validates or creates the _tmp directory for this instance.
     */
    private void validateOrCreateTmpPath(String tmpPathStr, String bindingId)
            throws ValidationException {
        Path tmpPath = new Path(tmpPathStr);

        try {
            if (!fileSystem.exists(tmpPath)) {
                // Try to create the _tmp directory
                boolean created = fileSystem.mkdirs(tmpPath);
                if (!created) {
                    throw new ValidationException(String.format(
                            "Binding '%s' failed to create _tmp directory: %s",
                            bindingId, tmpPathStr));
                }
                log.info("Created _tmp directory for binding '{}': {}", bindingId, tmpPathStr);
            }

            // Verify write access
            fileSystem.access(tmpPath, FsAction.WRITE);

            log.debug("Validated write access to _tmp for binding '{}': {}", bindingId, tmpPathStr);

        } catch (org.apache.hadoop.security.AccessControlException e) {
            throw new ValidationException(String.format(
                    "Binding '%s' _tmp directory is not writable: %s — %s",
                    bindingId, tmpPathStr, e.getMessage()));
        } catch (IOException e) {
            throw new ValidationException(String.format(
                    "Binding '%s' _tmp directory validation failed: %s — %s",
                    bindingId, tmpPathStr, e.getMessage()));
        }
    }

    /**
     * Cleans up stale temp files for THIS instance only.
     * From §8.1: On startup, sweep ONLY this instance's own _tmp/{instance_id}/
     * subtree for crash debris — never another instance's.
     *
     * @param basePath  the HDFS base path
     * @param maxAgeMs  maximum age in milliseconds
     * @return number of files deleted
     */
    public int cleanupInstanceTempFiles(String basePath, long maxAgeMs) throws IOException {
        String tmpDir = PartitionPath.tempDir(basePath, instanceId);
        Path tmpPath = new Path(tmpDir);

        if (!fileSystem.exists(tmpPath)) {
            return 0;
        }

        int deleted = 0;
        long cutoff = System.currentTimeMillis() - maxAgeMs;

        for (var status : fileSystem.listStatus(tmpPath)) {
            if (status.isFile() && status.getModificationTime() < cutoff) {
                if (fileSystem.delete(status.getPath(), false)) {
                    log.info("Deleted stale temp file from crash debris: {}", status.getPath());
                    deleted++;
                }
            }
        }

        if (deleted > 0) {
            log.info("Cleaned up {} stale temp files for instance '{}' at {}",
                    deleted, instanceId, tmpDir);
        }

        return deleted;
    }

    /**
     * Validates all bindings and throws if any validation fails.
     *
     * @param bindings the bindings to validate
     * @throws StartupValidationException if any validation fails
     */
    public void validateOrFail(List<BindingConfig> bindings) throws StartupValidationException {
        List<String> errors = validateBindings(bindings);

        if (!errors.isEmpty()) {
            String message = "Startup validation failed:\n  - " + String.join("\n  - ", errors);
            log.error(message);
            throw new StartupValidationException(message, errors);
        }

        log.info("Startup validation passed for {} bindings", bindings.size());
    }

    private static class ValidationException extends Exception {
        ValidationException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when startup validation fails.
     */
    public static class StartupValidationException extends Exception {
        private final List<String> errors;

        public StartupValidationException(String message, List<String> errors) {
            super(message);
            this.errors = List.copyOf(errors);
        }

        public List<String> getErrors() {
            return errors;
        }
    }
}
