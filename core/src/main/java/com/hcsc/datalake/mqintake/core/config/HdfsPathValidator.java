package com.hcsc.datalake.mqintake.core.config;

/**
 * Strategy interface for validating HDFS paths are writable.
 * Allows for testing without a real HDFS connection.
 */
public interface HdfsPathValidator {

    /**
     * Checks if the given base path and its _tmp subdirectory are writable.
     *
     * @param hdfsBasePath the HDFS base path to check
     * @return validation result
     */
    PathValidationResult validatePath(String hdfsBasePath);

    /**
     * Result of path validation.
     */
    class PathValidationResult {
        private final boolean valid;
        private final String error;

        private PathValidationResult(boolean valid, String error) {
            this.valid = valid;
            this.error = error;
        }

        public static PathValidationResult success() {
            return new PathValidationResult(true, null);
        }

        public static PathValidationResult failure(String error) {
            return new PathValidationResult(false, error);
        }

        public boolean isValid() {
            return valid;
        }

        public String getError() {
            return error;
        }
    }
}
