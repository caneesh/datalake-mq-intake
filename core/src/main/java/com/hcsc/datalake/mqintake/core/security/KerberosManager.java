package com.hcsc.datalake.mqintake.core.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages Kerberos authentication for the service.
 *
 * <p>From DESIGN.md §13: One login per JVM shared across all bindings.
 * checkTGTAndReloginFromKeytab() runs on a SINGLE dedicated thread —
 * NEVER from listener threads. There is a known thread-safety race in
 * UGI TGT handling that produces NPEs under concurrent access.
 *
 * <p>This is a HARD constraint, not a preference. The dedicated thread
 * is the only safe way to handle Kerberos relogin.
 */
public class KerberosManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KerberosManager.class);

    private final String principal;
    private final String keytabPath;
    private final long reloginIntervalMs;
    private final ScheduledExecutorService reloginExecutor;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicLong reloginFailureCount = new AtomicLong(0);
    private final AtomicLong lastSuccessfulRelogin = new AtomicLong(0);

    private volatile UserGroupInformation ugi;

    /**
     * Creates a KerberosManager with the specified configuration.
     *
     * @param principal           the Kerberos principal (service account)
     * @param keytabPath          path to the keytab file (must be outside JAR)
     * @param reloginIntervalMs   interval between relogin checks in milliseconds
     */
    public KerberosManager(String principal, String keytabPath, long reloginIntervalMs) {
        this.principal = Objects.requireNonNull(principal, "principal required");
        this.keytabPath = Objects.requireNonNull(keytabPath, "keytabPath required");
        this.reloginIntervalMs = reloginIntervalMs;

        if (reloginIntervalMs <= 0) {
            throw new IllegalArgumentException("reloginIntervalMs must be positive");
        }

        // CRITICAL: Single dedicated thread for relogin — NEVER use a shared pool
        this.reloginExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kerberos-relogin");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Initializes Kerberos authentication.
     * Must be called before any HDFS operations.
     *
     * @param hadoopConf Hadoop configuration
     * @throws KerberosLoginException if login fails
     */
    public void initialize(Configuration hadoopConf) throws KerberosLoginException {
        if (!initialized.compareAndSet(false, true)) {
            log.warn("KerberosManager already initialized");
            return;
        }

        validateKeytab();

        try {
            // Configure Hadoop for Kerberos
            hadoopConf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(hadoopConf);

            // Login from keytab
            log.info("Logging in as {} from keytab {}", principal, keytabPath);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            ugi = UserGroupInformation.getLoginUser();
            lastSuccessfulRelogin.set(System.currentTimeMillis());

            log.info("Kerberos login successful for principal: {}", ugi.getUserName());

            // Schedule periodic relogin on the SINGLE dedicated thread
            scheduleRelogin();

        } catch (IOException e) {
            initialized.set(false);
            throw new KerberosLoginException("Failed to login from keytab: " + e.getMessage(), e);
        }
    }

    /**
     * Validates that the keytab file exists and is readable.
     */
    private void validateKeytab() throws KerberosLoginException {
        Path keytab = Path.of(keytabPath);
        if (!Files.exists(keytab)) {
            throw new KerberosLoginException("Keytab file does not exist: " + keytabPath);
        }
        if (!Files.isReadable(keytab)) {
            throw new KerberosLoginException("Keytab file is not readable: " + keytabPath);
        }
        if (!Files.isRegularFile(keytab)) {
            throw new KerberosLoginException("Keytab path is not a regular file: " + keytabPath);
        }
    }

    /**
     * Schedules periodic TGT relogin on the dedicated thread.
     * CRITICAL: This must run on a SINGLE thread, never from listener threads.
     */
    private void scheduleRelogin() {
        reloginExecutor.scheduleAtFixedRate(
                this::performRelogin,
                reloginIntervalMs,
                reloginIntervalMs,
                TimeUnit.MILLISECONDS
        );
        log.info("Scheduled Kerberos relogin every {} ms", reloginIntervalMs);
    }

    /**
     * Performs TGT check and relogin if needed.
     * CRITICAL: This method runs ONLY on the dedicated relogin thread.
     */
    private void performRelogin() {
        try {
            if (ugi == null) {
                log.warn("UGI is null, cannot perform relogin");
                reloginFailureCount.incrementAndGet();
                return;
            }

            // checkTGTAndReloginFromKeytab() is NOT thread-safe
            // It must only be called from this single dedicated thread
            ugi.checkTGTAndReloginFromKeytab();
            lastSuccessfulRelogin.set(System.currentTimeMillis());

            log.debug("Kerberos TGT check/relogin completed successfully");

        } catch (IOException e) {
            reloginFailureCount.incrementAndGet();
            log.error("Kerberos relogin failed: {}", e.getMessage(), e);
        } catch (Exception e) {
            // Catch any unexpected exceptions to prevent the scheduled task from dying
            reloginFailureCount.incrementAndGet();
            log.error("Unexpected error during Kerberos relogin: {}", e.getMessage(), e);
        }
    }

    /**
     * Returns the current UserGroupInformation.
     * Safe to call from any thread — only reads the volatile reference.
     */
    public UserGroupInformation getUgi() {
        if (!initialized.get()) {
            throw new IllegalStateException("KerberosManager not initialized");
        }
        return ugi;
    }

    /**
     * Returns the count of relogin failures since initialization.
     */
    public long getReloginFailureCount() {
        return reloginFailureCount.get();
    }

    /**
     * Returns the timestamp of the last successful relogin.
     */
    public long getLastSuccessfulRelogin() {
        return lastSuccessfulRelogin.get();
    }

    /**
     * Returns true if Kerberos is initialized and healthy.
     */
    public boolean isHealthy() {
        if (!initialized.get() || ugi == null) {
            return false;
        }
        // Consider unhealthy if no successful relogin in the last 2 intervals
        long timeSinceLastRelogin = System.currentTimeMillis() - lastSuccessfulRelogin.get();
        return timeSinceLastRelogin < (reloginIntervalMs * 2);
    }

    public String getPrincipal() {
        return principal;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    @Override
    public void close() {
        log.info("Shutting down KerberosManager");
        reloginExecutor.shutdown();
        try {
            if (!reloginExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                reloginExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            reloginExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Exception thrown when Kerberos login fails.
     */
    public static class KerberosLoginException extends Exception {
        public KerberosLoginException(String message) {
            super(message);
        }

        public KerberosLoginException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
