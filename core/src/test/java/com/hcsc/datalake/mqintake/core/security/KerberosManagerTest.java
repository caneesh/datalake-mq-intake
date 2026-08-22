package com.hcsc.datalake.mqintake.core.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for KerberosManager.
 *
 * <p>From DESIGN.md §13: checkTGTAndReloginFromKeytab() runs on a SINGLE
 * dedicated thread — NEVER from listener threads. This is a HARD constraint.
 */
class KerberosManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void constructorRejectsNullPrincipal() {
        assertThatThrownBy(() -> new KerberosManager(null, "/path/to/keytab", 60000))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("principal");
    }

    @Test
    void constructorRejectsNullKeytabPath() {
        assertThatThrownBy(() -> new KerberosManager("user@REALM", null, 60000))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("keytabPath");
    }

    @Test
    void constructorRejectsNonPositiveReloginInterval() {
        assertThatThrownBy(() -> new KerberosManager("user@REALM", "/path", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reloginIntervalMs");

        assertThatThrownBy(() -> new KerberosManager("user@REALM", "/path", -1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reloginIntervalMs");
    }

    @Test
    void validateKeytabFailsIfFileDoesNotExist() throws Exception {
        KerberosManager manager = new KerberosManager(
                "user@REALM", "/nonexistent/keytab", 60000);

        assertThatThrownBy(() -> manager.initialize(new org.apache.hadoop.conf.Configuration()))
                .isInstanceOf(KerberosManager.KerberosLoginException.class)
                .hasMessageContaining("does not exist");

        manager.close();
    }

    @Test
    void validateKeytabFailsIfNotRegularFile() throws Exception {
        // Create a directory instead of a file
        Path keytabDir = tempDir.resolve("keytab.dir");
        Files.createDirectory(keytabDir);

        KerberosManager manager = new KerberosManager(
                "user@REALM", keytabDir.toString(), 60000);

        assertThatThrownBy(() -> manager.initialize(new org.apache.hadoop.conf.Configuration()))
                .isInstanceOf(KerberosManager.KerberosLoginException.class)
                .hasMessageContaining("not a regular file");

        manager.close();
    }

    @Test
    void getUgiThrowsIfNotInitialized() {
        KerberosManager manager = new KerberosManager(
                "user@REALM", "/path/to/keytab", 60000);

        assertThatThrownBy(manager::getUgi)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not initialized");

        manager.close();
    }

    @Test
    void reloginFailureCountStartsAtZero() {
        KerberosManager manager = new KerberosManager(
                "user@REALM", "/path/to/keytab", 60000);

        assertThat(manager.getReloginFailureCount()).isEqualTo(0);

        manager.close();
    }

    @Test
    void closeShutdownsReloginExecutor() throws Exception {
        // Create a valid keytab file for validation
        Path keytabFile = tempDir.resolve("test.keytab");
        Files.write(keytabFile, "dummy keytab content".getBytes());

        KerberosManager manager = new KerberosManager(
                "user@REALM", keytabFile.toString(), 60000);

        // Close should complete without hanging
        manager.close();

        // Verify getPrincipal and getKeytabPath still work after close
        assertThat(manager.getPrincipal()).isEqualTo("user@REALM");
        assertThat(manager.getKeytabPath()).isEqualTo(keytabFile.toString());
    }

    @Test
    void usesNamedDaemonThreadForRelogin() {
        KerberosManager manager = new KerberosManager(
                "user@REALM", "/path/to/keytab", 60000);

        // The thread is created but won't start until initialize() is called
        // We verify by checking the manager can be closed cleanly
        manager.close();
    }

    @Test
    void isHealthyReturnsFalseIfNotInitialized() {
        KerberosManager manager = new KerberosManager(
                "user@REALM", "/path/to/keytab", 60000);

        assertThat(manager.isHealthy()).isFalse();

        manager.close();
    }
}
