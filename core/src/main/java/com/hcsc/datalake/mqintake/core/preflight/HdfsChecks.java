package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.audit.AuditPaths;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Probes the storage side: reachability, permissions, and the durability
 * sequence the delivery guarantee rests on.
 *
 * <p><strong>Everything is written inside {@code _tmp/{instanceId}} and
 * removed.</strong> A probe file must never land in a data partition: the
 * partition is what reconciliation reads, and a stray file there would be
 * classified as an orphan on the next pass — preflight would create the
 * incident it exists to prevent. Staging inside the instance's own temp tree
 * also means anything left behind by an interrupted run is swept by the
 * startup cleanup that already owns that path.
 */
public final class HdfsChecks {

    private HdfsChecks() {
    }

    public static List<PreflightCheck> forAllBindings(IntakeProperties properties,
                                                      FileSystem fileSystem,
                                                      String instanceId) {
        List<PreflightCheck> checks = new ArrayList<>();
        checks.add(clusterConfigLoaded(properties));
        checks.add(filesystemReachable(fileSystem,
                properties.getHdfs().isAllowLocalFilesystem()));
        checks.add(nameserviceMatches(fileSystem, properties.getHdfs().getExpectedNameservice()));
        for (BindingConfig binding : properties.getBindings()) {
            String basePath = binding.getHdfs().getBasePath();
            checks.add(pathWritable("hdfs", binding.getId() + ".landing-path", basePath,
                    "the binding's landing directory exists and accepts writes", fileSystem));
            checks.add(tempWritable(binding, basePath, fileSystem, instanceId));
            checks.add(auditWritable(binding, properties, fileSystem));
            checks.add(durabilityRoundTrip(binding, basePath, fileSystem, instanceId));
        }
        return checks;
    }

    /**
     * Reports which cluster configuration files were found, before anything
     * tries to use them. On a host that also carries another Hadoop client's
     * configuration, "which directory did it read" is the first question worth
     * answering.
     */
    private static PreflightCheck clusterConfigLoaded(IntakeProperties properties) {
        return new MqChecks.AbstractCheck("hdfs", "cluster-config.resources",
                "the configured cluster's core-site.xml and hdfs-site.xml are present") {
            @Override
            public CheckOutcome run() {
                List<String> entries = properties.getHdfs().getConfigResources();
                List<String> found = new ArrayList<>();
                List<String> missing = new ArrayList<>();
                for (String entry : entries) {
                    if (entry == null || entry.isBlank()) {
                        continue;
                    }
                    java.io.File file = new java.io.File(entry.trim());
                    if (!file.exists()) {
                        missing.add(file.getAbsolutePath());
                    } else if (file.isDirectory()) {
                        boolean any = false;
                        for (String name : new String[]{"core-site.xml", "hdfs-site.xml"}) {
                            java.io.File resource = new java.io.File(file, name);
                            if (resource.isFile()) {
                                found.add(resource.getAbsolutePath());
                                any = true;
                            }
                        }
                        if (!any) {
                            missing.add(file.getAbsolutePath()
                                    + " (holds neither core-site.xml nor hdfs-site.xml)");
                        }
                    } else {
                        found.add(file.getAbsolutePath());
                    }
                }
                if (!missing.isEmpty()) {
                    return CheckOutcome.fail("cannot read " + missing,
                            "Set intake.hdfs.config-resources (HDFS_CONFIG_RESOURCES) to the "
                                    + "target cluster's conf directory.");
                }
                if (found.isEmpty() && properties.getHdfs().isAllowLocalFilesystem()) {
                    // Mirrors the application's own rule: writing to local disk
                    // is a legitimate configuration once asked for, and
                    // preflight must not be stricter than the service it
                    // predicts.
                    return CheckOutcome.skip("none configured — local filesystem explicitly "
                            + "allowed");
                }
                if (found.isEmpty()) {
                    return CheckOutcome.fail("no cluster configuration is configured",
                            "Without core-site.xml/hdfs-site.xml Hadoop uses its packaged "
                                    + "defaults and resolves fs.defaultFS to file:/// — the local "
                                    + "disk. Set intake.hdfs.config-resources "
                                    + "(HDFS_CONFIG_RESOURCES).");
                }
                return CheckOutcome.pass("loaded " + found);
            }
        };
    }

    /**
     * The wrong conf directory is an ordinary mistake, and every other check
     * passes cheerfully after it: the connection works, the paths exist, the
     * writes succeed. Only the cluster is wrong. This is the check that says so.
     */
    private static PreflightCheck nameserviceMatches(FileSystem fileSystem, String expected) {
        return new MqChecks.AbstractCheck("hdfs", "filesystem.nameservice",
                "fs.defaultFS names the cluster this service is configured to write to") {
            @Override
            public CheckOutcome run() {
                if (expected == null || expected.isBlank()) {
                    return CheckOutcome.skip("intake.hdfs.expected-nameservice not configured — "
                            + "setting it is what makes a wrong conf directory detectable");
                }
                String uri = String.valueOf(fileSystem.getUri());
                if (!uri.contains(expected.trim())) {
                    return CheckOutcome.fail(
                            "fs.defaultFS is '" + uri + "', which does not name '"
                                    + expected.trim() + "'",
                            "The loaded configuration points at a DIFFERENT cluster than the one "
                                    + "configured. Check that intake.hdfs.config-resources names "
                                    + "the intended cluster's conf directory — on a host that "
                                    + "carries more than one Hadoop client configuration this is "
                                    + "the mistake that silently lands data on the wrong "
                                    + "cluster.");
                }
                return CheckOutcome.pass(uri + " matches expected nameservice '"
                        + expected.trim() + "'");
            }
        };
    }

    private static PreflightCheck filesystemReachable(FileSystem fileSystem,
                                                      boolean allowLocalFilesystem) {
        return new MqChecks.AbstractCheck("hdfs", "filesystem.connect",
                "the configured filesystem answers, under the identity the service will use") {
            @Override
            public CheckOutcome run() {
                try {
                    String uri = String.valueOf(fileSystem.getUri());
                    String user = org.apache.hadoop.security.UserGroupInformation
                            .getCurrentUser().getUserName();
                    fileSystem.getStatus();   // a real round trip, not just a handle
                    if ("file".equals(fileSystem.getUri().getScheme()) && !allowLocalFilesystem) {
                        // Reachable, yes — but this is the server's own disk.
                        // Saying "pass" without saying that would let a
                        // connectivity test certify the wrong destination.
                        return CheckOutcome.fail(
                                "resolved to the LOCAL filesystem (" + uri + "), not HDFS",
                                "A fat jar started with java -jar has no cluster configuration on "
                                        + "its classpath, so Hadoop falls back to file:///. Set "
                                        + "intake.hdfs.config-resources (HDFS_CONFIG_RESOURCES) to "
                                        + "/etc/hadoop/conf or the core-site.xml/hdfs-site.xml "
                                        + "files. If local disk is genuinely intended, set "
                                        + "intake.hdfs.allow-local-filesystem=true.");
                    }
                    if ("file".equals(fileSystem.getUri().getScheme())) {
                        return CheckOutcome.pass(uri + " reachable as '" + user
                                + "' — LOCAL filesystem, explicitly allowed");
                    }
                    return CheckOutcome.pass(uri + " reachable as '" + user + "'");
                } catch (Exception e) {
                    return CheckOutcome.fail("filesystem unreachable", e,
                            "Check fs.defaultFS and core-site/hdfs-site on the classpath. If "
                                    + "Kerberos is enabled, a GSS failure here means the keytab "
                                    + "or principal is wrong for this realm.");
                }
            }
        };
    }

    private static PreflightCheck pathWritable(String group, String name, String path,
                                               String describes, FileSystem fileSystem) {
        return new MqChecks.AbstractCheck(group, name, describes) {
            @Override
            public CheckOutcome run() {
                if (path == null || path.isBlank()) {
                    return CheckOutcome.skip("not configured");
                }
                try {
                    Path target = new Path(path);
                    if (!fileSystem.exists(target)) {
                        return CheckOutcome.fail("does not exist: " + path,
                                "Create it and grant write to the service principal. The "
                                        + "application refuses to start without it.");
                    }
                    fileSystem.access(target, FsAction.WRITE);
                    return CheckOutcome.pass(path + " exists and is writable");
                } catch (Exception e) {
                    return CheckOutcome.fail("not writable: " + path, e,
                            "Grant write to the service principal (or fix the Kerberos identity "
                                    + "— an AccessControlException naming a different user means "
                                    + "the process authenticated as someone else).");
                }
            }
        };
    }

    private static PreflightCheck tempWritable(BindingConfig binding, String basePath,
                                               FileSystem fileSystem, String instanceId) {
        return new MqChecks.AbstractCheck("hdfs", binding.getId() + ".temp-path",
                "this instance's staging directory can be created and written") {
            @Override
            public CheckOutcome run() {
                if (basePath == null || basePath.isBlank()) {
                    return CheckOutcome.skip("no base path configured");
                }
                String tempDir = PartitionPath.tempDir(basePath, instanceId);
                try {
                    Path target = new Path(tempDir);
                    if (!fileSystem.exists(target) && !fileSystem.mkdirs(target)) {
                        return CheckOutcome.fail("could not create " + tempDir,
                                "Every batch stages here before it is renamed into its "
                                        + "partition; without it nothing can land.");
                    }
                    fileSystem.access(target, FsAction.WRITE);
                    return CheckOutcome.pass(tempDir + " ready");
                } catch (Exception e) {
                    return CheckOutcome.fail("staging directory unusable: " + tempDir, e, null);
                }
            }
        };
    }

    private static PreflightCheck auditWritable(BindingConfig binding, IntakeProperties properties,
                                                FileSystem fileSystem) {
        String auditBase = properties.getHdfs().getAuditBasePath();
        String path = (auditBase == null || auditBase.isBlank())
                ? null : AuditPaths.bindingDir(auditBase, binding.getId());
        return new MqChecks.AbstractCheck("hdfs", binding.getId() + ".audit-path",
                "audit records can be written — they are fail-closed, so this blocks ingestion") {
            @Override
            public CheckOutcome run() {
                if (path == null) {
                    return CheckOutcome.skip("no audit base path configured");
                }
                try {
                    // Mirrors StartupValidator, which mkdirs the binding's
                    // audit directory: a missing-but-creatable directory is
                    // normal on a fresh environment, and reporting it as a
                    // failure would make preflight stricter than the service
                    // it predicts.
                    Path target = new Path(path);
                    boolean created = false;
                    if (!fileSystem.exists(target)) {
                        if (!fileSystem.mkdirs(target)) {
                            return CheckOutcome.fail("does not exist and could not be created: " + path,
                                    "The audit trail is a control: an unwritable audit path stops "
                                            + "ingestion at the first batch.");
                        }
                        created = true;
                    }
                    fileSystem.access(target, FsAction.WRITE);
                    return CheckOutcome.pass(path + (created ? " created and writable" : " writable"));
                } catch (Exception e) {
                    return CheckOutcome.fail("audit path unusable: " + path, e,
                            "The audit trail is a control: an unwritable audit path stops "
                                    + "ingestion at the first batch.");
                }
            }
        };
    }

    private static PreflightCheck durabilityRoundTrip(BindingConfig binding, String basePath,
                                                      FileSystem fileSystem, String instanceId) {
        return new MqChecks.AbstractCheck("hdfs", binding.getId() + ".durability-roundtrip",
                "write, hsync, close, atomic rename and read-back all work on this cluster") {
            @Override
            public CheckOutcome run() {
                if (basePath == null || basePath.isBlank()) {
                    return CheckOutcome.skip("no base path configured");
                }
                String tempDir = PartitionPath.tempDir(basePath, instanceId);
                String token = "preflight-" + UUID.randomUUID();
                Path staged = new Path(tempDir, token + ".probe");
                Path renamed = new Path(tempDir, token + ".probe.done");
                byte[] payload = ("preflight " + token).getBytes(StandardCharsets.UTF_8);
                boolean hsynced = false;
                try {
                    fileSystem.mkdirs(new Path(tempDir));

                    // The exact sequence a batch performs: write, force to
                    // disk, close, then rename to publish.
                    try (FSDataOutputStream out = fileSystem.create(staged, true)) {
                        out.write(payload);
                        out.hsync();
                        hsynced = true;
                    }
                    if (!fileSystem.rename(staged, renamed)) {
                        return CheckOutcome.fail("rename returned false: " + staged + " -> " + renamed,
                                "Rename is how a batch becomes visible. If it fails here it will "
                                        + "fail for every batch — check directory permissions and "
                                        + "that the target parent exists.");
                    }
                    byte[] readBack = new byte[payload.length];
                    try (FSDataInputStream in = fileSystem.open(renamed)) {
                        in.readFully(readBack);
                    }
                    if (!java.util.Arrays.equals(payload, readBack)) {
                        return CheckOutcome.fail("read-back did not match what was written",
                                "The cluster acknowledged a write it did not store faithfully. "
                                        + "Escalate to the platform team before landing real data.");
                    }
                    return CheckOutcome.pass("write -> hsync -> close -> rename -> read-back "
                            + "verified (" + payload.length + " bytes, probe removed)");
                } catch (UnsupportedOperationException e) {
                    return CheckOutcome.fail("hsync is not supported by this filesystem", e,
                            "hsync is the durability floor for a no-loss feed. On a filesystem "
                                    + "without it, set hdfs.hsync-on-flush=false only with an "
                                    + "explicit decision to accept the power-loss window.");
                } catch (Exception e) {
                    return CheckOutcome.fail(
                            "durability sequence failed" + (hsynced ? " after hsync" : " before hsync"),
                            e, "This is the exact sequence every batch performs; the application "
                                    + "cannot land data until it works.");
                } finally {
                    deleteQuietly(fileSystem, staged);
                    deleteQuietly(fileSystem, renamed);
                }
            }
        };
    }

    private static void deleteQuietly(FileSystem fileSystem, Path path) {
        try {
            fileSystem.delete(path, false);
        } catch (Exception ignored) {
            // Probe debris lives under _tmp/{instanceId}, which the startup
            // sweep owns — a failed cleanup is untidy, never harmful.
        }
    }
}
