package com.hcsc.datalake.mqintake.core.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds the Hadoop {@link Configuration} for one specific cluster.
 *
 * <p>The problem this solves is naming the <em>right</em> cluster. A host that
 * already runs another Hadoop client has that client's configuration sitting on
 * disk, and a service that loads "whatever Hadoop finds" will authenticate,
 * connect, write and report healthy against the wrong filesystem. Nothing
 * downstream can detect that: the paths exist, the permissions are fine, the
 * audit trail is clean. Only the data is in the wrong place.
 *
 * <p>So configuration is assembled from two sources and no others: defaults
 * from the Hadoop jars packaged inside this application, and site files from
 * the directory named by {@code intake.hdfs.config-resources}. The host's
 * classpath contributes nothing — with {@code java -jar} it cannot, since that
 * form ignores both {@code $CLASSPATH} and {@code -cp}, and the only jar on the
 * classpath is this one. What the operator points at is what gets loaded.
 *
 * <p>Then the result is checked against {@code intake.hdfs.expected-nameservice}
 * before anything opens a connection, because "pointed at the wrong conf
 * directory" is an ordinary mistake and this is the only place it is still
 * cheap to catch.
 */
public final class HadoopConfigurationFactory {

    private static final Logger log = LoggerFactory.getLogger(HadoopConfigurationFactory.class);

    private static final String[] SITE_FILES = {"core-site.xml", "hdfs-site.xml"};

    private HadoopConfigurationFactory() {
    }

    /**
     * @param hdfs           the {@code intake.hdfs} properties
     * @param productionMode whether production gates are armed
     * @param enforce        whether validation failures throw. False during
     *                       preflight, whose whole purpose is to <em>report</em>
     *                       these failures one line at a time rather than die
     *                       on the first one with a stack trace.
     */
    public static Configuration create(IntakeProperties.HdfsProperties hdfs,
                                       boolean productionMode,
                                       boolean enforce) {

        Configuration conf = newConfiguration(hdfs.isIsolateConfiguration());
        List<String> loaded = loadConfigResources(conf, hdfs.getConfigResources(), enforce);

        // Applied after the site files so an operator can override a single key
        // without editing cluster-managed XML — dfs.client.use.datanode.hostname
        // being the one this deployment is expected to need.
        for (Map.Entry<String, String> override : hdfs.getProperties().entrySet()) {
            conf.set(override.getKey(), override.getValue());
            log.info("Hadoop property override: {} = {}", override.getKey(), override.getValue());
        }

        if (loaded.isEmpty() && productionMode && !hdfs.isAllowLocalFilesystem()) {
            fail(enforce, "Production mode is enabled but intake.hdfs.config-resources is empty, "
                    + "so no cluster configuration was loaded and Hadoop will fall back to its "
                    + "own defaults (fs.defaultFS=file:///). Point it at the cluster's "
                    + "core-site.xml/hdfs-site.xml, or at the directory holding them.");
        }

        String defaultFs = conf.get("fs.defaultFS");
        if (defaultFs == null || defaultFs.trim().isEmpty()) {
            // Only reachable in isolated mode: core-default.xml always supplies
            // a value, and skipping the defaults is what can leave it unset.
            fail(enforce, "fs.defaultFS is not set after loading " + loaded
                    + ". With intake.hdfs.isolate-configuration=true the packaged Hadoop "
                    + "defaults are skipped, so core-site.xml must define it.");
            defaultFs = "";
        }

        validateNameservice(defaultFs, hdfs.getExpectedNameservice(), enforce);

        log.info("Hadoop configuration ready — fs.defaultFS='{}', resources={}", defaultFs,
                loaded.isEmpty() ? "none (packaged defaults only)" : loaded);
        return conf;
    }

    private static Configuration newConfiguration(boolean isolate) {
        if (!isolate) {
            // Defaults come from the Hadoop jars inside this application, which
            // are the versions it was built and tested against.
            return new Configuration();
        }

        // Skipping the defaults also skips the implementation bindings and every
        // tuned client default (retry policy, socket timeouts, block size), so
        // the bindings have to be restored by hand. Offered for environments
        // whose Hadoop team requires it; not the default, because the isolation
        // it buys is already provided by the classpath being just this jar.
        Configuration conf = new Configuration(false);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
        log.info("Hadoop configuration isolated (defaults skipped, hdfs implementations restored)");
        return conf;
    }

    private static List<String> loadConfigResources(Configuration conf, List<String> entries,
                                                    boolean enforce) {
        List<String> loaded = new ArrayList<>();
        for (String entry : entries) {
            if (entry == null || entry.isBlank()) {
                continue;
            }
            File file = new File(entry.trim());
            if (!file.exists()) {
                // Loud, because the alternative is a silent fall back to
                // file:/// or to whichever cluster the defaults name.
                fail(enforce, "intake.hdfs.config-resources names something that does not exist: "
                        + file.getAbsolutePath()
                        + " — point it at core-site.xml/hdfs-site.xml or the directory holding "
                        + "them.");
                continue;
            }
            if (file.isDirectory()) {
                loadFromDirectory(conf, file, loaded, enforce);
            } else {
                addResource(conf, file, loaded);
            }
        }
        return loaded;
    }

    private static void loadFromDirectory(Configuration conf, File dir, List<String> loaded,
                                          boolean enforce) {
        int before = loaded.size();
        for (String name : SITE_FILES) {
            File resource = new File(dir, name);
            if (resource.isFile()) {
                addResource(conf, resource, loaded);
            }
        }
        if (loaded.size() == before) {
            fail(enforce, "intake.hdfs.config-resources directory holds neither core-site.xml "
                    + "nor hdfs-site.xml: " + dir.getAbsolutePath());
        }
    }

    private static void addResource(Configuration conf, File file, List<String> loaded) {
        conf.addResource(new Path(file.getAbsolutePath()));
        loaded.add(file.getAbsolutePath());
        log.info("Loaded Hadoop configuration: {}", file.getAbsolutePath());
    }

    private static void validateNameservice(String defaultFs, String expected, boolean enforce) {
        if (expected == null || expected.isBlank()) {
            log.warn("intake.hdfs.expected-nameservice is not set, so fs.defaultFS ('{}') is "
                    + "accepted as-is. Setting it is what stops a wrong config directory from "
                    + "landing data on another cluster.", defaultFs);
            return;
        }
        if (!defaultFs.contains(expected.trim())) {
            fail(enforce, "fs.defaultFS is '" + defaultFs + "' but intake.hdfs.expected-nameservice "
                    + "is '" + expected.trim() + "'. The loaded configuration names a DIFFERENT "
                    + "cluster than the one this service is configured to write to — check that "
                    + "intake.hdfs.config-resources points at the intended cluster's conf "
                    + "directory.");
            return;
        }
        log.info("fs.defaultFS '{}' matches expected nameservice '{}'", defaultFs, expected.trim());
    }

    private static void fail(boolean enforce, String message) {
        if (enforce) {
            throw new IllegalStateException(message);
        }
        log.warn("{} (not fatal: this is a preflight run, which reports rather than aborts)",
                message);
    }
}
