package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.security.KerberosManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import javax.annotation.PreDestroy;
import java.io.IOException;

/**
 * Configures Hadoop FileSystem with optional Kerberos authentication.
 */
@org.springframework.context.annotation.Configuration
public class HdfsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(HdfsConfiguration.class);

    private KerberosManager kerberosManager;

    @Bean
    public Configuration hadoopConfiguration(IntakeProperties properties) {
        Configuration conf = new Configuration();
        // Classpath lookups: work when the config XMLs are packaged or added
        // to the classpath, which is NOT the case for a fat jar started with
        // java -jar. Kept because they cost nothing when absent.
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");

        for (String entry : properties.getHdfs().getConfigResources()) {
            if (entry == null || entry.isBlank()) {
                continue;
            }
            java.io.File file = new java.io.File(entry.trim());
            if (!file.exists()) {
                // Loud, because the alternative is a silent fall back to
                // file:/// and data written to the server's local disk.
                throw new IllegalStateException(
                        "intake.hdfs.config-resources names something that does not exist: "
                                + file.getAbsolutePath()
                                + " — point it at core-site.xml/hdfs-site.xml or the directory "
                                + "holding them (usually /etc/hadoop/conf).");
            }
            if (file.isDirectory()) {
                boolean found = false;
                for (String name : new String[]{"core-site.xml", "hdfs-site.xml"}) {
                    java.io.File resource = new java.io.File(file, name);
                    if (resource.isFile()) {
                        conf.addResource(new org.apache.hadoop.fs.Path(resource.getAbsolutePath()));
                        log.info("Loaded Hadoop configuration: {}", resource.getAbsolutePath());
                        found = true;
                    }
                }
                if (!found) {
                    throw new IllegalStateException(
                            "intake.hdfs.config-resources directory holds neither core-site.xml "
                                    + "nor hdfs-site.xml: " + file.getAbsolutePath());
                }
            } else {
                conf.addResource(new org.apache.hadoop.fs.Path(file.getAbsolutePath()));
                log.info("Loaded Hadoop configuration: {}", file.getAbsolutePath());
            }
        }

        log.info("Hadoop fs.defaultFS resolves to '{}'", conf.get("fs.defaultFS", "file:///"));
        return conf;
    }

    @Bean
    public KerberosManager kerberosManager(IntakeProperties properties, Configuration hadoopConf)
            throws KerberosManager.KerberosLoginException {

        IntakeProperties.KerberosProperties kerberos = properties.getKerberos();

        if (!kerberos.isEnabled()) {
            log.info("Kerberos disabled — using simple authentication");
            return null;
        }

        kerberosManager = new KerberosManager(
                kerberos.getPrincipal(),
                kerberos.getKeytabPath(),
                kerberos.getReloginIntervalMs()
        );

        kerberosManager.initialize(hadoopConf);
        log.info("Kerberos initialized: principal={}", kerberos.getPrincipal());

        return kerberosManager;
    }

    @Bean(destroyMethod = "close")
    public FileSystem fileSystem(Configuration hadoopConf,
                                  IntakeProperties properties,
                                  ProductionMode productionMode,
                                  @Autowired(required = false) KerberosManager kerberosManager)
            throws IOException, InterruptedException {

        FileSystem fs;
        if (kerberosManager != null) {
            fs = kerberosManager.getUgi().doAs(
                    (java.security.PrivilegedExceptionAction<FileSystem>) () ->
                            FileSystem.get(hadoopConf)
            );
            log.info("FileSystem obtained with Kerberos credentials");
        } else {
            fs = FileSystem.get(hadoopConf);
            log.info("FileSystem obtained without Kerberos");
        }

        String scheme = fs.getUri().getScheme();
        log.info("FileSystem is {} ({})", fs.getUri(), fs.getClass().getSimpleName());

        // A fat jar with no cluster configuration resolves to file:/// and
        // lands every batch on the server's own disk — successfully, healthily,
        // and nowhere anyone will look. In production mode that is never what
        // was intended, so refuse rather than run.
        if (productionMode.isEnabled()
                && "file".equals(scheme)
                && !properties.getHdfs().isAllowLocalFilesystem()) {
            throw new IllegalStateException(
                    "Production mode is enabled but the filesystem resolved to '" + fs.getUri()
                            + "' — the LOCAL disk, not HDFS. Point intake.hdfs.config-resources "
                            + "at core-site.xml/hdfs-site.xml (or /etc/hadoop/conf) so the "
                            + "cluster is reachable. Set intake.hdfs.allow-local-filesystem=true "
                            + "only if writing to local disk is genuinely intended.");
        }

        return fs;
    }

    /**
     * Production HdfsPathValidator bean. Declared explicitly (not via
     * component scanning with @ConditionalOnBean, which is evaluated before
     * the FileSystem @Bean exists and silently drops the validator).
     */
    @Bean
    public com.hcsc.datalake.mqintake.core.config.HdfsPathValidator hdfsPathValidator(FileSystem fileSystem) {
        return new com.hcsc.datalake.mqintake.core.hdfs.FileSystemHdfsPathValidator(fileSystem);
    }

    @PreDestroy
    public void shutdown() {
        if (kerberosManager != null) {
            kerberosManager.close();
        }
    }
}
