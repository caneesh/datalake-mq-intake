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
    public Configuration hadoopConfiguration(IntakeProperties properties,
                                             ProductionMode productionMode) {
        // Preflight exists to report a misconfigured cluster one line at a
        // time; throwing here would kill the context before it prints anything.
        boolean enforce = !properties.getPreflight().isEnabled();
        return HadoopConfigurationFactory.create(
                properties.getHdfs(), productionMode.isEnabled(), enforce);
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

        try {
            kerberosManager.initialize(hadoopConf);
        } catch (KerberosManager.KerberosLoginException e) {
            if (!properties.getPreflight().isEnabled()) {
                throw e;
            }
            // A wrong keytab path is the likeliest thing to be wrong on a first
            // deployment, and preflight is what an operator runs to find that
            // out. Failing the context here buries one actionable line under
            // six frames of Spring wrapping and prints no report at all — so
            // during preflight the failure is carried into the report instead,
            // where it is one line with a remedy. Nothing is consumed and
            // nothing is written either way.
            log.warn("Kerberos login failed: {} — reported by preflight rather than aborting",
                    e.getMessage());
            kerberosManager = null;
            return null;
        }
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

            // Preflight is the diagnostic that exists to *report* this, and it
            // starts no listener, so nothing can land while it runs. Throwing
            // here would kill the context before the report is printed and
            // replace one actionable line with a page of Spring stack trace —
            // and it would block an MQ-only run on a host where the cluster
            // configuration is not in place yet.
            if (properties.getPreflight().isEnabled()) {
                log.warn("Filesystem resolved to '{}' — the LOCAL disk, not HDFS. Allowed only "
                        + "because this is a preflight run; the hdfs checks report it.", fs.getUri());
                return fs;
            }

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
