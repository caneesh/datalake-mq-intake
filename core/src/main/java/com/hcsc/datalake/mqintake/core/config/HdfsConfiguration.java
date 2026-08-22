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
    public Configuration hadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
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

        return fs;
    }

    @PreDestroy
    public void shutdown() {
        if (kerberosManager != null) {
            kerberosManager.close();
        }
    }
}
