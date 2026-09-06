package com.hcsc.datalake.mqintake.core.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What an operator can put in {@code HDFS_CONFIG_RESOURCES}.
 *
 * <p>DEPLOYMENT.md tells operators they may name files instead of a directory,
 * comma-separated, when the cluster needs a file the directory form will not
 * read — {@code ssl-client.xml} for HDFS wire encryption being the usual case.
 * That instruction is only true if two things hold, and neither was tested:
 * the comma-separated value has to bind to a list, and a named file that is
 * not one of the two site files has to actually load.
 *
 * <p>A runbook instruction nobody can run is worse than no instruction: it is
 * followed once, at 2am, on a cluster that will not come up.
 */
class ConfigResourcesBindingTest {

    @Test
    void aCommaSeparatedValueBindsToOneEntryPerFile() {
        ConfigurationPropertySource source = new MapConfigurationPropertySource(Map.of(
                "intake.hdfs.config-resources",
                "/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml,"
                        + "/etc/hadoop/conf/ssl-client.xml"));

        IntakeProperties properties = new Binder(source).bind("intake", IntakeProperties.class).get();

        assertThat(properties.getHdfs().getConfigResources())
                .containsExactly("/etc/hadoop/conf/core-site.xml",
                        "/etc/hadoop/conf/hdfs-site.xml",
                        "/etc/hadoop/conf/ssl-client.xml");
    }

    @Test
    void aNamedFileLoadsEvenWhenItIsNotOneOfTheTwoSiteFiles(@TempDir Path tmp) throws IOException {
        // The point of the advice. A DIRECTORY reads core-site.xml and
        // hdfs-site.xml and nothing else, so a cluster needing ssl-client.xml
        // has to name its files — which only helps if naming one works.
        Path core = write(tmp, "core-site.xml", "fs.defaultFS", "hdfs://testnn");
        Path ssl = write(tmp, "ssl-client.xml", "ssl.client.truststore.location", "/etc/pki/ts.jks");

        IntakeProperties.HdfsProperties hdfs = new IntakeProperties.HdfsProperties();
        hdfs.setConfigResources(java.util.List.of(core.toString(), ssl.toString()));

        Configuration conf = HadoopConfigurationFactory.create(hdfs, false, true);

        assertThat(conf.get("fs.defaultFS")).isEqualTo("hdfs://testnn");
        assertThat(conf.get("ssl.client.truststore.location"))
                .as("a named non-site file must load, or the runbook advice is wrong")
                .isEqualTo("/etc/pki/ts.jks");
    }

    @Test
    void thatSameFileIsIgnoredWhenTheDIRECTORYIsNamedInstead(@TempDir Path tmp) throws IOException {
        // The other half, and the reason the advice exists at all.
        write(tmp, "core-site.xml", "fs.defaultFS", "hdfs://testnn");
        write(tmp, "ssl-client.xml", "ssl.client.truststore.location", "/etc/pki/ts.jks");

        IntakeProperties.HdfsProperties hdfs = new IntakeProperties.HdfsProperties();
        hdfs.setConfigResources(java.util.List.of(tmp.toString()));

        Configuration conf = HadoopConfigurationFactory.create(hdfs, false, true);

        assertThat(conf.get("fs.defaultFS")).isEqualTo("hdfs://testnn");
        assertThat(conf.get("ssl.client.truststore.location"))
                .as("a directory reads the two site files and nothing else")
                .isNull();
    }

    private Path write(Path dir, String name, String key, String value) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file,
                "<?xml version=\"1.0\"?><configuration><property>"
                        + "<name>" + key + "</name><value>" + value + "</value>"
                        + "</property></configuration>");
        return file;
    }
}
