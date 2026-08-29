package com.hcsc.datalake.mqintake.core.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The wrong conf directory is the failure this class exists to prevent.
 *
 * <p>On a host that already runs another Hadoop client, pointing at the wrong
 * configuration produces a service that connects, authenticates, writes and
 * reports healthy — against someone else's cluster. No downstream check can
 * see it: the paths exist, the permissions are right, the audit trail balances.
 * These tests pin the two things that can: what gets loaded, and what
 * {@code fs.defaultFS} is allowed to say afterwards.
 */
class HadoopConfigurationFactoryTest {

    private static final boolean PRODUCTION = true;
    private static final boolean DEV = false;
    private static final boolean ENFORCE = true;
    private static final boolean REPORT_ONLY = false;

    private IntakeProperties.HdfsProperties props(Path... resources) {
        IntakeProperties.HdfsProperties hdfs = new IntakeProperties.HdfsProperties();
        hdfs.setConfigResources(java.util.Arrays.stream(resources)
                .map(Path::toString).collect(java.util.stream.Collectors.toList()));
        return hdfs;
    }

    /** A minimal core-site.xml declaring one property. */
    private void writeSite(Path dir, String fileName, String key, String value) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(fileName),
                "<?xml version=\"1.0\"?><configuration><property>"
                        + "<name>" + key + "</name><value>" + value + "</value>"
                        + "</property></configuration>");
    }

    @Test
    void aConfigDirectoryThatDoesNotExistIsRejected(@TempDir Path tmp) {
        IntakeProperties.HdfsProperties hdfs = props(tmp.resolve("absent"));
        assertThatThrownBy(() -> HadoopConfigurationFactory.create(hdfs, DEV, ENFORCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void aDirectoryWithNeitherSiteFileIsRejected(@TempDir Path tmp) throws IOException {
        Files.createDirectories(tmp.resolve("conf"));
        Files.writeString(tmp.resolve("conf/readme.txt"), "not a site file");
        assertThatThrownBy(() -> HadoopConfigurationFactory.create(props(tmp.resolve("conf")),
                DEV, ENFORCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("neither core-site.xml nor hdfs-site.xml");
    }

    @Test
    void eitherSiteFileAloneIsEnoughToLoad(@TempDir Path tmp) throws IOException {
        // core-site.xml carries fs.defaultFS, but a directory holding only
        // hdfs-site.xml is a legitimate split — rejecting it would be stricter
        // than Hadoop itself.
        Path onlyHdfsSite = tmp.resolve("a");
        writeSite(onlyHdfsSite, "hdfs-site.xml", "dfs.replication", "3");
        Configuration conf = HadoopConfigurationFactory.create(props(onlyHdfsSite), DEV, ENFORCE);
        assertThat(conf.get("dfs.replication")).isEqualTo("3");
    }

    @Test
    void theConfiguredClusterIsLoadedAndItsDefaultFsPreserved(@TempDir Path tmp)
            throws IOException {
        Path conf = tmp.resolve("target-conf");
        writeSite(conf, "core-site.xml", "fs.defaultFS", "hdfs://target-ns");

        IntakeProperties.HdfsProperties hdfs = props(conf);
        hdfs.setExpectedNameservice("target-ns");

        Configuration result = HadoopConfigurationFactory.create(hdfs, PRODUCTION, ENFORCE);
        assertThat(result.get("fs.defaultFS")).isEqualTo("hdfs://target-ns");
    }

    @Test
    void aDefaultFsNamingADifferentClusterIsRejected(@TempDir Path tmp) throws IOException {
        // The whole point: the file loads, the XML is valid, the cluster is
        // real — and it is the wrong one.
        Path conf = tmp.resolve("other-conf");
        writeSite(conf, "core-site.xml", "fs.defaultFS", "hdfs://some-other-cluster");

        IntakeProperties.HdfsProperties hdfs = props(conf);
        hdfs.setExpectedNameservice("target-ns");

        assertThatThrownBy(() -> HadoopConfigurationFactory.create(hdfs, PRODUCTION, ENFORCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("names a DIFFERENT cluster")
                .hasMessageContaining("some-other-cluster")
                .hasMessageContaining("target-ns");
    }

    @Test
    void withNoExpectedNameserviceAnyClusterIsAccepted(@TempDir Path tmp) throws IOException {
        // Documented, warned about, and deliberately permitted: the check can
        // only be as specific as the value the operator supplies.
        Path conf = tmp.resolve("conf");
        writeSite(conf, "core-site.xml", "fs.defaultFS", "hdfs://whatever");
        assertThatCode(() -> HadoopConfigurationFactory.create(props(conf), PRODUCTION, ENFORCE))
                .doesNotThrowAnyException();
    }

    @Test
    void productionWithNoClusterConfigurationAtAllIsRejected() {
        IntakeProperties.HdfsProperties hdfs = new IntakeProperties.HdfsProperties();
        assertThatThrownBy(() -> HadoopConfigurationFactory.create(hdfs, PRODUCTION, ENFORCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("config-resources is empty");
    }

    @Test
    void productionOnLocalDiskIsPermittedOnceAcknowledged() {
        IntakeProperties.HdfsProperties hdfs = new IntakeProperties.HdfsProperties();
        hdfs.setAllowLocalFilesystem(true);
        assertThatCode(() -> HadoopConfigurationFactory.create(hdfs, PRODUCTION, ENFORCE))
                .doesNotThrowAnyException();
    }

    @Test
    void propertyOverridesWinOverTheSiteFiles(@TempDir Path tmp) throws IOException {
        // The reason this exists: a client outside the cluster's network
        // segment needs dfs.client.use.datanode.hostname, and cluster-managed
        // XML is not ours to edit.
        Path conf = tmp.resolve("conf");
        writeSite(conf, "hdfs-site.xml", "dfs.client.use.datanode.hostname", "false");

        IntakeProperties.HdfsProperties hdfs = props(conf);
        hdfs.getProperties().put("dfs.client.use.datanode.hostname", "true");

        Configuration result = HadoopConfigurationFactory.create(hdfs, DEV, ENFORCE);
        assertThat(result.get("dfs.client.use.datanode.hostname")).isEqualTo("true");
    }

    @Test
    void nothingIsInheritedFromTheHostsOwnHadoopConfiguration(@TempDir Path tmp)
            throws IOException {
        // Defaults come from the Hadoop jars packaged with this application.
        // A key that appears in no packaged default and in no configured
        // resource must be absent — if the host's configuration were reaching
        // us, this is where it would show up.
        Path conf = tmp.resolve("conf");
        writeSite(conf, "core-site.xml", "fs.defaultFS", "hdfs://target-ns");

        Configuration result = HadoopConfigurationFactory.create(props(conf), DEV, ENFORCE);
        assertThat(result.get("some.site.specific.key.that.only.a.host.would.define")).isNull();
        assertThat(result.get("fs.defaultFS")).isEqualTo("hdfs://target-ns");
    }

    @Test
    void isolatedModeSkipsDefaultsButKeepsTheHdfsBindings(@TempDir Path tmp) throws IOException {
        Path conf = tmp.resolve("conf");
        writeSite(conf, "core-site.xml", "fs.defaultFS", "hdfs://target-ns");

        IntakeProperties.HdfsProperties hdfs = props(conf);
        hdfs.setIsolateConfiguration(true);

        Configuration result = HadoopConfigurationFactory.create(hdfs, DEV, ENFORCE);
        assertThat(result.get("fs.defaultFS")).isEqualTo("hdfs://target-ns");
        // Skipping core-default.xml also skips these, so they are restored by
        // hand; without them an hdfs:// URI has no implementation to resolve to.
        assertThat(result.get("fs.hdfs.impl"))
                .isEqualTo("org.apache.hadoop.hdfs.DistributedFileSystem");
        assertThat(result.get("fs.AbstractFileSystem.hdfs.impl"))
                .isEqualTo("org.apache.hadoop.fs.Hdfs");
        // And a default that only core-default.xml supplies is genuinely gone,
        // which is the cost this mode carries.
        assertThat(result.get("io.file.buffer.size")).isNull();
    }

    @Test
    void isolatedModeWithoutADefaultFsIsRejected(@TempDir Path tmp) throws IOException {
        Path conf = tmp.resolve("conf");
        writeSite(conf, "core-site.xml", "dfs.replication", "3");

        IntakeProperties.HdfsProperties hdfs = props(conf);
        hdfs.setIsolateConfiguration(true);

        assertThatThrownBy(() -> HadoopConfigurationFactory.create(hdfs, DEV, ENFORCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fs.defaultFS is not set");
    }

    @Test
    void reportOnlyModeReturnsAConfigurationInsteadOfThrowing(@TempDir Path tmp) {
        // Preflight's job is to print every problem, not to die on the first.
        IntakeProperties.HdfsProperties hdfs = props(tmp.resolve("absent"));
        hdfs.setExpectedNameservice("target-ns");

        Configuration result = HadoopConfigurationFactory.create(hdfs, PRODUCTION, REPORT_ONLY);
        assertThat(result).isNotNull();
    }

    @Test
    void blankAndNullResourceEntriesAreIgnored(@TempDir Path tmp) throws IOException {
        // An unset ${HDFS_CONFIG_RESOURCES:} binds as a blank entry, not an
        // empty list — treating that as a missing file would fail every
        // developer's boot.
        Path conf = tmp.resolve("conf");
        writeSite(conf, "core-site.xml", "fs.defaultFS", "hdfs://target-ns");

        IntakeProperties.HdfsProperties hdfs = new IntakeProperties.HdfsProperties();
        hdfs.setConfigResources(java.util.Arrays.asList("", "  ", conf.toString()));

        Configuration result = HadoopConfigurationFactory.create(hdfs, DEV, ENFORCE);
        assertThat(result.get("fs.defaultFS")).isEqualTo("hdfs://target-ns");
    }

    @Test
    void aSingleFileMayBeNamedDirectly(@TempDir Path tmp) throws IOException {
        Path conf = tmp.resolve("conf");
        writeSite(conf, "core-site.xml", "fs.defaultFS", "hdfs://target-ns");

        IntakeProperties.HdfsProperties hdfs = new IntakeProperties.HdfsProperties();
        hdfs.setConfigResources(List.of(conf.resolve("core-site.xml").toString()));

        Configuration result = HadoopConfigurationFactory.create(hdfs, DEV, ENFORCE);
        assertThat(result.get("fs.defaultFS")).isEqualTo("hdfs://target-ns");
    }
}
