package com.hcsc.datalake.mqintake.core.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Production mode must refuse a local filesystem.
 *
 * <p>A Spring Boot fat jar started with {@code java -jar} has only itself on
 * the classpath, so Hadoop's {@code core-site.xml}/{@code hdfs-site.xml}
 * lookups find nothing and {@code fs.defaultFS} falls back to
 * {@code file:///}. Every batch then lands on the server's own disk —
 * successfully, with green health and a clean audit trail, and nowhere anyone
 * will look for it. It is the most dangerous configuration mistake available
 * to this service precisely because nothing else complains, so the refusal has
 * to happen at startup.
 */
class LocalFilesystemGateTest {

    /**
     * The properties bean is registered directly rather than bound from
     * property values: {@code withPropertyValues} feeds the Environment, and a
     * hand-registered bean never goes through {@code @ConfigurationProperties}
     * binding, so anything set that way would silently stay at its default.
     */
    private ApplicationContextRunner runner(ProductionMode mode, boolean allowLocal,
                                            String... configResources) {
        IntakeProperties properties = new IntakeProperties();
        properties.getHdfs().setAllowLocalFilesystem(allowLocal);
        // With no config resources Hadoop resolves file:/// — exactly what
        // happens on a server whose cluster configuration was never found.
        properties.getHdfs().setConfigResources(java.util.Arrays.asList(configResources));
        return new ApplicationContextRunner()
                .withBean(IntakeProperties.class, () -> properties)
                .withBean(ProductionMode.class, () -> mode)
                .withUserConfiguration(HdfsConfiguration.class);
    }

    @Test
    void productionModeRefusesToStartOnTheLocalFilesystem() {
        runner(ProductionMode.enabled(), false).run(context -> {
            assertThat(context.getStartupFailure())
                    .as("landing production data on local disk must never start quietly")
                    .isNotNull();
            assertThat(context.getStartupFailure())
                    .hasStackTraceContaining("the LOCAL disk, not HDFS")
                    .hasStackTraceContaining("intake.hdfs.config-resources");
        });
    }

    @Test
    void anExplicitAcknowledgementIsHonoured() {
        // Some environments genuinely want local disk — a smoke test, a
        // laptop. The escape hatch exists, but it has to be asked for.
        runner(ProductionMode.enabled(), true).run(context ->
                assertThat(context.getStartupFailure()).isNull());
    }

    @Test
    void outsideProductionModeLocalFilesystemIsFine() {
        runner(ProductionMode.disabled(), false).run(context ->
                assertThat(context.getStartupFailure()).isNull());
    }

    @Test
    void aConfigResourceThatDoesNotExistFailsLoudly() {
        // Silently ignoring a bad path would land us back on file:/// with the
        // operator believing the cluster was configured.
        runner(ProductionMode.disabled(), true, "/no/such/hadoop/conf")
                .run(context -> assertThat(context.getStartupFailure())
                        .isNotNull()
                        .hasStackTraceContaining("does not exist"));
    }
}
