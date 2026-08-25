package com.hcsc.datalake.mqintake.core.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Instance-id resolution.
 *
 * <p>The id names this process's _tmp directory and appears in every landed
 * filename, so a collision between two JVMs on one host lets one sweep the
 * other's in-flight temp files and lets both emit the same output filename.
 */
class InstanceIdTest {

    @Test
    void generatedIdCombinesHostnameAndPid() {
        // HOSTNAME alone is unique per host, not per process
        InstanceId id = new InstanceId(null, "datanode07", 4242L);

        assertThat(id.value()).isEqualTo("datanode07-4242");
        assertThat(id.isGenerated()).isTrue();
    }

    @Test
    void twoProcessesOnTheSameHostGetDistinctIds() {
        InstanceId first = new InstanceId(null, "datanode07", 100L);
        InstanceId second = new InstanceId(null, "datanode07", 101L);

        assertThat(first.value()).isNotEqualTo(second.value());
    }

    @Test
    void explicitConfigurationOverridesTheGeneratedDefault() {
        InstanceId id = new InstanceId("rms-blue-1", "datanode07", 4242L);

        assertThat(id.value()).isEqualTo("rms-blue-1");
        assertThat(id.isGenerated()).isFalse();
    }

    @Test
    void blankConfigurationFallsBackToGeneration() {
        // application.yml supplies an empty default, which must mean "derive"
        assertThat(new InstanceId("", "host", 7L).value()).isEqualTo("host-7");
        assertThat(new InstanceId("   ", "host", 7L).value()).isEqualTo("host-7");
        assertThat(new InstanceId("   ", "host", 7L).isGenerated()).isTrue();
    }

    @Test
    void configuredValueIsTrimmed() {
        assertThat(new InstanceId("  rms-1  ", "host", 1L).value()).isEqualTo("rms-1");
    }

    @Test
    void charactersUnsafeInAPathAreReplaced() {
        // The id becomes an HDFS path component; a slash would silently create
        // a nested directory and break the "sweep only my own subtree" rule.
        assertThat(new InstanceId("rms/blue 1", "host", 1L).value()).isEqualTo("rms-blue-1");
        assertThat(new InstanceId(null, "host.name", 5L).value()).isEqualTo("host.name-5");
    }

    @Test
    void theValueIsStableForTheLifeOfTheProcess() {
        // Must not be re-derived per call: a changing id would orphan the _tmp
        // subtree the process had already been writing to.
        InstanceId id = new InstanceId(null, "host", 9L);

        String first = id.value();
        for (int i = 0; i < 100; i++) {
            assertThat(id.value()).isEqualTo(first);
        }
    }

    @Test
    void anIdThatIdentifiesNothingIsRejected() {
        // Sanitising replaces unsafe characters rather than dropping them, so
        // "///" arrives as "---": not blank, but no more useful as an identity
        // than an empty string would be.
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> new InstanceId("///", "host", 1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no alphanumeric characters");

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> new InstanceId("   -  ", "host", 1L))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void theFactoryProducesAnExplicitlyConfiguredId() {
        InstanceId id = InstanceId.of("explicit");

        assertThat(id.value()).isEqualTo("explicit");
        assertThat(id.isGenerated()).isFalse();
    }

    @Test
    void nullPropertiesDoesNotBlowUp() {
        assertThat(new InstanceId((IntakeProperties) null).value()).isNotBlank();
    }
}
