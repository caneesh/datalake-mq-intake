package com.hcsc.datalake.mqintake.core.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The migration guard for the 2026-08 binding-key grouping.
 *
 * <p>The failure mode this guards against is silence: Spring ignores keys
 * that no longer bind, so an environment-specific override written against
 * the old flat names would simply stop applying. The detector must catch
 * every legacy form loudly, and must NOT complain about the new grouped keys
 * or about env-var forms that still bind under relaxed mapping.
 */
class LegacyBindingKeyDetectorTest {

    private StandardEnvironment environmentWith(Map<String, Object> properties) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(
                new MapPropertySource("test-overrides", properties));
        return environment;
    }

    @Test
    void legacyDottedKeyFailsAndNamesTheReplacement() {
        var env = environmentWith(Map.of("intake.bindings[0].batch-size", "4000"));

        assertThatThrownBy(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("intake.bindings[0].batch-size")
                .hasMessageContaining("intake.bindings[0].batch.size")
                .hasMessageContaining("test-overrides");
    }

    @Test
    void everyLegacyLeafIsCaughtWithItsGroupedReplacement() {
        Map<String, String> expected = Map.of(
                "intake.bindings[2].backout-queue", "backout.queue",
                "intake.bindings[2].hdfs-base-path", "hdfs.base-path",
                "intake.bindings[2].record-index-enabled", "hdfs.record-index-enabled",
                "intake.bindings[2].fail-batch-on-audit-error", "audit.fail-batch-on-error",
                "intake.bindings[2].fail-batch-on-tracker-error", "tracker.fail-batch-on-error",
                "intake.bindings[2].degradation-strategy", "degradation.strategy",
                "intake.bindings[2].successes-required-to-restore",
                "degradation.successes-required-to-restore");

        for (Map.Entry<String, String> entry : expected.entrySet()) {
            var env = environmentWith(Map.of(entry.getKey(), "x"));
            assertThatThrownBy(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                    .as(entry.getKey())
                    .hasMessageContaining(entry.getKey())
                    .hasMessageContaining("intake.bindings[2]." + entry.getValue());
        }
    }

    @Test
    void camelCaseLegacyFormIsCaughtToo() {
        var env = environmentWith(Map.of("intake.bindings[0].trackerQueue", "Q"));

        assertThatThrownBy(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .hasMessageContaining("trackerQueue")
                .hasMessageContaining("tracker.queue");
    }

    @Test
    void legacyTrackerFieldsSubKeyIsCaught() {
        var env = environmentWith(
                Map.of("intake.bindings[0].tracker-fields.source-system", "IIB"));

        assertThatThrownBy(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .hasMessageContaining("tracker-fields.source-system")
                .hasMessageContaining("tracker.fields");
    }

    @Test
    void brokenEnvVarFormsFailNamingTheNewVariable() {
        var env = environmentWith(Map.of(
                "INTAKE_BINDINGS_0_RECORD_INDEX_ENABLED", "true",
                "INTAKE_BINDINGS_1_SUCCESSES_REQUIRED_TO_RESTORE", "5"));

        assertThatThrownBy(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .hasMessageContaining("INTAKE_BINDINGS_0_HDFS_RECORD_INDEX_ENABLED")
                .hasMessageContaining("INTAKE_BINDINGS_1_DEGRADATION_SUCCESSES_REQUIRED_TO_RESTORE");
    }

    @Test
    void newGroupedKeysPass() {
        var env = environmentWith(Map.of(
                "intake.bindings[0].batch.size", "4000",
                "intake.bindings[0].batch.interval-ms", "0",
                "intake.bindings[0].hdfs.base-path", "/data/raw/x",
                "intake.bindings[0].hdfs.record-index-enabled", "true",
                "intake.bindings[0].tracker.queue", "Q",
                "intake.bindings[0].tracker.fail-batch-on-error", "false",
                "intake.bindings[0].backout.queue", "BOQ",
                "intake.bindings[0].audit.fail-batch-on-error", "true",
                "intake.bindings[0].degradation.strategy", "BISECT"));

        assertThatCode(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .doesNotThrowAnyException();
    }

    @Test
    void stillBindingEnvVarFormsPass() {
        // Underscores map to both '-' and '.', so these old-style variables
        // still bind to the new grouped keys and must NOT be rejected —
        // rejecting them would break working deployments for no reason.
        var env = environmentWith(Map.of(
                "INTAKE_BINDINGS_0_BATCH_SIZE", "4000",
                "INTAKE_BINDINGS_0_BACKOUT_QUEUE", "BOQ",
                "INTAKE_BINDINGS_0_TRACKER_QUEUE", "Q",
                "INTAKE_BINDINGS_0_HDFS_BASE_PATH", "/data/raw/x",
                "INTAKE_BINDINGS_0_DEGRADATION_STRATEGY", "BISECT"));

        assertThatCode(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .doesNotThrowAnyException();
    }

    @Test
    void unrelatedKeysPass() {
        var env = environmentWith(Map.of(
                "intake.bindings[0].id", "rms",
                "intake.bindings[0].source-queue", "IN",
                "intake.bindings[0].mq-connection", "primary",
                "intake.bindings[0].listener-threads", "4",
                "intake.mq-connections.primary.host", "localhost",
                "some.other.batch-size", "1"));

        assertThatCode(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .doesNotThrowAnyException();
    }

    @Test
    void aSpringContextWithALegacyKeyRefusesToStart() {
        // Proves the wiring, not just the detector: IntakeConfiguration must
        // actually run the scan during context refresh.
        new ApplicationContextRunner()
                .withUserConfiguration(IntakeConfiguration.class)
                .withPropertyValues("intake.bindings[0].batch-size=5")
                .run(context -> assertThat(context.getStartupFailure())
                        .isNotNull()
                        .hasStackTraceContaining("intake.bindings[0].batch.size"));
    }

    @Test
    void aSpringContextWithGroupedKeysStarts() {
        new ApplicationContextRunner()
                .withUserConfiguration(IntakeConfiguration.class)
                .withPropertyValues(
                        "intake.bindings[0].id=rms",
                        "intake.bindings[0].batch.size=5",
                        "intake.bindings[0].backout.queue=BOQ")
                .run(context -> assertThat(context.getStartupFailure()).isNull());
    }

    @Test
    void reportsAllOffendersAtOnceNotJustTheFirst() {
        var env = environmentWith(Map.of(
                "intake.bindings[0].batch-size", "4000",
                "intake.bindings[0].backout-queue", "BOQ",
                "intake.bindings[1].hsync-on-flush", "true"));

        assertThatThrownBy(() -> LegacyBindingKeyDetector.failOnLegacyKeys(env))
                .hasMessageContaining("batch-size")
                .hasMessageContaining("backout-queue")
                .hasMessageContaining("hsync-on-flush");
    }
}
