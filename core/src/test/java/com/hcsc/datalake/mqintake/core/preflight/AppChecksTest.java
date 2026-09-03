package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.Message;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests every branch of the checks an operator runs before starting.
 *
 * <p>Preflight had no test of any kind, which is the wrong place for a gap:
 * the deployment documentation designates it as the substitute for MQ admin
 * access, so its report is what someone reads instead of asking for a
 * {@code DISPLAY QLOCAL}. A check that silently passes is worse than no check,
 * because it is read as evidence.
 *
 * <p>{@code AppChecks} is the part that needs no live dependency — it inspects
 * configuration, builds the serializer and tracker, and reports the posture
 * gates. {@code MqChecks} and {@code HdfsChecks} open real resources and are
 * covered by their own suites and by the Docker-backed preflight run.
 */
class AppChecksTest {

    @TempDir
    Path tempDir;

    // --- production-mode gate ---

    @Test
    void productionModeReportsTheGatesAsArmed() {
        CheckOutcome outcome = run("app", "production-mode", propsWith(landOnly()),
                ProductionMode.enabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
        assertThat(outcome.getDetail()).contains("ARMED");
    }

    @Test
    void nonProductionSaysTheGatesAreAdvisory() {
        // The dangerous misreading is a green preflight in a dev posture taken
        // as evidence the gates would hold in production, so the report says
        // which posture produced it.
        CheckOutcome outcome = run("app", "production-mode", propsWith(landOnly()),
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
        assertThat(outcome.getDetail()).contains("not armed");
    }

    // --- kerberos identity ---

    @Test
    void kerberosDisabledIsSkippedNotPassed() {
        // A skip is not a pass: the report must not imply an identity was
        // proven when none was configured.
        IntakeProperties props = propsWith(landOnly());
        props.getKerberos().setEnabled(false);

        CheckOutcome outcome = run("app", "kerberos", props,
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.SKIP);
    }

    @Test
    void kerberosEnabledWithNoPrincipalFails() {
        IntakeProperties props = propsWith(landOnly());
        props.getKerberos().setEnabled(true);
        props.getKerberos().setKeytabPath("/some/keytab");

        CheckOutcome outcome = run("app", "kerberos", props,
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(outcome.getDetail()).contains("principal");
    }

    @Test
    void kerberosEnabledWithNoKeytabFails() {
        IntakeProperties props = propsWith(landOnly());
        props.getKerberos().setEnabled(true);
        props.getKerberos().setPrincipal("svc-intake@REALM");

        CheckOutcome outcome = run("app", "kerberos", props,
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(outcome.getDetail()).contains("keytab");
    }

    @Test
    void aMissingKeytabFileIsNamedInFull() {
        // The likeliest first-deployment mistake, per the commit that added
        // this check. The path has to appear so it can be compared by eye.
        IntakeProperties props = propsWith(landOnly());
        props.getKerberos().setEnabled(true);
        props.getKerberos().setPrincipal("svc-intake@REALM");
        props.getKerberos().setKeytabPath(tempDir.resolve("absent.keytab").toString());

        CheckOutcome outcome = run("app", "kerberos", props,
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(outcome.getDetail())
                .contains("does not exist")
                .contains("absent.keytab");
    }

    @Test
    void aKeytabPresentButLoginFailedIsDistinctFromAMissingOne() throws Exception {
        // Different remedies: a missing file is a path problem, a failed login
        // with the file present is a principal spelling or KDC problem.
        Path keytab = Files.createFile(tempDir.resolve("present.keytab"));
        IntakeProperties props = propsWith(landOnly());
        props.getKerberos().setEnabled(true);
        props.getKerberos().setPrincipal("svc-intake@REALM");
        props.getKerberos().setKeytabPath(keytab.toString());

        CheckOutcome outcome = run("app", "kerberos", props,
                ProductionMode.disabled(), goodSerializer(), null, false);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(outcome.getDetail()).contains("login failed");
    }

    @Test
    void aKeytabPresentAndLoggedInPasses() throws Exception {
        Path keytab = Files.createFile(tempDir.resolve("good.keytab"));
        IntakeProperties props = propsWith(landOnly());
        props.getKerberos().setEnabled(true);
        props.getKerberos().setPrincipal("svc-intake@REALM");
        props.getKerberos().setKeytabPath(keytab.toString());

        CheckOutcome outcome = run("app", "kerberos", props,
                ProductionMode.disabled(), goodSerializer(), null, true);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
        assertThat(outcome.getDetail()).contains("svc-intake@REALM");
    }

    // --- serializer ---

    @Test
    void aBuildableSerializerReportsTheLayoutItWrites() {
        // The layout is in the report because a serializer declaring the wrong
        // Writable types produces files a production consumer cannot open.
        CheckOutcome outcome = run("app", "land-only.serializer", propsWith(landOnly()),
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
        assertThat(outcome.getDetail()).contains("LongWritable").contains("Text");
    }

    @Test
    void aPlaceholderSerializerFailsProductionUnlessAccepted() {
        BindingConfig binding = landOnly();
        CheckOutcome outcome = run("app", "land-only.serializer", propsWith(binding),
                ProductionMode.enabled(), placeholderSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(outcome.getDetail()).contains("PLACEHOLDER");
    }

    @Test
    void aPlaceholderSerializerPassesProductionWhenExplicitlyAccepted() {
        // Claims ships this way; the acceptance is per binding and recorded in
        // the report rather than being silent.
        BindingConfig binding = landOnly();
        binding.setAcceptPlaceholderSerializer(true);

        CheckOutcome outcome = run("app", "land-only.serializer", propsWith(binding),
                ProductionMode.enabled(), placeholderSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
        assertThat(outcome.getDetail()).contains("ACCEPTED");
    }

    @Test
    void aPlaceholderSerializerOutsideProductionIsSkipped() {
        CheckOutcome outcome = run("app", "land-only.serializer", propsWith(landOnly()),
                ProductionMode.disabled(), placeholderSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.SKIP);
    }

    @Test
    void aSerializerThatCannotBeBuiltFailsRatherThanThrowing() {
        // Preflight must always produce a report. A factory that throws is a
        // finding, not a crash — that distinction is why preflight exists.
        RecordSerializerFactory broken = config -> {
            throw new IllegalStateException("identity field not configured");
        };

        CheckOutcome outcome = run("app", "land-only.serializer", propsWith(landOnly()),
                ProductionMode.disabled(), broken, null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(outcome.getDetail()).contains("could not be built");
    }

    // --- tracker ---

    @Test
    void aLandOnlyBindingSkipsTheTrackerCheck() {
        CheckOutcome outcome = run("app", "land-only.tracker-builder", propsWith(landOnly()),
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.SKIP);
        assertThat(outcome.getDetail()).contains("LAND_ONLY");
    }

    @Test
    void aTrackedBindingWithNoTrackerFactoryFails() {
        // The RMS misconfiguration that would otherwise surface as every
        // listener thread dying at session initialisation.
        CheckOutcome outcome = run("app", "tracked.tracker-builder", propsWith(tracked()),
                ProductionMode.disabled(), goodSerializer(), null);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(outcome.getDetail()).contains("TrackerMessageBuilderFactory");
    }

    @Test
    void aTrackedBindingWithATrackerFactoryReportsTheBuilder() {
        TrackerMessageBuilderFactory factory = config -> (session, source) -> Optional.empty();

        CheckOutcome outcome = run("app", "tracked.tracker-builder", propsWith(tracked()),
                ProductionMode.disabled(), goodSerializer(), factory);

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
    }

    // --- control summary ---

    @Test
    void theControlSummaryPrintsTheLiveBindingModeAndControls() {
        // RMS running LAND_ONLY sends no tracker messages at all, and the
        // deployment checklist gates cutover on this line reading TRACKED.
        BindingConfig binding = tracked();
        binding.getAudit().setBalanceCheckEnabled(true);
        binding.getBackout().setRouteOnlyOnDataFailures(true);

        CheckOutcome outcome = run("app", "tracked.controls", propsWith(binding),
                ProductionMode.disabled(), goodSerializer(),
                config -> (session, source) -> Optional.empty());

        assertThat(outcome.getDetail()).contains("TRACKED");
    }

    @Test
    void everyBindingContributesItsOwnChecks() {
        IntakeProperties props = new IntakeProperties();
        props.setBindings(List.of(landOnly(), tracked()));

        List<PreflightCheck> checks = AppChecks.forAllBindings(props,
                ProductionMode.disabled(), goodSerializer(),
                config -> (session, source) -> Optional.empty());

        // Two process-wide checks, then three per binding.
        assertThat(checks).hasSize(2 + 3 * 2);
        assertThat(checks.stream().map(PreflightCheck::group).distinct())
                .containsExactly("app");
    }

    // --- helpers ---

    private CheckOutcome run(String group, String name, IntakeProperties props,
                             ProductionMode mode, RecordSerializerFactory serializers,
                             TrackerMessageBuilderFactory trackers) {
        return run(group, name, props, mode, serializers, trackers, true);
    }

    private CheckOutcome run(String group, String name, IntakeProperties props,
                             ProductionMode mode, RecordSerializerFactory serializers,
                             TrackerMessageBuilderFactory trackers, boolean kerberosLoggedIn) {
        PreflightCheck check = AppChecks
                .forAllBindings(props, mode, serializers, trackers, kerberosLoggedIn)
                .stream()
                .filter(c -> c.group().equals(group) && c.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no check named " + group + "." + name));
        return check.run();
    }

    private IntakeProperties propsWith(BindingConfig binding) {
        IntakeProperties props = new IntakeProperties();
        props.setBindings(List.of(binding));
        return props;
    }

    private BindingConfig landOnly() {
        BindingConfig config = new BindingConfig();
        config.setId("land-only");
        config.setMode(BindingMode.LAND_ONLY);
        config.setSourceQueue("SRC.IN");
        config.getHdfs().setBasePath("/data/raw/land-only");
        return config;
    }

    private BindingConfig tracked() {
        BindingConfig config = new BindingConfig();
        config.setId("tracked");
        config.setMode(BindingMode.TRACKED);
        config.setSourceQueue("SRC.IN");
        config.getTracker().setQueue("SRC.TRACKER");
        config.getHdfs().setBasePath("/data/raw/tracked");
        return config;
    }

    private RecordSerializerFactory goodSerializer() {
        return config -> new ProductionLayoutSerializer();
    }

    private RecordSerializerFactory placeholderSerializer() {
        return config -> new PlaceholderLayoutSerializer();
    }

    /** Declares the production Writable types, as RMS and Claims both do. */
    private static class ProductionLayoutSerializer implements RecordSerializer {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata) {
            return new SerializedRecord(new LongWritable(0), new Text(""));
        }

        @Override
        public Class<? extends Writable> getKeyClass() {
            return LongWritable.class;
        }

        @Override
        public Class<? extends Writable> getValueClass() {
            return Text.class;
        }
    }

    private static class PlaceholderLayoutSerializer extends ProductionLayoutSerializer
            implements PlaceholderSerializer {
        @Override
        public String getPlaceholderReason() {
            return "identity field not confirmed";
        }
    }
}
