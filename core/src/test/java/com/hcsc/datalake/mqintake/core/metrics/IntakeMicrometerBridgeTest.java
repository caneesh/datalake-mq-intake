package com.hcsc.datalake.mqintake.core.metrics;

import com.hcsc.datalake.mqintake.core.runtime.IntakeRuntimeManager;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the bridge that makes every {@code mq_intake_*} metric visible.
 *
 * <p>This class had no test at all, which mattered more than its size
 * suggests: it is the only thing standing between a populated
 * {@link BindingMetrics} and a monitoring system. A meter that is never
 * registered, or registered under a different name, does not fail anything —
 * it produces an alert rule that can never fire, which reads exactly like a
 * healthy system. Every alert named in the deployment runbook depends on a
 * name asserted here.
 */
class IntakeMicrometerBridgeTest {

    private SimpleMeterRegistry meterRegistry;
    private MetricsRegistry intakeMetrics;
    private IntakeMicrometerBridge bridge;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        intakeMetrics = new MetricsRegistry();

        IntakeRuntimeManager runtimeManager = mock(IntakeRuntimeManager.class);
        when(runtimeManager.getMetricsRegistry()).thenReturn(intakeMetrics);

        bridge = new IntakeMicrometerBridge(meterRegistry, runtimeManager);
    }

    @Test
    void everyMetricTheRunbookAlertsOnIsPublished() {
        intakeMetrics.forBinding("rms");
        bridge.bindMetrics();

        // Named individually rather than by counting meters: the point of the
        // test is that these exact strings exist, because an alert rule is
        // written against the string and fails silently when it drifts.
        assertThat(publishedNames()).contains(
                "mq_intake_balance_check_failures_total",  // any increment = a page
                "mq_intake_backout_queue_depth",           // poison sitting on the BOQ
                "mq_intake_tracker_sent_total",            // flatline = landing unacked
                "mq_intake_tracker_suppressed_total",
                "mq_intake_tracker_failures_total",
                "mq_intake_messages_consumed_total",
                "mq_intake_healthy",
                "mq_intake_batches_rolled_back_total",
                "mq_intake_identity_misses_total",
                "mq_intake_suspect_count",
                "mq_intake_degraded");
    }

    @Test
    void everyMeterCarriesItsBindingTag() {
        intakeMetrics.forBinding("rms");
        intakeMetrics.forBinding("claims");
        bridge.bindMetrics();

        // DESIGN §14: aggregate dashboards hide a single stalled binding, so
        // nothing per-binding may be published untagged.
        List<Meter> untagged = meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().startsWith("mq_intake_"))
                .filter(m -> !m.getId().getName().equals("mq_intake_kerberos_relogin_failures"))
                .filter(m -> m.getId().getTag("binding") == null)
                .collect(Collectors.toList());

        assertThat(untagged).as("per-binding meters must carry a binding tag").isEmpty();

        Set<String> bindings = meterRegistry.getMeters().stream()
                .map(m -> m.getId().getTag("binding"))
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toSet());
        assertThat(bindings).containsExactlyInAnyOrder("rms", "claims");
    }

    @Test
    void countersTrackTheLiveValueRatherThanASnapshot() {
        BindingMetrics rms = intakeMetrics.forBinding("rms");
        bridge.bindMetrics();

        assertThat(counter("mq_intake_messages_consumed_total", "rms")).isZero();

        // Registered as FunctionCounters over the existing cumulative values,
        // so a scrape after the fact sees the movement. If they were plain
        // Counters incremented separately, the two would drift.
        rms.recordMessagesConsumed(7);
        rms.recordTrackerSent();
        rms.recordTrackerSent();

        assertThat(counter("mq_intake_messages_consumed_total", "rms")).isEqualTo(7.0);
        assertThat(counter("mq_intake_tracker_sent_total", "rms")).isEqualTo(2.0);
    }

    @Test
    void gaugesFollowTheirSource() {
        BindingMetrics rms = intakeMetrics.forBinding("rms");
        bridge.bindMetrics();

        rms.setBackoutQueueDepth(4);
        rms.setSuspectCount(11);
        rms.setHealthy(false);

        assertThat(gauge("mq_intake_backout_queue_depth", "rms")).isEqualTo(4.0);
        assertThat(gauge("mq_intake_suspect_count", "rms")).isEqualTo(11.0);
        assertThat(gauge("mq_intake_healthy", "rms"))
                .as("an unhealthy binding reads 0, which is the alertable value")
                .isEqualTo(0.0);

        rms.setHealthy(true);
        assertThat(gauge("mq_intake_healthy", "rms")).isEqualTo(1.0);
    }

    @Test
    void flushLatencyIsPublishedInSeconds() {
        BindingMetrics rms = intakeMetrics.forBinding("rms");
        bridge.bindMetrics();

        rms.recordFlushLatency(Duration.ofMillis(250));

        assertThat(gauge("mq_intake_flush_latency_seconds", "rms"))
                .as("published in seconds, not the nanos the counter holds")
                .isEqualTo(0.25);
    }

    @Test
    void bindingTwiceDoesNotDoubleRegister() {
        intakeMetrics.forBinding("rms");

        bridge.bindMetrics();
        int afterFirst = meterRegistry.getMeters().size();
        // ApplicationReadyEvent can fire more than once in a multi-context
        // setup; the guard must be explicit rather than relying on Micrometer
        // silently de-duplicating.
        bridge.bindMetrics();

        assertThat(meterRegistry.getMeters()).hasSize(afterFirst);
    }

    @Test
    void aBindingRegisteredAfterBindingIsNotPublished() {
        // Documents the ordering contract rather than a defect: meters bind
        // once at ApplicationReadyEvent, by which point every binding has
        // registered. A binding appearing later would be invisible, so if
        // bindings ever become dynamic this test is the one that fails.
        intakeMetrics.forBinding("rms");
        bridge.bindMetrics();

        intakeMetrics.forBinding("late");

        assertThat(meterRegistry.getMeters().stream()
                .map(m -> m.getId().getTag("binding"))
                .filter(java.util.Objects::nonNull))
                .doesNotContain("late");
    }

    @Test
    void theKerberosGaugeReflectsRealFailuresNotASeparateCounter() {
        // The gauge was published correctly and read a counter nothing wrote.
        // Real relogin failures increment KerberosManager's own counter;
        // MetricsRegistry has a second one whose recordKerberosReloginFailure()
        // has no production caller, so this reported 0.0 for the life of the
        // process however many relogins failed.
        //
        // IntakeRuntimeManager now points the registry's supplier at the live
        // manager. This asserts the VALUE, which the publication test below
        // does not — that is why the defect survived a test asserting the
        // meter existed.
        intakeMetrics.setKerberosReloginFailureSupplier(() -> 7L);
        intakeMetrics.forBinding("rms");
        bridge.bindMetrics();

        assertThat(meterRegistry.find("mq_intake_kerberos_relogin_failures").gauge().value())
                .isEqualTo(7.0);
    }

    @Test
    void kerberosReloginFailuresArePublishedOncePerProcess() {
        intakeMetrics.forBinding("rms");
        bridge.bindMetrics();

        // Process-wide, not per binding — the one metric here with no tag.
        assertThat(publishedNames()).contains("mq_intake_kerberos_relogin_failures");
        assertThat(meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("mq_intake_kerberos_relogin_failures"))
                .count()).isEqualTo(1);
    }

    @Test
    void noBindingsPublishesNothingPerBindingAndDoesNotFail() {
        bridge.bindMetrics();

        assertThat(meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getTag("binding") != null))
                .isEmpty();
    }

    private Set<String> publishedNames() {
        return meterRegistry.getMeters().stream()
                .map(m -> m.getId().getName())
                .collect(Collectors.toSet());
    }

    private double counter(String name, String binding) {
        return meterRegistry.find(name).tag("binding", binding).functionCounter().count();
    }

    private double gauge(String name, String binding) {
        return meterRegistry.find(name).tag("binding", binding).gauge().value();
    }
}
