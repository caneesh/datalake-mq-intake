package com.hcsc.datalake.mqintake.core.metrics;

import com.hcsc.datalake.mqintake.core.runtime.IntakeRuntimeManager;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.function.ToDoubleFunction;

/**
 * Publishes the intake's own metrics through Actuator.
 *
 * <p>{@link BindingMetrics} was fully populated but read by nothing: the
 * counters lived in memory, `MetricsSnapshot` had no caller outside its own
 * tests, and no monitoring system could see any of it. Everything the service
 * knows about its own behaviour — commits, rollbacks, poison routed, reconnects
 * — was invisible in production.
 *
 * <p>This bridges each {@link BindingMetrics} to Micrometer, which Actuator
 * already exposes, rather than introducing a monitoring stack. Meters are
 * registered once, after startup, and read the live counters when scraped —
 * so this adds nothing to the hot path.
 *
 * <p>Counters are registered as {@link FunctionCounter}s over the existing
 * cumulative values rather than as Micrometer {@code Counter}s, so there is one
 * source of truth. Incrementing both would let them drift.
 *
 * <p>All meters carry a {@code binding} tag. DESIGN §14 is explicit that
 * aggregate dashboards hide a single stalled binding, so nothing here is
 * published untagged.
 */
@Component
public class IntakeMicrometerBridge {

    private static final Logger log = LoggerFactory.getLogger(IntakeMicrometerBridge.class);

    private static final String PREFIX = "mq_intake_";

    private final MeterRegistry meterRegistry;
    private final IntakeRuntimeManager runtimeManager;

    public IntakeMicrometerBridge(MeterRegistry meterRegistry,
                                  IntakeRuntimeManager runtimeManager) {
        this.meterRegistry = meterRegistry;
        this.runtimeManager = runtimeManager;
    }

    /**
     * Binds after startup, by which point every binding has registered its
     * metrics — {@code BindingRuntimeFactory.create} calls
     * {@code metricsRegistry.forBinding} while the runtime manager starts,
     * which happens during context refresh, before this event.
     */
    private final java.util.concurrent.atomic.AtomicBoolean bound =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    @EventListener(ApplicationReadyEvent.class)
    public void bindMetrics() {
        if (!bound.compareAndSet(false, true)) {
            // ApplicationReadyEvent can fire more than once in multi-context
            // setups; re-registering relied on Micrometer's dedup behaviour
            // rather than stating the intent.
            return;
        }
        MetricsRegistry registry = runtimeManager.getMetricsRegistry();
        int bound = 0;

        for (BindingMetrics metrics : registry.getAllBindingMetrics()) {
            bindBinding(metrics);
            bound++;
        }

        Gauge.builder(PREFIX + "kerberos_relogin_failures", registry,
                        MetricsRegistry::getKerberosReloginFailures)
                .description("Kerberos relogin failures since startup")
                .register(meterRegistry);

        log.info("Published intake metrics through Actuator for {} binding(s)", bound);
    }

    private void bindBinding(BindingMetrics metrics) {
        Tags tags = Tags.of("binding", metrics.getBindingId());

        // --- Throughput ---
        counter(metrics, tags, "messages_consumed_total", BindingMetrics::getMessagesConsumed,
                "Messages consumed and committed off the source queue");
        counter(metrics, tags, "messages_written_total", BindingMetrics::getMessagesWritten,
                "Records written to HDFS");
        counter(metrics, tags, "bytes_written_total", BindingMetrics::getBytesWritten,
                "Bytes written to HDFS");
        counter(metrics, tags, "batches_committed_total", BindingMetrics::getCommitCount,
                "Batches committed");
        counter(metrics, tags, "batches_rolled_back_total", BindingMetrics::getRollbackCount,
                "Batches rolled back");
        counter(metrics, tags, "flushes_total", BindingMetrics::getFlushCount,
                "Completed HDFS flushes");

        // --- Failures worth alerting on ---
        counter(metrics, tags, "poison_routed_total", BindingMetrics::getPoisonMessagesRouted,
                "Messages routed to the backout queue");
        counter(metrics, tags, "tracker_failures_total", BindingMetrics::getTrackerFailureCount,
                "Tracker notifications that could not be sent");
        counter(metrics, tags, "tracker_sent_total", BindingMetrics::getTrackerSentCount,
                "Tracker notifications put on the tracker queue — the only positive proof "
                        + "tracking is working. Alert when this stops advancing while "
                        + "messages_consumed_total does");
        counter(metrics, tags, "tracker_suppressed_total",
                BindingMetrics::getTrackerSuppressedCount,
                "Messages landed with no tracker notification because the builder suppressed "
                        + "one (RMS: no MessageHeaderDetails). Expected at zero for RMS, so a "
                        + "sustained climb means an upstream regression and unacknowledged data");
        counter(metrics, tags, "audit_failures_total", BindingMetrics::getAuditFailureCount,
                "Audit records that could not be written after a commit");
        counter(metrics, tags, "balance_check_failures_total",
                BindingMetrics::getBalanceCheckFailures,
                "Batches rolled back because consumed != written + backout — any increment "
                        + "means the pipeline dropped a message pre-commit and warrants a page");
        counter(metrics, tags, "identity_misses_total", BindingMetrics::getIdentityMisses,
                "Payloads landed without an extractable identity — each one removes its whole "
                        + "file from identity-based reconciliation, so a sustained climb means "
                        + "an upstream schema regression");
        counter(metrics, tags, "reconnects_total", BindingMetrics::getReconnectSuccessCount,
                "Successful JMS session recoveries");
        counter(metrics, tags, "reconnect_failures_total", BindingMetrics::getReconnectFailureCount,
                "Failed JMS session recovery attempts");
        counter(metrics, tags, "degraded_entries_total", BindingMetrics::getDegradedModeEntries,
                "Times the binding entered degraded mode");
        counter(metrics, tags, "reconciliation_discrepancies_total",
                BindingMetrics::getReconciliationDiscrepancyCount,
                "Reconciliation discrepancies detected");

        // --- Current state ---
        gauge(metrics, tags, "backout_queue_depth", BindingMetrics::getBackoutQueueDepth,
                "Messages currently on the backout queue (non-zero warrants a page)");
        gauge(metrics, tags, "current_batch_size", BindingMetrics::getCurrentBatchSize,
                "Messages in the in-flight batch");
        gauge(metrics, tags, "degraded", m -> m.isInDegradedMode() ? 1 : 0,
                "1 while the binding is in degraded (reduced batch size) mode");
        gauge(metrics, tags, "suspect_count", BindingMetrics::getSuspectCount,
                "Unresolved suspect message IDs — non-zero for long means the binding is "
                        + "pinned at reduced batch size and needs investigation");
        gauge(metrics, tags, "healthy", m -> m.isHealthy() ? 1 : 0,
                "1 while the last batch outcome was a success");
        gauge(metrics, tags, "flush_latency_seconds",
                m -> m.getLastFlushLatency().toNanos() / 1_000_000_000.0,
                "Duration of the most recent HDFS flush");
        gauge(metrics, tags, "flush_latency_seconds_avg",
                m -> m.getAverageFlushLatency().toNanos() / 1_000_000_000.0,
                "Mean HDFS flush duration since startup");

        // Source and tracker queue depth are deliberately absent. Unlike the
        // backout queue — empty when healthy, so cheap to browse — those are
        // deep by design, and browsing them to produce a number would cost
        // more than the number is worth. They need an MQ admin query (PCF or
        // equivalent), which is a separate piece of work.
    }

    private void counter(BindingMetrics metrics, Tags tags, String name,
                         ToDoubleFunction<BindingMetrics> value, String description) {
        FunctionCounter.builder(PREFIX + name, metrics, value)
                .tags(tags)
                .description(description)
                .register(meterRegistry);
    }

    private void gauge(BindingMetrics metrics, Tags tags, String name,
                       ToDoubleFunction<BindingMetrics> value, String description) {
        Gauge.builder(PREFIX + name, metrics, value)
                .tags(tags)
                .description(description)
                .register(meterRegistry);
    }
}
