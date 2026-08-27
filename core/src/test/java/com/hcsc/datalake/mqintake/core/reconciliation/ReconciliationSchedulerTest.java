package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Scheduling reconciliation — the check half of ABC.
 *
 * <p>The behaviour that matters most here is negative: reconciliation must
 * never be able to interfere with ingestion. A checking mechanism that can
 * halt the thing it checks is worse than no check.
 */
class ReconciliationSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-08-26T10:47:00Z");
    private static final Clock FIXED = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void nothingIsScheduledWhenReconciliationIsDisabled() {
        IntakeProperties properties = properties(false);
        RecordingService service = new RecordingService();

        try (ReconciliationScheduler scheduler = scheduler(service, properties)) {
            scheduler.start();

            assertThat(scheduler.isScheduled()).isFalse();
            assertThat(service.calls.get()).isZero();
        }
    }

    @Test
    void schedulingStartsWhenEnabled() {
        IntakeProperties properties = properties(true);
        RecordingService service = new RecordingService();

        try (ReconciliationScheduler scheduler = scheduler(service, properties)) {
            scheduler.start();

            assertThat(scheduler.isScheduled()).isTrue();
        }
    }

    @Test
    void aRunReconcilesEveryBindingOverTheLookbackWindows() {
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(3);
        RecordingService service = new RecordingService();
        ReconciliationScheduler scheduler = scheduler(service, properties);

        for (BindingConfig binding : properties.getBindings()) {
            scheduler.runBindingQuietly(binding);
        }

        // 2 bindings x 3 windows
        assertThat(service.calls.get()).isEqualTo(6);
        assertThat(service.bindingIds).containsExactlyInAnyOrder(
                "rms", "rms", "rms", "claims", "claims", "claims");
    }

    @Test
    void overlappingRunsForTheSameBindingAreSkipped() throws Exception {
        // A pass slower than the interval would otherwise start again beneath
        // itself, doubling HDFS load exactly when it is already slow.
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(1);

        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BlockingService service = new BlockingService(inside, release);
        ReconciliationScheduler scheduler = scheduler(service, properties);

        BindingConfig rms = properties.getBindings().get(0);

        Thread first = new Thread(() -> scheduler.runBindingQuietly(rms));
        first.start();
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();

        // Second run while the first is still inside
        scheduler.runBindingQuietly(rms);
        assertThat(service.calls.get())
                .as("the overlapping run must be skipped, not queued")
                .isEqualTo(1);

        release.countDown();
        first.join(5000);

        // Once the first finishes, a later run proceeds normally
        scheduler.runBindingQuietly(rms);
        assertThat(service.calls.get()).isEqualTo(2);
    }

    @Test
    void aDifferentBindingIsNotBlockedByAnotherStillRunning() throws Exception {
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(1);

        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BlockingService service = new BlockingService(inside, release);
        ReconciliationScheduler scheduler = scheduler(service, properties);

        Thread first = new Thread(() -> scheduler.runBindingQuietly(properties.getBindings().get(0)));
        first.start();
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();

        release.countDown();
        scheduler.runBindingQuietly(properties.getBindings().get(1));
        first.join(5000);

        assertThat(service.calls.get()).isEqualTo(2);
    }

    @Test
    void aReconciliationFailureIsContainedAndCounted() {
        // The critical property: reconciliation is a check on ingestion, not a
        // participant. A failure here must not propagate anywhere near it.
        IntakeProperties properties = properties(true);
        BindingMetrics metrics = new BindingMetrics("rms");
        ReconciliationScheduler scheduler = new ReconciliationScheduler(
                new ThrowingService(), properties, id -> metrics, FIXED);

        assertThatCode(() -> scheduler.runBindingQuietly(properties.getBindings().get(0)))
                .as("a reconciliation failure must never escape towards ingestion")
                .doesNotThrowAnyException();

        assertThat(metrics.getReconciliationDiscrepancyCount()).isPositive();
    }

    @Test
    void aFailureLeavesTheBindingRunnableAgain() {
        // The in-progress flag must be released on the failure path too, or one
        // exception disables reconciliation for that binding permanently.
        IntakeProperties properties = properties(true);
        ThrowingService service = new ThrowingService();
        ReconciliationScheduler scheduler =
                new ReconciliationScheduler(service, properties, id -> null, FIXED);
        BindingConfig rms = properties.getBindings().get(0);

        scheduler.runBindingQuietly(rms);
        scheduler.runBindingQuietly(rms);

        assertThat(service.calls.get()).isEqualTo(2);
    }

    @Test
    void identityApprovalFollowsWhetherTheBindingWritesAnIndex() {
        // Reconciliation can only compare identities where they are recorded.
        // RMS writes a sidecar index; Claims does not, and must be told so
        // rather than left to compare against nothing.
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(1);
        RecordingService service = new RecordingService();
        ReconciliationScheduler scheduler = scheduler(service, properties);

        for (BindingConfig binding : properties.getBindings()) {
            scheduler.runBindingQuietly(binding);
        }

        assertThat(service.identityApprovedByBinding).containsEntry("rms", true);
        assertThat(service.identityApprovedByBinding).containsEntry("claims", false);
    }

    @Test
    void lookbackWindowsStartOneWindowBackAndStepByAQuarterHour() {
        ReconciliationScheduler scheduler =
                scheduler(new RecordingService(), properties(true));

        List<Instant> windows = scheduler.recentWindows(3);

        // The current window is still being written to, so it is not examined
        assertThat(windows).containsExactly(
                NOW.minus(Duration.ofMinutes(15)),
                NOW.minus(Duration.ofMinutes(30)),
                NOW.minus(Duration.ofMinutes(45)));
    }

    @Test
    void closeIsSafeBeforeStartAndTwice() {
        ReconciliationScheduler scheduler =
                scheduler(new RecordingService(), properties(true));

        assertThatCode(() -> {
            scheduler.close();
            scheduler.start();
            scheduler.close();
            scheduler.close();
        }).doesNotThrowAnyException();

        assertThat(scheduler.isScheduled()).isFalse();
    }

    // --- harness ---

    private ReconciliationScheduler scheduler(PartitionReconciler service,
                                              IntakeProperties properties) {
        return new ReconciliationScheduler(service, properties, id -> null, FIXED);
    }

    private IntakeProperties properties(boolean enabled) {
        IntakeProperties properties = new IntakeProperties();
        properties.getReconciliation().setEnabled(enabled);
        properties.getReconciliation().setIntervalMs(50);
        properties.getReconciliation().setLookbackWindows(2);
        properties.setBindings(new ArrayList<>(List.of(
                binding("rms", true), binding("claims", false))));
        return properties;
    }

    private BindingConfig binding(String id, boolean recordIndexEnabled) {
        BindingConfig config = new BindingConfig();
        config.setId(id);
        config.setSourceQueue("Q." + id);
        config.setMode(BindingMode.LAND_ONLY);
        config.getHdfs().setBasePath("/data/" + id);
        config.getBatch().setSize(10);
        config.getBatch().setBytes(1024);
        config.getBatch().setIntervalMs(0);
        config.setListenerThreads(1);
        config.getHdfs().setRecordIndexEnabled(recordIndexEnabled);
        return config;
    }

    /** Records what it was asked to reconcile. */
    private static class RecordingService extends StubService {
        final List<String> bindingIds = java.util.Collections.synchronizedList(new ArrayList<>());
        final java.util.Map<String, Boolean> identityApprovedByBinding =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public PartitionReconciliationService.ReconciliationReport reconcilePartition(String bindingId, String basePath,
                Instant partitionInstant, boolean identityApproved,
                boolean quarantineDuplicates, BindingMetrics metrics) {
            calls.incrementAndGet();
            bindingIds.add(bindingId);
            identityApprovedByBinding.put(bindingId, identityApproved);
            return PartitionReconciliationService.ReconciliationReport.notReady(
                bindingId, basePath, "stub");
        }
    }

    /** Blocks inside a run so overlap can be observed. */
    private static class BlockingService extends StubService {
        private final CountDownLatch inside;
        private final CountDownLatch release;

        BlockingService(CountDownLatch inside, CountDownLatch release) {
            this.inside = inside;
            this.release = release;
        }

        @Override
        public PartitionReconciliationService.ReconciliationReport reconcilePartition(String bindingId, String basePath,
                Instant partitionInstant, boolean identityApproved,
                boolean quarantineDuplicates, BindingMetrics metrics) {
            calls.incrementAndGet();
            inside.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return PartitionReconciliationService.ReconciliationReport.notReady(
                bindingId, basePath, "stub");
        }
    }

    private static class ThrowingService extends StubService {
        @Override
        public PartitionReconciliationService.ReconciliationReport reconcilePartition(String bindingId, String basePath,
                Instant partitionInstant, boolean identityApproved,
                boolean quarantineDuplicates, BindingMetrics metrics) {
            calls.incrementAndGet();
            throw new IllegalStateException("HDFS unavailable");
        }
    }

    private abstract static class StubService implements PartitionReconciler {
        final AtomicInteger calls = new AtomicInteger();
    }
}
