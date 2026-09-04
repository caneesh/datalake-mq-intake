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
    void aBlockedBindingDoesNotHoldUpAnother() throws Exception {
        // Rewritten. The previous version created its own Thread and released
        // the first binding BEFORE invoking the second, so it proved only that
        // both were called — it could not have failed while a single scheduler
        // thread ran every binding in turn.
        //
        // This one goes through start(), so the production dispatch decides
        // the outcome, and it never releases the first binding: the second has
        // to complete while the first is still blocked.
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(1);

        CountDownLatch rmsInside = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        CountDownLatch claimsFinished = new CountDownLatch(1);

        PartitionReconciler service = (bindingId, basePath, instant, identityApproved,
                                       quarantine, metrics) -> {
            if ("rms".equals(bindingId)) {
                rmsInside.countDown();
                try {
                    neverReleased.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                claimsFinished.countDown();
            }
            return PartitionReconciliationService.ReconciliationReport
                    .notReady(bindingId, "partition", "stub");
        };

        ReconciliationScheduler scheduler = scheduler(service, properties);
        try {
            scheduler.start();

            assertThat(rmsInside.await(5, TimeUnit.SECONDS))
                    .as("the first binding reached the blocking call").isTrue();
            assertThat(claimsFinished.await(5, TimeUnit.SECONDS))
                    .as("the second binding must run while the first is still blocked")
                    .isTrue();
        } finally {
            neverReleased.countDown();
            scheduler.close();
        }
    }

    @Test
    void aPartitionThatAgedOutOfTheLookbackIsStillReconciled() throws Exception {
        // The defect: work was rebuilt from the last N closed windows only, so
        // an unresolved partition left the schedule once it aged past them —
        // its discrepancy intact, nothing looking at it again. isRetryLater()
        // was set for exactly this and had no consumer.
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(1);
        properties.setBindings(new ArrayList<>(List.of(binding("rms", true))));

        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("pending-it");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "file:///");
        try (org.apache.hadoop.fs.FileSystem fs =
                     org.apache.hadoop.fs.FileSystem.getLocal(conf)) {

            PendingPartitions pending =
                    new PendingPartitions(fs, dir.resolve("audit").toString());
            Instant stale = FIXED.instant().minus(java.time.Duration.ofHours(3));
            pending.retain("rms", stale);

            java.util.Set<Instant> examined =
                    java.util.concurrent.ConcurrentHashMap.newKeySet();
            PartitionReconciler service = (bindingId, basePath, instant, identityApproved,
                                           quarantine, metrics) -> {
                examined.add(instant);
                return PartitionReconciliationService.ReconciliationReport
                        .notReady(bindingId, "partition", "stub");
            };

            ReconciliationScheduler scheduler = new ReconciliationScheduler(
                    service, properties, id -> null, FIXED, pending);
            scheduler.runBindingQuietly(properties.getBindings().get(0));

            assertThat(examined)
                    .as("three hours old, far outside a single lookback window")
                    .contains(stale);
        }
    }

    @Test
    void aResolvedPartitionLeavesTheBacklog() throws Exception {
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(1);
        properties.setBindings(new ArrayList<>(List.of(binding("rms", true))));

        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("pending-it");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "file:///");
        try (org.apache.hadoop.fs.FileSystem fs =
                     org.apache.hadoop.fs.FileSystem.getLocal(conf)) {

            PendingPartitions pending =
                    new PendingPartitions(fs, dir.resolve("audit").toString());
            Instant stale = FIXED.instant().minus(java.time.Duration.ofHours(3));
            pending.retain("rms", stale);

            // A clean report — retryLater false — retires it.
            PartitionReconciler service = (bindingId, basePath, instant, identityApproved,
                                           quarantine, metrics) ->
                    new PartitionReconciliationService.ReconciliationReport(
                            bindingId, "partition",
                            PartitionReconciliationService.ReconciliationStatus.CLEAN,
                            List.of(), 0, 0, 0, 0, false, null);

            ReconciliationScheduler scheduler = new ReconciliationScheduler(
                    service, properties, id -> null, FIXED, pending);
            scheduler.runBindingQuietly(properties.getBindings().get(0));

            assertThat(pending.pending("rms")).doesNotContain(stale);
        }
    }

    @Test
    void aNotReadyBindingDoesNotAccumulateABacklogForever() throws Exception {
        // NOT_READY sets retryLater, but it means the BINDING has no approved
        // identity — a standing property, not something another pass on this
        // partition could change. Enqueuing it would add every window and
        // retire none.
        IntakeProperties properties = properties(true);
        properties.getReconciliation().setLookbackWindows(2);
        properties.setBindings(new ArrayList<>(List.of(binding("rms", true))));

        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("pending-it");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "file:///");
        try (org.apache.hadoop.fs.FileSystem fs =
                     org.apache.hadoop.fs.FileSystem.getLocal(conf)) {

            PendingPartitions pending =
                    new PendingPartitions(fs, dir.resolve("audit").toString());
            PartitionReconciler service = (bindingId, basePath, instant, identityApproved,
                                           quarantine, metrics) ->
                    PartitionReconciliationService.ReconciliationReport
                            .notReady(bindingId, "partition", "no approved identity");

            ReconciliationScheduler scheduler = new ReconciliationScheduler(
                    service, properties, id -> null, FIXED, pending);
            scheduler.runBindingQuietly(properties.getBindings().get(0));
            scheduler.runBindingQuietly(properties.getBindings().get(0));

            assertThat(pending.pending("rms")).isEmpty();
        }
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
