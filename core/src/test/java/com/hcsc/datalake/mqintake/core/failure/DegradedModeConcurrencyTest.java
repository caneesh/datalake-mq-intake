package com.hcsc.datalake.mqintake.core.failure;

import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The degraded-mode transitions under contention.
 *
 * <p>Every prior unit test of {@code DegradedModeManager} was single-threaded,
 * yet the class exists precisely for the multi-threaded case — MQ
 * redistributes a rolled-back batch across listener threads. The review found
 * the transitions were individually thread-safe but not collectively: a
 * restore could fire in the gap between a concurrent failure's classification
 * and its suspect-marking.
 */
class DegradedModeConcurrencyTest {

    @Test
    void aRestoreCannotFireInsideAConcurrentFailureTransition() throws Exception {
        // Deterministic proof the lock covers the whole transition: thread A
        // blocks INSIDE recordFailure; thread B's recordSuccess — which
        // previously could observe "suspects empty + enough successes" in that
        // gap and restore full batch size with A's poison still in flight —
        // must wait for A, and afterwards the binding is degraded with A's
        // suspect registered.
        CountDownLatch insideFailure = new CountDownLatch(1);
        CountDownLatch releaseFailure = new CountDownLatch(1);
        BlockableManager manager = new BlockableManager(insideFailure, releaseFailure);

        // Arm the restore condition: degraded, suspects cleared, one success
        // short of restore (successesRequiredToRestore = 1).
        manager.passThrough(true);
        manager.recordFailure(dataFailure(), List.of("ID:seed"));
        manager.clearSuspects(List.of("ID:seed"));
        assertThat(manager.isInDegradedMode()).isTrue();
        manager.passThrough(false);

        // A enters recordFailure and parks mid-transition, holding the lock.
        Thread a = new Thread(() -> manager.recordFailure(dataFailure(), List.of("ID:race")));
        a.start();
        assertThat(insideFailure.await(5, TimeUnit.SECONDS)).isTrue();

        // B attempts the restore-completing success.
        AtomicBoolean successReturned = new AtomicBoolean(false);
        Thread b = new Thread(() -> {
            manager.recordSuccess();
            successReturned.set(true);
        });
        b.start();

        // Give B ample time to (wrongly) slip through if the lock were absent.
        Thread.sleep(300);
        assertThat(successReturned.get())
                .as("recordSuccess must block behind the in-flight failure transition")
                .isFalse();

        releaseFailure.countDown();
        a.join(5000);
        b.join(5000);

        assertThat(manager.isInDegradedMode())
                .as("after both complete: degraded, with the racing suspect registered")
                .isTrue();
        assertThat(manager.getSuspectCount()).isEqualTo(1);
        assertThat(manager.getCurrentBatchSize()).isLessThan(16);
    }

    @Test
    void hammeringTransitionsFromManyThreadsLeavesConsistentState() throws Exception {
        DegradedModeManager manager = new DegradedModeManager(
                "stress", 64, DegradationStrategy.BISECT, 3);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(4);

        for (int t = 0; t < 4; t++) {
            final int id = t;
            pool.submit(() -> {
                try {
                    go.await();
                    for (int i = 0; i < 500; i++) {
                        String msgId = "ID:" + id + ":" + i;
                        manager.recordFailure(dataFailure(), List.of(msgId));
                        manager.clearSuspects(List.of(msgId));
                        manager.recordSuccess();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        go.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // Consistency, not a specific trajectory: no orphaned suspects, batch
        // size inside its bounds, and a clean run of successes restores.
        assertThat(manager.getSuspectCount()).isZero();
        assertThat(manager.getCurrentBatchSize()).isBetween(1, 64);
        for (int i = 0; i < 3; i++) {
            manager.recordSuccess();
        }
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(64);
    }

    @Test
    void racingFailuresReportTheEntryEdgeExactlyOnce() throws Exception {
        // The edge is decided inside the synchronized transition, so of N
        // threads whose batches fail together — the normal case, since MQ
        // redistributes a rolled-back batch — exactly one must see
        // enteredDegradedMode()=true, or the loop's entry health/metrics
        // updates would fire once per racing thread.
        DegradedModeManager manager = new DegradedModeManager(
                "race-entry", 64, DegradationStrategy.BISECT, 3);
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger entries =
                new java.util.concurrent.atomic.AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final String id = "ID:entry:" + t;
            pool.submit(() -> {
                go.await();
                if (manager.recordFailure(dataFailure(), List.of(id)).enteredDegradedMode()) {
                    entries.incrementAndGet();
                }
                return null;
            });
        }
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(entries.get()).isEqualTo(1);
        assertThat(manager.isInDegradedMode()).isTrue();
    }

    @Test
    void racingSuccessesReportTheExitEdgeExactlyOnce() throws Exception {
        DegradedModeManager manager = new DegradedModeManager(
                "race-exit", 64, DegradationStrategy.BISECT, 1);
        manager.recordFailure(dataFailure(), List.of("ID:seed"));
        manager.clearSuspects(List.of("ID:seed"));
        assertThat(manager.isInDegradedMode()).isTrue();

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger exits =
                new java.util.concurrent.atomic.AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                go.await();
                if (manager.recordSuccess()) {
                    exits.incrementAndGet();
                }
                return null;
            });
        }
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        assertThat(exits.get()).isEqualTo(1);
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(64);
    }

    // --- helpers ---

    private RecordSerializer.SerializationException dataFailure() {
        return new RecordSerializer.SerializationException("bad payload");
    }

    /**
     * Parks inside the (synchronized) failure transition while the latch pair
     * is armed, so a second thread's transition attempt can be observed to
     * wait. Pass-through mode lets seed calls run unimpeded.
     */
    private static class BlockableManager extends DegradedModeManager {
        private final CountDownLatch inside;
        private final CountDownLatch release;
        private final AtomicBoolean passThrough = new AtomicBoolean(false);

        BlockableManager(CountDownLatch inside, CountDownLatch release) {
            super("race", 16, DegradationStrategy.BISECT, 1);
            this.inside = inside;
            this.release = release;
        }

        void passThrough(boolean value) {
            passThrough.set(value);
        }

        @Override
        public synchronized DegradationPolicy.FailureResult recordFailure(Throwable throwable,
                java.util.Collection<String> batchMessageIds) {
            if (!passThrough.get()) {
                inside.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.recordFailure(throwable, batchMessageIds);
        }
    }
}
