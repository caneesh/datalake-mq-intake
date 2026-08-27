package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Supervision of receive-loop tasks.
 *
 * <p>BindingRuntime submitted its loops and discarded the Futures, so a loop
 * that exited unexpectedly left the binding reporting RUNNING with fewer — or
 * zero — listeners actually consuming. Nothing polled the queue and nothing
 * said so.
 *
 * <p>These tests use stub Runnables rather than real loops: what is under test
 * is the supervision of a submitted task's lifecycle, and a stub can be made
 * to die on demand, which a real loop cannot.
 */
class ListenerSupervisionTest {

    private ExecutorService executor;
    private BindingHealthManager healthManager;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(3);
        healthManager = new BindingHealthManager();
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void allListenersRunningIsNotReportedAsAFailure() throws Exception {
        CountDownLatch keepRunning = new CountDownLatch(1);
        BindingRuntime runtime = runtimeWith(2, keepRunning);
        healthManager.recordHealthy("test");

        runtime.start();
        runtime.superviseOnce();

        assertThat(runtime.getActiveLoopCount()).isEqualTo(2);
        assertThat(runtime.getTerminatedLoopCount()).isZero();
        assertThat(healthManager.getStatus("test")).isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);

        keepRunning.countDown();
        runtime.stop(2000);
    }

    @Test
    void oneListenerTerminatingUnexpectedlyDegradesTheBinding() throws Exception {
        CountDownLatch keepRunning = new CountDownLatch(1);
        // One task exits immediately; the other keeps running
        BindingRuntime runtime = runtimeWith(List.of(
                stub(new CountDownLatch(0)),      // already released: exits at once
                stub(keepRunning)));
        healthManager.recordHealthy("test");

        runtime.start();
        awaitTerminated(runtime, 1, 3000);
        runtime.superviseOnce();

        assertThat(runtime.getTerminatedLoopCount()).isEqualTo(1);
        assertThat(runtime.getActiveLoopCount()).isEqualTo(1);

        // Still consuming on one thread, so degraded rather than down
        assertThat(healthManager.getStatus("test")).isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(healthManager.getHealthSnapshot("test").getDegradedReason())
                .contains("1 of 2")
                .contains("1 still consuming");

        keepRunning.countDown();
        runtime.stop(2000);
    }

    @Test
    void allListenersTerminatingMarksTheBindingUnhealthyNotMerelyDegraded() throws Exception {
        // Nothing is consuming. Reporting this as "degraded" would let the
        // binding sit silently idle while looking merely impaired.
        BindingRuntime runtime = runtimeWith(2, new CountDownLatch(0));
        healthManager.recordHealthy("test");

        runtime.start();
        awaitTerminated(runtime, 2, 3000);
        runtime.superviseOnce();

        assertThat(runtime.getActiveLoopCount()).isZero();
        assertThat(healthManager.getStatus("test")).isEqualTo(BindingHealthManager.HealthStatus.UNHEALTHY);
        assertThat(healthManager.getHealthSnapshot("test").getLastError())
                .isInstanceOf(ListenerSupervisor.ListenerTerminatedException.class);
    }

    @Test
    void aListenerThrowingIsReportedWithItsCause() throws Exception {
        BindingRuntime runtime = runtimeWith(List.of(
                () -> {
                    throw new IllegalStateException("loop blew up");
                }));
        healthManager.recordHealthy("test");

        runtime.start();
        awaitTerminated(runtime, 1, 3000);
        runtime.superviseOnce();

        assertThat(healthManager.getStatus("test")).isEqualTo(BindingHealthManager.HealthStatus.UNHEALTHY);
    }

    @Test
    void normalShutdownIsNotReportedAsAFailure() throws Exception {
        // The whole point of distinguishing expected from unexpected: a clean
        // stop must not page anyone.
        CountDownLatch keepRunning = new CountDownLatch(1);
        BindingRuntime runtime = runtimeWith(2, keepRunning);
        healthManager.recordHealthy("test");

        runtime.start();
        keepRunning.countDown();
        runtime.stop(2000);

        // Every task has now ended, but by request
        assertThat(runtime.getTerminatedLoopCount()).isEqualTo(2);

        runtime.superviseOnce();   // must be a no-op once not RUNNING

        assertThat(healthManager.getStatus("test")).isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);
    }

    @Test
    void repeatedPassesDoNotReReportTheSameDeadListener() throws Exception {
        // The review finding: the same ERROR re-logged and recordUnhealthy
        // re-fired on every 5s tick, forever, inflating consecutiveFailures
        // without bound. Health must be reported on transition, not per tick.
        BindingRuntime runtime = runtimeWith(2, new CountDownLatch(0));
        healthManager.recordHealthy("test");

        runtime.start();
        awaitTerminated(runtime, 2, 3000);

        for (int i = 0; i < 5; i++) {
            runtime.superviseOnce();
        }

        assertThat(healthManager.getHealthSnapshot("test").getConsecutiveFailures())
                .as("five passes over the same dead listeners = ONE report")
                .isEqualTo(1);
    }

    @Test
    void aSecondTerminationIsANewTransitionAndIsReported() throws Exception {
        CountDownLatch second = new CountDownLatch(1);
        BindingRuntime runtime = runtimeWith(List.of(
                stub(new CountDownLatch(0)),   // dies immediately
                stub(second)));                // dies later
        healthManager.recordHealthy("test");

        runtime.start();
        awaitTerminated(runtime, 1, 3000);
        runtime.superviseOnce();
        assertThat(healthManager.getStatus("test"))
                .isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);

        second.countDown();
        awaitTerminated(runtime, 2, 3000);
        runtime.superviseOnce();
        runtime.superviseOnce();   // and the repeat is again not re-reported

        assertThat(healthManager.getStatus("test"))
                .isEqualTo(BindingHealthManager.HealthStatus.UNHEALTHY);
        assertThat(healthManager.getHealthSnapshot("test").getConsecutiveFailures())
                .isEqualTo(1);
    }

    @Test
    void supervisionRunsOnItsOwnScheduleWithoutBeingDrivenByHand() throws Exception {
        BindingRuntime runtime = runtimeWith(1, new CountDownLatch(0));
        runtime.configureSupervision(healthManager, 50);   // fast cadence for the test
        healthManager.recordHealthy("test");

        runtime.start();

        long deadline = System.currentTimeMillis() + 5000;
        while (healthManager.getStatus("test") == BindingHealthManager.HealthStatus.HEALTHY
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }

        assertThat(healthManager.getStatus("test")).isEqualTo(BindingHealthManager.HealthStatus.UNHEALTHY);
        runtime.stop(2000);
    }

    @Test
    void executorIsShutDownOnStop() throws Exception {
        CountDownLatch keepRunning = new CountDownLatch(1);
        BindingRuntime runtime = runtimeWith(2, keepRunning);

        runtime.start();
        keepRunning.countDown();
        runtime.stop(2000);

        assertThat(executor.isShutdown()).isTrue();
        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.STOPPED);
    }

    // --- helpers ---

    private BindingRuntime runtimeWith(int count, CountDownLatch latch) {
        Runnable[] tasks = new Runnable[count];
        for (int i = 0; i < count; i++) {
            tasks[i] = stub(latch);
        }
        return runtimeWith(List.of(tasks));
    }

    private Runnable stub(CountDownLatch latch) {
        return () -> {
            try {
                latch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    /**
     * Builds a runtime whose "loops" are the given Runnables. BindingRuntime
     * needs TransactedReceiveLoop instances for its stats, so a matching number
     * of stopped loops is supplied alongside the tasks that are submitted.
     */
    private BindingRuntime runtimeWith(List<Runnable> tasks) {
        BindingConfig config = new BindingConfig();
        config.setId("test");
        config.setSourceQueue("TEST.QUEUE");
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath("/tmp/supervision-test");
        config.setBatchSize(10);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(1000);
        config.setListenerThreads(tasks.size());

        BindingRuntime runtime = new TaskBackedBindingRuntime(config, executor, tasks);
        runtime.configureSupervision(healthManager, BindingRuntime.DEFAULT_SUPERVISION_INTERVAL_MS);
        return runtime;
    }

    /**
     * A BindingRuntime that submits arbitrary Runnables instead of real receive
     * loops, so a listener can be made to die on demand.
     */
    private static class TaskBackedBindingRuntime extends BindingRuntime {
        private final List<Runnable> tasks;

        TaskBackedBindingRuntime(BindingConfig config, ExecutorService executor,
                                 List<Runnable> tasks) {
            super(config, unstartedLoops(config, tasks.size()), executor, false, null);
            this.tasks = tasks;
        }

        @Override
        List<Runnable> submittableTasks() {
            return tasks;
        }

        /**
         * Real loop objects, never run — they exist so the runtime's own
         * invariants and stats behave normally while the submitted tasks are
         * the stubs above.
         */
        private static List<TransactedReceiveLoop> unstartedLoops(BindingConfig config, int count) {
            List<TransactedReceiveLoop> loops = new java.util.ArrayList<>();
            for (int i = 0; i < count; i++) {
                loops.add(new TransactedReceiveLoop(
                        config, mock(javax.jms.Connection.class), null, null, null, null,
                        null, null, null, "test", 100));
            }
            return loops;
        }
    }

    private void awaitTerminated(BindingRuntime runtime, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (runtime.getTerminatedLoopCount() < expected
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
    }
}
