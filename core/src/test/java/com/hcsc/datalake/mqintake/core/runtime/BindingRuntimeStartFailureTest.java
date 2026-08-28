package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.metrics.BackoutQueueDepthMonitor;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * A {@code start()} that fails AFTER the listener tasks were submitted.
 *
 * <p>The review found the original catch block set FAILED and rethrew without
 * cancelling the already-submitted futures or shutting the executor down —
 * and since {@code stop()}'s RUNNING→STOPPING guard refuses a FAILED runtime,
 * the leaked non-daemon consumer threads were permanent: consuming and
 * committing inside an application whose startup had failed, unreachable by
 * any code path for the life of the JVM.
 */
class BindingRuntimeStartFailureTest {

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void startFailureAfterTaskSubmissionLeavesNothingRunning() throws Exception {
        executor = Executors.newFixedThreadPool(1);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskInterrupted = new CountDownLatch(1);
        Runnable listenerStandIn = () -> {
            taskStarted.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                taskInterrupted.countDown();
            }
        };

        // The depth monitor starts after the tasks are submitted, so a throw
        // here is the failure point the leak needed. RejectedExecutionException
        // stands in for the real trigger (thread-creation failure under
        // resource exhaustion) — nothing in the monitor throws routinely.
        BackoutQueueDepthMonitor throwingMonitor = new BackoutQueueDepthMonitor(
                "leak-test", "LEAK.BOQ", mock(Connection.class),
                BindingMetrics.noop(), 1_000) {
            @Override
            public void start() {
                throw new RejectedExecutionException("no threads left");
            }
        };

        BindingRuntime runtime = new BindingRuntime(
                minimalConfig(), List.of(unstartedLoop()), executor, false, throwingMonitor) {
            @Override
            List<Runnable> submittableTasks() {
                return List.of(listenerStandIn);
            }
        };

        assertThatThrownBy(runtime::start)
                .isInstanceOf(BindingRuntime.BindingStartupException.class)
                .hasRootCauseInstanceOf(RejectedExecutionException.class);

        // The task genuinely ran — the leak was real, not hypothetical —
        // and the failed start itself shut it down.
        assertThat(taskStarted.await(2, TimeUnit.SECONDS))
                .as("the submitted task must actually have started").isTrue();
        assertThat(taskInterrupted.await(5, TimeUnit.SECONDS))
                .as("a failed start must interrupt its submitted tasks").isTrue();
        assertThat(executor.isShutdown())
                .as("a failed start must shut its executor down").isTrue();
        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.FAILED);
        assertThat(runtime.getActiveLoopCount()).isZero();
    }

    // --- harness ---

    private BindingConfig minimalConfig() {
        BindingConfig config = new BindingConfig();
        config.setId("leak-test");
        config.setMode(BindingMode.LAND_ONLY);
        config.setSourceQueue("LEAK.SOURCE");
        return config;
    }

    /** A real loop object that is never run; the submitted task is the stand-in. */
    private TransactedReceiveLoop unstartedLoop() {
        return new TransactedReceiveLoop(
                minimalConfig(), mock(Connection.class), null, null, null, null,
                null, null, null, "test", 100);
    }
}
