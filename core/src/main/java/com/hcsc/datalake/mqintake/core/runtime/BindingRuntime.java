package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.metrics.BackoutQueueDepthMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runtime state for a single binding.
 *
 * <p>Encapsulates the receive loops, executor, and lifecycle for one binding.
 * Each binding gets its own BindingRuntime with N receive loops (one per listener thread).
 *
 * <p>Key invariants:
 * <ul>
 *   <li>Each receive loop runs on its own thread with its own transacted JMS Session</li>
 *   <li>TRACKED bindings have a TrackerMessageBuilder; LAND_ONLY do not</li>
 *   <li>Lifecycle is: CREATED → STARTING → RUNNING → STOPPING → STOPPED</li>
 * </ul>
 */
public class BindingRuntime {

    private static final Logger log = LoggerFactory.getLogger(BindingRuntime.class);

    public enum State {
        CREATED,
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
        FAILED
    }

    private final BindingConfig config;
    private final List<TransactedReceiveLoop> loops;
    private final ExecutorService executor;
    private final boolean hasTrackerBuilder;
    private final BackoutQueueDepthMonitor backoutDepthMonitor;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private volatile Throwable failureCause;

    /**
     * Futures for the submitted loops, retained so their termination can be
     * observed. Without them a loop that dies leaves the binding reporting
     * RUNNING with fewer — or zero — listeners actually consuming.
     */
    private final List<Future<?>> loopFutures = new ArrayList<>();
    private volatile BindingHealthManager healthManager;
    private volatile ListenerSupervisor supervisor;
    private long supervisionIntervalMs = DEFAULT_SUPERVISION_INTERVAL_MS;

    static final long DEFAULT_SUPERVISION_INTERVAL_MS = 5_000;

    BindingRuntime(BindingConfig config,
                   List<TransactedReceiveLoop> loops,
                   ExecutorService executor,
                   boolean hasTrackerBuilder) {
        this(config, loops, executor, hasTrackerBuilder, null);
    }

    BindingRuntime(BindingConfig config,
                   List<TransactedReceiveLoop> loops,
                   ExecutorService executor,
                   boolean hasTrackerBuilder,
                   BackoutQueueDepthMonitor backoutDepthMonitor) {
        this.config = Objects.requireNonNull(config, "config required");
        this.loops = new ArrayList<>(Objects.requireNonNull(loops, "loops required"));
        this.executor = Objects.requireNonNull(executor, "executor required");
        this.hasTrackerBuilder = hasTrackerBuilder;
        this.backoutDepthMonitor = backoutDepthMonitor;  // null when no backout queue

        if (loops.isEmpty()) {
            throw new IllegalArgumentException("At least one receive loop required");
        }

        if (config.getMode() == BindingMode.TRACKED && !hasTrackerBuilder) {
            throw new IllegalArgumentException(
                    "TRACKED binding '" + config.getId() + "' requires a TrackerMessageBuilder");
        }
    }

    /**
     * Starts all receive loops.
     *
     * @throws BindingStartupException if startup fails
     */
    public void start() throws BindingStartupException {
        if (!state.compareAndSet(State.CREATED, State.STARTING)) {
            throw new IllegalStateException(
                    "Cannot start binding '" + config.getId() + "' in state " + state.get());
        }

        log.info("Starting binding '{}': mode={}, threads={}",
                config.getId(), config.getMode(), loops.size());

        try {
            loopFutures.clear();
            for (Runnable task : submittableTasks()) {
                loopFutures.add(executor.submit(task));
            }
            if (backoutDepthMonitor != null) {
                backoutDepthMonitor.start();
            }
            state.set(State.RUNNING);
            supervisor = new ListenerSupervisor(config.getId(), loopFutures, healthManager,
                    supervisionIntervalMs, () -> state.get() == State.RUNNING);
            supervisor.start();
            log.info("Binding '{}' started with {} listener threads", config.getId(), loops.size());
        } catch (Exception e) {
            state.set(State.FAILED);
            failureCause = e;
            abortAfterFailedStart();
            throw new BindingStartupException(
                    "Failed to start binding '" + config.getId() + "': " + e.getMessage(), e);
        }
    }

    /**
     * Cleanup for a {@code start()} that failed after some work had begun.
     *
     * <p>The listener tasks are submitted before the depth monitor and the
     * supervisor start, so a failure in either leaves live, consuming,
     * non-daemon threads behind — and {@link #stop(long)}'s RUNNING→STOPPING
     * guard (correctly) refuses a FAILED runtime, which would make those
     * threads permanent: consumers committing inside an application whose
     * startup failed, with no owner and no way to reach them. A failed start
     * must therefore leave nothing running without depending on the caller.
     * Signalling the loops before interrupting drains them through their
     * normal path; anything uncommitted rolls back, so nothing is lost.
     */
    private void abortAfterFailedStart() {
        if (supervisor != null) {
            supervisor.close();
        }
        if (backoutDepthMonitor != null) {
            backoutDepthMonitor.stop(); // no-op if it never started
        }
        for (TransactedReceiveLoop loop : loops) {
            loop.stop();
        }
        for (Future<?> f : loopFutures) {
            f.cancel(true);
        }
        executor.shutdownNow();
    }

    /**
     * Stops all receive loops gracefully.
     *
     * @param timeoutMs maximum time to wait for loops to stop
     */
    public void stop(long timeoutMs) {
        if (!state.compareAndSet(State.RUNNING, State.STOPPING)) {
            log.debug("Binding '{}' not running, skipping stop", config.getId());
            return;
        }

        log.info("Stopping binding '{}'", config.getId());

        // Supervision stops before the loops do. State is already STOPPING, but
        // stopping the supervisor first removes any chance of a shutting-down
        // loop being reported as an unexpected termination.
        if (supervisor != null) {
            supervisor.close();
        }

        // Stopped next: its browse sessions are created from the same
        // Connection the loops use, and there is no reason to keep sampling
        // while they drain.
        if (backoutDepthMonitor != null) {
            backoutDepthMonitor.stop();
        }

        for (TransactedReceiveLoop loop : loops) {
            loop.stop();
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                log.warn("Binding '{}' did not stop within {}ms, forcing shutdown",
                        config.getId(), timeoutMs);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        state.set(State.STOPPED);
        log.info("Binding '{}' stopped", config.getId());
    }

    /**
     * The tasks {@link #start()} submits — the receive loops in production.
     *
     * <p>Overridable so supervision can be tested with tasks that terminate on
     * demand. A real {@code TransactedReceiveLoop} cannot be made to die
     * without a live queue manager, which would leave the behaviour this
     * method exists to support untested.
     */
    List<Runnable> submittableTasks() {
        return new ArrayList<>(loops);
    }

    /** Number of submitted loops whose task has ended. */
    public int getTerminatedLoopCount() {
        int done = 0;
        for (Future<?> f : loopFutures) {
            if (f.isDone()) {
                done++;
            }
        }
        return done;
    }

    /** Number of submitted loops still executing. */
    public int getActiveLoopCount() {
        return loopFutures.size() - getTerminatedLoopCount();
    }

    /** Runs one supervision pass. Package-private so tests need not wait on the scheduler. */
    void superviseOnce() {
        if (supervisor != null) {
            supervisor.checkOnce();
        }
    }

    /**
     * Injects the health manager and supervision cadence.
     *
     * <p>Set by {@link BindingRuntimeFactory} after construction rather than
     * through the constructor, to leave the existing constructors — used
     * directly by several tests — unchanged.
     */
    void configureSupervision(BindingHealthManager healthManager, long supervisionIntervalMs) {
        this.healthManager = healthManager;
        this.supervisionIntervalMs = supervisionIntervalMs;
    }


    public String getBindingId() {
        return config.getId();
    }

    public BindingConfig getConfig() {
        return config;
    }

    public BindingMode getMode() {
        return config.getMode();
    }

    public State getState() {
        return state.get();
    }

    public boolean isRunning() {
        return state.get() == State.RUNNING;
    }

    /** The backout depth monitor, or null if the binding has no backout queue. */
    public BackoutQueueDepthMonitor getBackoutDepthMonitor() {
        return backoutDepthMonitor;
    }

    public boolean hasTrackerBuilder() {
        return hasTrackerBuilder;
    }

    public int getLoopCount() {
        return loops.size();
    }

    public List<TransactedReceiveLoop> getLoops() {
        return List.copyOf(loops);
    }

    public Throwable getFailureCause() {
        return failureCause;
    }

    /**
     * Returns aggregate statistics across all loops.
     */
    public RuntimeStats getStats() {
        long totalCommits = 0;
        long totalRollbacks = 0;
        long totalMessages = 0;
        int runningLoops = 0;

        for (TransactedReceiveLoop loop : loops) {
            totalCommits += loop.getCommitCount();
            totalRollbacks += loop.getRollbackCount();
            totalMessages += loop.getMessageCount();
            if (loop.isRunning()) {
                runningLoops++;
            }
        }

        return new RuntimeStats(totalCommits, totalRollbacks, totalMessages, runningLoops);
    }

    public static class RuntimeStats {
        private final long commitCount;
        private final long rollbackCount;
        private final long messageCount;
        private final int runningLoops;

        public RuntimeStats(long commitCount, long rollbackCount, long messageCount, int runningLoops) {
            this.commitCount = commitCount;
            this.rollbackCount = rollbackCount;
            this.messageCount = messageCount;
            this.runningLoops = runningLoops;
        }

        public long getCommitCount() { return commitCount; }
        public long getRollbackCount() { return rollbackCount; }
        public long getMessageCount() { return messageCount; }
        public int getRunningLoops() { return runningLoops; }
    }

    public static class BindingStartupException extends Exception {
        public BindingStartupException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
