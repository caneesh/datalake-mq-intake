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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
            for (TransactedReceiveLoop loop : loops) {
                executor.submit(loop);
            }
            if (backoutDepthMonitor != null) {
                backoutDepthMonitor.start();
            }
            state.set(State.RUNNING);
            log.info("Binding '{}' started with {} listener threads", config.getId(), loops.size());
        } catch (Exception e) {
            state.set(State.FAILED);
            failureCause = e;
            throw new BindingStartupException(
                    "Failed to start binding '" + config.getId() + "': " + e.getMessage(), e);
        }
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

        // Stopped first: its browse sessions are created from the same
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
