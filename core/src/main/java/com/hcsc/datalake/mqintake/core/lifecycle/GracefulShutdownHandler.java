package com.hcsc.datalake.mqintake.core.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles graceful shutdown of the service.
 *
 * <p>From DESIGN.md §14: A shutdown hook must drain the in-flight batch
 * FOR EVERY BINDING: stop accepting new gets, flush, close, rename,
 * put acks, commit, then exit. Bind the drain to a bounded timeout so
 * one stuck binding cannot block shutdown indefinitely.
 *
 * <p>CRITICAL: On timeout, roll back and close the MQ session.
 * NEVER force a rename to "save" a batch whose MQ unit of work cannot
 * commit — that manufactures exactly the LANDED-without-commit state
 * that §10 then has to disambiguate.
 */
public class GracefulShutdownHandler {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownHandler.class);

    private final long drainTimeoutMs;
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final Map<String, BindingDrainTask> drainTasks = new ConcurrentHashMap<>();
    private final ExecutorService drainExecutor;

    /**
     * Creates a shutdown handler with the specified drain timeout.
     *
     * @param drainTimeoutMs maximum time to wait for each binding to drain
     */
    public GracefulShutdownHandler(long drainTimeoutMs) {
        if (drainTimeoutMs <= 0) {
            throw new IllegalArgumentException("drainTimeoutMs must be positive");
        }
        this.drainTimeoutMs = drainTimeoutMs;
        this.drainExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "shutdown-drain");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Registers a binding for graceful shutdown.
     *
     * @param bindingId  the binding identifier
     * @param drainTask  the task that drains the binding's in-flight batch
     */
    public void registerBinding(String bindingId, BindingDrainTask drainTask) {
        Objects.requireNonNull(bindingId, "bindingId required");
        Objects.requireNonNull(drainTask, "drainTask required");
        drainTasks.put(bindingId, drainTask);
        log.debug("Registered binding '{}' for graceful shutdown", bindingId);
    }

    /**
     * Unregisters a binding from graceful shutdown.
     *
     * @param bindingId the binding identifier
     */
    public void unregisterBinding(String bindingId) {
        drainTasks.remove(bindingId);
        log.debug("Unregistered binding '{}' from graceful shutdown", bindingId);
    }

    /**
     * Returns true if shutdown has been requested.
     */
    public boolean isShutdownRequested() {
        return shutdownRequested.get();
    }

    /**
     * Initiates graceful shutdown.
     * Drains all registered bindings with a bounded timeout.
     *
     * @return results for each binding
     */
    public ShutdownResult shutdown() {
        if (!shutdownRequested.compareAndSet(false, true)) {
            log.warn("Shutdown already in progress");
            return new ShutdownResult(List.of(), List.of(), List.of());
        }

        log.info("Initiating graceful shutdown for {} bindings", drainTasks.size());

        List<String> successful = new ArrayList<>();
        List<String> timedOut = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        // Submit all drain tasks concurrently
        Map<String, Future<DrainOutcome>> futures = new ConcurrentHashMap<>();
        for (var entry : drainTasks.entrySet()) {
            String bindingId = entry.getKey();
            BindingDrainTask task = entry.getValue();
            futures.put(bindingId, drainExecutor.submit(() -> drainBinding(bindingId, task)));
        }

        // Wait for each with timeout
        for (var entry : futures.entrySet()) {
            String bindingId = entry.getKey();
            Future<DrainOutcome> future = entry.getValue();

            try {
                DrainOutcome outcome = future.get(drainTimeoutMs, TimeUnit.MILLISECONDS);
                switch (outcome) {
                    case SUCCESS:
                        successful.add(bindingId);
                        break;
                    case TIMEOUT:
                        timedOut.add(bindingId);
                        break;
                    case ERROR:
                        failed.add(bindingId);
                        break;
                }
            } catch (TimeoutException e) {
                log.warn("Binding '{}' drain timed out after {}ms — rolling back",
                        bindingId, drainTimeoutMs);
                timedOut.add(bindingId);
                future.cancel(true);
            } catch (InterruptedException e) {
                log.warn("Binding '{}' drain interrupted", bindingId);
                failed.add(bindingId);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Binding '{}' drain failed: {}", bindingId, e.getCause().getMessage());
                failed.add(bindingId);
            }
        }

        // Shutdown the executor
        drainExecutor.shutdown();

        log.info("Graceful shutdown complete: {} successful, {} timed out, {} failed",
                successful.size(), timedOut.size(), failed.size());

        return new ShutdownResult(successful, timedOut, failed);
    }

    /**
     * Drains a single binding.
     */
    private DrainOutcome drainBinding(String bindingId, BindingDrainTask task) {
        log.info("Draining binding '{}'", bindingId);
        try {
            boolean success = task.drain(drainTimeoutMs);
            if (success) {
                log.info("Binding '{}' drained successfully", bindingId);
                return DrainOutcome.SUCCESS;
            } else {
                log.warn("Binding '{}' drain did not complete — rolling back", bindingId);
                return DrainOutcome.TIMEOUT;
            }
        } catch (Exception e) {
            log.error("Binding '{}' drain failed: {}", bindingId, e.getMessage(), e);
            return DrainOutcome.ERROR;
        }
    }

    /**
     * Creates and registers a JVM shutdown hook.
     *
     * @return the shutdown hook thread (for testing)
     */
    public Thread registerShutdownHook() {
        Thread hook = new Thread(() -> {
            log.info("JVM shutdown hook triggered");
            shutdown();
        }, "shutdown-hook");
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    /**
     * Task that drains a binding's in-flight work.
     */
    @FunctionalInterface
    public interface BindingDrainTask {
        /**
         * Drains the binding's in-flight batch.
         *
         * <p>Should: stop accepting new gets, flush, close, rename,
         * put tracker acks, commit.
         *
         * <p>On timeout or failure: MUST roll back and close the session.
         * NEVER force a rename to "save" a batch whose MQ transaction
         * cannot commit.
         *
         * @param timeoutMs maximum time to wait
         * @return true if drain completed successfully, false if timed out
         * @throws Exception if drain fails
         */
        boolean drain(long timeoutMs) throws Exception;
    }

    /**
     * Helper to create a drain task that properly rolls back on failure.
     */
    public static BindingDrainTask createSafeDrainTask(
            Session session,
            Runnable flushAndCommit,
            Runnable cleanup) {

        return timeoutMs -> {
            try {
                // Try to complete the in-flight batch
                flushAndCommit.run();
                return true;
            } catch (Exception e) {
                // CRITICAL: On any failure, roll back — never force a commit
                log.warn("Drain failed, rolling back: {}", e.getMessage());
                try {
                    session.rollback();
                } catch (JMSException rollbackEx) {
                    log.error("Rollback failed: {}", rollbackEx.getMessage());
                }
                return false;
            } finally {
                // Always clean up resources
                if (cleanup != null) {
                    try {
                        cleanup.run();
                    } catch (Exception cleanupEx) {
                        log.warn("Cleanup failed: {}", cleanupEx.getMessage());
                    }
                }
            }
        };
    }

    private enum DrainOutcome {
        SUCCESS, TIMEOUT, ERROR
    }

    /**
     * Result of a graceful shutdown.
     */
    public static class ShutdownResult {
        private final List<String> successful;
        private final List<String> timedOut;
        private final List<String> failed;

        public ShutdownResult(List<String> successful, List<String> timedOut, List<String> failed) {
            this.successful = List.copyOf(successful);
            this.timedOut = List.copyOf(timedOut);
            this.failed = List.copyOf(failed);
        }

        public List<String> getSuccessful() { return successful; }
        public List<String> getTimedOut() { return timedOut; }
        public List<String> getFailed() { return failed; }

        public boolean allSuccessful() {
            return timedOut.isEmpty() && failed.isEmpty();
        }

        public int totalBindings() {
            return successful.size() + timedOut.size() + failed.size();
        }
    }
}
