package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Watches a binding's listener tasks and reports when one dies unexpectedly.
 *
 * <p>Without this, a binding whose receive loops had exited still reported
 * RUNNING: no messages moving, no error, nothing to alert on. The Futures were
 * discarded at submit time, so nothing could observe termination at all.
 *
 * <p>Deliberately does <em>not</em> restart anything. Recoverable MQ failures
 * are already the receive loop's responsibility, with bounded reconnect and
 * backoff; a competing restart loop here would fight that logic. A loop that
 * exited despite it has hit something reconnection cannot fix, so the binding
 * is made visibly unhealthy and the deployment platform decides.
 *
 * <p>Separated from {@link BindingRuntime} because supervising tasks and
 * running a lifecycle are different jobs — the runtime should say "start, then
 * watch", not carry the watching itself.
 */
public class ListenerSupervisor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ListenerSupervisor.class);

    private final String bindingId;
    private final List<Future<?>> futures;
    private final BindingHealthManager healthManager;
    private final long intervalMs;

    /**
     * Whether termination should currently be treated as unexpected. False
     * while the binding is stopping, so a clean shutdown never pages anyone.
     */
    private final BooleanSupplier supervisionActive;

    /**
     * Indices whose termination has already been logged. The previous scalar
     * (`reported < futures.size()`) stayed true forever in the partial-death
     * case, so the same ERROR line re-logged every tick for the life of the
     * process — enough volume over days to bury real diagnostics.
     */
    private final java.util.Set<Integer> loggedIndices =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Last termination count reported to health; report only on change. */
    private final AtomicInteger lastReportedTerminations = new AtomicInteger(0);

    private volatile ScheduledExecutorService scheduler;

    public ListenerSupervisor(String bindingId,
                              List<Future<?>> futures,
                              BindingHealthManager healthManager,
                              long intervalMs,
                              BooleanSupplier supervisionActive) {
        this.bindingId = Objects.requireNonNull(bindingId, "bindingId required");
        this.futures = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(futures, "futures required")));
        this.healthManager = healthManager;
        this.intervalMs = intervalMs;
        this.supervisionActive = Objects.requireNonNull(supervisionActive, "supervisionActive required");
    }

    /** A non-positive interval disables supervision entirely. */
    public void start() {
        if (intervalMs <= 0) {
            log.info("Listener supervision disabled for binding '{}'", bindingId);
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "supervise-" + bindingId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::checkQuietly, intervalMs, intervalMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdownNow();
            try {
                // Wait for any in-flight pass. Without this, a pass that read
                // "still RUNNING" moments before stop() could observe the
                // loops' just-completed futures and report a clean shutdown as
                // an unexpected termination.
                s.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Wraps {@link #checkOnce()} so nothing escapes into the scheduler. A task
     * that throws out of scheduleWithFixedDelay is cancelled silently, which
     * would leave the binding unsupervised with no indication — the same
     * failure mode this class exists to remove.
     */
    private void checkQuietly() {
        try {
            checkOnce();
        } catch (Throwable t) {
            log.error("Listener supervision failed for binding '{}': {}",
                    bindingId, t.getMessage(), t);
        }
    }

    /**
     * One supervision pass. Package-private so tests can drive it
     * deterministically rather than waiting on the scheduler.
     */
    void checkOnce() {
        if (!supervisionActive.getAsBoolean()) {
            return;   // stopping or stopped: termination is expected
        }

        int terminated = 0;
        for (int i = 0; i < futures.size(); i++) {
            Future<?> future = futures.get(i);
            if (!future.isDone()) {
                continue;
            }
            terminated++;

            if (loggedIndices.add(i)) {
                Throwable cause = terminationCause(future);
                log.error("Receive loop {} of {} for binding '{}' terminated unexpectedly "
                                + "while the binding is RUNNING: {}",
                        i + 1, futures.size(), bindingId,
                        cause == null ? "no exception reported" : cause.toString(), cause);
            }
        }

        if (terminated == 0) {
            return;
        }

        // Health is reported on TRANSITION, not per tick. recordUnhealthy has
        // no internal dedup — it logs an ERROR and increments the failure
        // counter on every call — so calling it every 5 seconds forever made
        // consecutiveFailures meaningless and flooded the log channel.
        if (lastReportedTerminations.getAndSet(terminated) == terminated) {
            return;
        }

        // Re-check right before reporting: stop() may have raced this pass.
        if (!supervisionActive.getAsBoolean()) {
            return;
        }

        reportHealth(terminated);
    }

    private void reportHealth(int terminated) {
        if (healthManager == null) {
            return;
        }

        int alive = futures.size() - terminated;
        String reason = String.format(
                "%d of %d listener threads for binding '%s' have terminated unexpectedly; "
                        + "%d still consuming",
                terminated, futures.size(), bindingId, alive);

        if (alive == 0) {
            // Nothing is consuming. The binding is not degraded, it is down,
            // and anything softer would let it sit silently idle.
            healthManager.recordUnhealthy(bindingId, new ListenerTerminatedException(reason));
        } else {
            healthManager.recordDegraded(bindingId, reason);
        }
    }

    private Throwable terminationCause(Future<?> future) {
        try {
            future.get(0, TimeUnit.MILLISECONDS);
            return null;   // returned normally
        } catch (ExecutionException e) {
            return e.getCause() != null ? e.getCause() : e;
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            return e;
        }
    }

    public int terminatedCount() {
        int done = 0;
        for (Future<?> f : futures) {
            if (f.isDone()) {
                done++;
            }
        }
        return done;
    }

    public int activeCount() {
        return futures.size() - terminatedCount();
    }

    /** Raised only to carry the reason into the health manager. */
    public static class ListenerTerminatedException extends RuntimeException {
        public ListenerTerminatedException(String message) {
            super(message);
        }
    }
}
