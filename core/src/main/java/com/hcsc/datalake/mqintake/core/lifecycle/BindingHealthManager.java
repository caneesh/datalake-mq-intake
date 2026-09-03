package com.hcsc.datalake.mqintake.core.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages health status for each binding.
 *
 * <p>From DESIGN.md §14: A failure in one binding must not propagate to
 * another's threads. The service stays up serving healthy bindings and
 * reports the failed one unhealthy.
 *
 * <p>This class provides binding isolation by tracking health independently
 * for each binding, allowing the service to continue operating healthy
 * bindings while reporting unhealthy ones.
 */
public class BindingHealthManager {

    private static final Logger log = LoggerFactory.getLogger(BindingHealthManager.class);

    private final Map<String, BindingHealth> healthByBinding = new ConcurrentHashMap<>();

    /**
     * Listeners that cannot complete a unit of work, keyed by binding.
     *
     * <p>Tracked per listener rather than as one flag per binding because a
     * binding's listener threads report independently. With a single flag, the
     * first thread to recover would clear a stall its siblings are still in,
     * and the endpoint would read healthy while most of the binding was
     * committing nothing.
     */
    private final Map<String, Set<String>> stalledListeners = new ConcurrentHashMap<>();

    /**
     * Records a healthy state for a binding.
     *
     * @param bindingId the binding identifier
     */
    public void recordHealthy(String bindingId) {
        BindingHealth health = getOrCreateHealth(bindingId);
        if (health.status != HealthStatus.HEALTHY) {
            log.info("Binding '{}' transitioned to HEALTHY", bindingId);
        }
        health.status = HealthStatus.HEALTHY;
        health.lastHealthyTime = Instant.now();
        health.lastError = null;
        health.consecutiveFailures.set(0);
    }

    /**
     * Records an unhealthy state for a binding with the given error.
     *
     * @param bindingId the binding identifier
     * @param error     the error that caused the unhealthy state
     */
    public void recordUnhealthy(String bindingId, Throwable error) {
        BindingHealth health = getOrCreateHealth(bindingId);
        health.status = HealthStatus.UNHEALTHY;
        health.lastUnhealthyTime = Instant.now();
        health.lastError = error;
        int failures = health.consecutiveFailures.incrementAndGet();

        log.error("Binding '{}' is UNHEALTHY (failure #{}): {}",
                bindingId, failures, error.getMessage());
    }

    /**
     * Records a degraded state for a binding.
     *
     * @param bindingId the binding identifier
     * @param reason    reason for degraded state
     */
    public void recordDegraded(String bindingId, String reason) {
        BindingHealth health = getOrCreateHealth(bindingId);
        if (health.status != HealthStatus.DEGRADED) {
            log.warn("Binding '{}' entered DEGRADED mode: {}", bindingId, reason);
        }
        health.status = HealthStatus.DEGRADED;
        health.degradedReason = reason;
    }

    /**
     * Records that one of a binding's listeners is rolling back repeatedly and
     * committing nothing.
     *
     * <p>This is the reporting path for an infrastructure failure that keeps
     * failing: an unreachable tracker queue, an unwritable HDFS path, an audit
     * store that will not accept a record. Such a failure classifies as
     * infrastructure, which by design never enters degraded batch mode — so
     * before this existed it reached the health manager through no path at
     * all, and a binding could roll back indefinitely while
     * {@code /actuator/health} reported UP.
     *
     * <p>DEGRADED rather than UNHEALTHY on purpose. UNHEALTHY on a
     * single-binding application aggregates to DOWN, which maps to 503 and
     * restarts the pod — and a restart fixes none of the causes above. It
     * would trade a visible stall for a crash loop.
     *
     * @param bindingId  the binding identifier
     * @param listenerId identifies the listener thread, so siblings do not
     *                   clear each other's stalls
     * @param reason     what is failing, for the health endpoint's detail
     */
    public void recordListenerStalled(String bindingId, String listenerId, String reason) {
        Objects.requireNonNull(bindingId, "bindingId required");
        Objects.requireNonNull(listenerId, "listenerId required");
        Set<String> stalled =
                stalledListeners.computeIfAbsent(bindingId, k -> ConcurrentHashMap.newKeySet());
        stalled.add(listenerId);
        recordDegraded(bindingId, reason + " [" + stalled.size() + " listener(s) stalled]");
    }

    /**
     * Records that a listener has committed again.
     *
     * <p>Returns without touching health when this listener was not stalled,
     * which is the normal case on every committed batch. That guard is what
     * keeps an ordinary commit from clearing a DEGRADED state some other
     * mechanism set — degraded batch mode, most obviously.
     *
     * <p>Health is restored only when the LAST stalled listener recovers.
     *
     * @param bindingId  the binding identifier
     * @param listenerId the listener that is committing again
     */
    public void recordListenerProgressing(String bindingId, String listenerId) {
        Set<String> stalled = stalledListeners.get(bindingId);
        if (stalled == null || !stalled.remove(listenerId)) {
            return;
        }
        if (stalled.isEmpty()) {
            log.info("Binding '{}': every stalled listener is committing again", bindingId);
            recordHealthy(bindingId);
        } else {
            recordDegraded(bindingId, stalled.size() + " listener(s) still stalled");
        }
    }

    /** How many of a binding's listeners are currently unable to commit. */
    public int getStalledListenerCount(String bindingId) {
        Set<String> stalled = stalledListeners.get(bindingId);
        return stalled == null ? 0 : stalled.size();
    }

    /**
     * Records that a binding is recovering (e.g., reconnecting to MQ).
     *
     * @param bindingId the binding identifier
     * @param reason    reason for recovery (e.g., "session reconnect attempt 2/10")
     */
    public void recordRecovering(String bindingId, String reason) {
        BindingHealth health = getOrCreateHealth(bindingId);
        if (health.status != HealthStatus.RECOVERING) {
            log.warn("Binding '{}' entered RECOVERING state: {}", bindingId, reason);
        }
        health.status = HealthStatus.RECOVERING;
        health.degradedReason = reason;
    }

    /**
     * Marks a binding as stopped (normal shutdown).
     *
     * @param bindingId the binding identifier
     */
    public void recordStopped(String bindingId) {
        BindingHealth health = getOrCreateHealth(bindingId);
        health.status = HealthStatus.STOPPED;
        log.info("Binding '{}' stopped", bindingId);
    }

    /**
     * Returns the current health status for a binding.
     *
     * @param bindingId the binding identifier
     * @return health status, or UNKNOWN if not registered
     */
    public HealthStatus getStatus(String bindingId) {
        BindingHealth health = healthByBinding.get(bindingId);
        return health != null ? health.status : HealthStatus.UNKNOWN;
    }

    /**
     * Returns the full health snapshot for a binding.
     *
     * @param bindingId the binding identifier
     * @return health snapshot, or null if not registered
     */
    public BindingHealthSnapshot getHealthSnapshot(String bindingId) {
        BindingHealth health = healthByBinding.get(bindingId);
        if (health == null) {
            return null;
        }
        return new BindingHealthSnapshot(
                bindingId,
                health.status,
                health.lastHealthyTime,
                health.lastUnhealthyTime,
                health.consecutiveFailures.get(),
                health.lastError,
                health.degradedReason
        );
    }

    /**
     * Returns true if any binding is unhealthy.
     */
    public boolean hasUnhealthyBindings() {
        return healthByBinding.values().stream()
                .anyMatch(h -> h.status == HealthStatus.UNHEALTHY);
    }

    /**
     * Returns true if all registered bindings are healthy or stopped.
     */
    public boolean allBindingsHealthyOrStopped() {
        return healthByBinding.values().stream()
                .allMatch(h -> h.status == HealthStatus.HEALTHY ||
                        h.status == HealthStatus.STOPPED);
    }

    /**
     * Returns a map of binding ID to health status.
     */
    public Map<String, HealthStatus> getAllStatuses() {
        Map<String, HealthStatus> result = new ConcurrentHashMap<>();
        healthByBinding.forEach((id, health) -> result.put(id, health.status));
        return result;
    }

    private BindingHealth getOrCreateHealth(String bindingId) {
        return healthByBinding.computeIfAbsent(bindingId, id -> new BindingHealth());
    }

    /**
     * Health status for a binding.
     */
    public enum HealthStatus {
        /** Binding is operating normally */
        HEALTHY,
        /** Binding is in degraded mode (batch-of-one or bisecting) */
        DEGRADED,
        /** Binding is recovering from a failure (reconnecting to MQ) */
        RECOVERING,
        /** Binding has failed and is not processing messages */
        UNHEALTHY,
        /** Binding has been stopped (normal shutdown) */
        STOPPED,
        /** Binding status is not known */
        UNKNOWN
    }

    /**
     * Mutable health state for a binding.
     */
    private static class BindingHealth {
        volatile HealthStatus status = HealthStatus.UNKNOWN;
        volatile Instant lastHealthyTime;
        volatile Instant lastUnhealthyTime;
        // Atomic: concurrent recordUnhealthy calls — every listener thread of
        // a binding failing together when a shared connection dies — raced a
        // plain ++ and undercounted the streak.
        final java.util.concurrent.atomic.AtomicInteger consecutiveFailures =
                new java.util.concurrent.atomic.AtomicInteger();
        volatile Throwable lastError;
        volatile String degradedReason;
    }

    /**
     * Immutable snapshot of binding health.
     */
    public static class BindingHealthSnapshot {
        private final String bindingId;
        private final HealthStatus status;
        private final Instant lastHealthyTime;
        private final Instant lastUnhealthyTime;
        private final int consecutiveFailures;
        private final Throwable lastError;
        private final String degradedReason;

        public BindingHealthSnapshot(String bindingId, HealthStatus status,
                                      Instant lastHealthyTime, Instant lastUnhealthyTime,
                                      int consecutiveFailures, Throwable lastError,
                                      String degradedReason) {
            this.bindingId = bindingId;
            this.status = status;
            this.lastHealthyTime = lastHealthyTime;
            this.lastUnhealthyTime = lastUnhealthyTime;
            this.consecutiveFailures = consecutiveFailures;
            this.lastError = lastError;
            this.degradedReason = degradedReason;
        }

        public String getBindingId() { return bindingId; }
        public HealthStatus getStatus() { return status; }
        public Instant getLastHealthyTime() { return lastHealthyTime; }
        public Instant getLastUnhealthyTime() { return lastUnhealthyTime; }
        public int getConsecutiveFailures() { return consecutiveFailures; }
        public Throwable getLastError() { return lastError; }
        public String getDegradedReason() { return degradedReason; }
    }
}
