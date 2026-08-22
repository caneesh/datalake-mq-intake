package com.hcsc.datalake.mqintake.core.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
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
        health.consecutiveFailures = 0;
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
        health.consecutiveFailures++;

        log.error("Binding '{}' is UNHEALTHY (failure #{}): {}",
                bindingId, health.consecutiveFailures, error.getMessage());
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
                health.consecutiveFailures,
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
        volatile int consecutiveFailures = 0;
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
