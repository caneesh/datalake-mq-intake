package com.hcsc.datalake.mqintake.core.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Central registry for all metrics.
 *
 * <p>From DESIGN.md §14: Metrics must be dimensioned by binding_id.
 * This registry provides per-binding metrics plus JVM-level metrics
 * like Kerberos relogin failures.
 *
 * <p>Aggregate dashboards hide a single stalled binding.
 * Alert on per-binding thresholds.
 */
public class MetricsRegistry {

    private final Map<String, BindingMetrics> bindingMetrics = new ConcurrentHashMap<>();

    // JVM-level metrics
    private final AtomicLong kerberosReloginFailures = new AtomicLong(0);
    private Supplier<Long> kerberosReloginFailureSupplier;

    /**
     * Gets or creates metrics for a binding.
     *
     * @param bindingId the binding identifier
     * @return metrics for the binding
     */
    public BindingMetrics forBinding(String bindingId) {
        return bindingMetrics.computeIfAbsent(bindingId, BindingMetrics::new);
    }

    /**
     * Returns all registered binding metrics.
     */
    public Collection<BindingMetrics> getAllBindingMetrics() {
        return bindingMetrics.values();
    }

    /**
     * Returns metrics for a specific binding, or null if not registered.
     */
    public BindingMetrics getBindingMetrics(String bindingId) {
        return bindingMetrics.get(bindingId);
    }

    /**
     * Returns true if metrics exist for the given binding.
     */
    public boolean hasBinding(String bindingId) {
        return bindingMetrics.containsKey(bindingId);
    }

    // --- JVM-level metrics ---

    /**
     * Records a Kerberos relogin failure.
     */
    public void recordKerberosReloginFailure() {
        kerberosReloginFailures.incrementAndGet();
    }

    /**
     * Gets the count of Kerberos relogin failures.
     */
    public long getKerberosReloginFailures() {
        if (kerberosReloginFailureSupplier != null) {
            return kerberosReloginFailureSupplier.get();
        }
        return kerberosReloginFailures.get();
    }

    /**
     * Sets a supplier for Kerberos relogin failures (e.g., from KerberosManager).
     */
    public void setKerberosReloginFailureSupplier(Supplier<Long> supplier) {
        this.kerberosReloginFailureSupplier = supplier;
    }

    /**
     * Returns a snapshot of all metrics.
     */
    public RegistrySnapshot snapshot() {
        Map<String, BindingMetrics.MetricsSnapshot> bindings = new ConcurrentHashMap<>();
        bindingMetrics.forEach((id, metrics) -> bindings.put(id, metrics.snapshot()));
        return new RegistrySnapshot(bindings, getKerberosReloginFailures());
    }

    /**
     * Returns aggregate statistics across all bindings.
     */
    public AggregateStats aggregateStats() {
        long totalCommits = 0;
        long totalRollbacks = 0;
        long totalMessages = 0;
        long totalBytes = 0;
        long totalPoisonMessages = 0;
        int healthyBindings = 0;
        int degradedBindings = 0;
        int unhealthyBindings = 0;

        for (BindingMetrics metrics : bindingMetrics.values()) {
            totalCommits += metrics.getCommitCount();
            totalRollbacks += metrics.getRollbackCount();
            totalMessages += metrics.getMessagesWritten();
            totalBytes += metrics.getBytesWritten();
            totalPoisonMessages += metrics.getPoisonMessagesRouted();

            if (metrics.isHealthy()) {
                if (metrics.isInDegradedMode()) {
                    degradedBindings++;
                } else {
                    healthyBindings++;
                }
            } else {
                unhealthyBindings++;
            }
        }

        return new AggregateStats(
                bindingMetrics.size(),
                totalCommits,
                totalRollbacks,
                totalMessages,
                totalBytes,
                totalPoisonMessages,
                healthyBindings,
                degradedBindings,
                unhealthyBindings,
                getKerberosReloginFailures()
        );
    }

    /**
     * Immutable snapshot of all metrics.
     */
    public static class RegistrySnapshot {
        private final Map<String, BindingMetrics.MetricsSnapshot> bindingSnapshots;
        private final long kerberosReloginFailures;

        public RegistrySnapshot(Map<String, BindingMetrics.MetricsSnapshot> bindingSnapshots,
                                 long kerberosReloginFailures) {
            this.bindingSnapshots = Map.copyOf(bindingSnapshots);
            this.kerberosReloginFailures = kerberosReloginFailures;
        }

        public Map<String, BindingMetrics.MetricsSnapshot> getBindingSnapshots() {
            return bindingSnapshots;
        }

        public long getKerberosReloginFailures() {
            return kerberosReloginFailures;
        }
    }

    /**
     * Aggregate statistics across all bindings.
     */
    public static class AggregateStats {
        private final int totalBindings;
        private final long totalCommits;
        private final long totalRollbacks;
        private final long totalMessages;
        private final long totalBytes;
        private final long totalPoisonMessages;
        private final int healthyBindings;
        private final int degradedBindings;
        private final int unhealthyBindings;
        private final long kerberosReloginFailures;

        public AggregateStats(int totalBindings, long totalCommits, long totalRollbacks,
                               long totalMessages, long totalBytes, long totalPoisonMessages,
                               int healthyBindings, int degradedBindings, int unhealthyBindings,
                               long kerberosReloginFailures) {
            this.totalBindings = totalBindings;
            this.totalCommits = totalCommits;
            this.totalRollbacks = totalRollbacks;
            this.totalMessages = totalMessages;
            this.totalBytes = totalBytes;
            this.totalPoisonMessages = totalPoisonMessages;
            this.healthyBindings = healthyBindings;
            this.degradedBindings = degradedBindings;
            this.unhealthyBindings = unhealthyBindings;
            this.kerberosReloginFailures = kerberosReloginFailures;
        }

        public int getTotalBindings() { return totalBindings; }
        public long getTotalCommits() { return totalCommits; }
        public long getTotalRollbacks() { return totalRollbacks; }
        public long getTotalMessages() { return totalMessages; }
        public long getTotalBytes() { return totalBytes; }
        public long getTotalPoisonMessages() { return totalPoisonMessages; }
        public int getHealthyBindings() { return healthyBindings; }
        public int getDegradedBindings() { return degradedBindings; }
        public int getUnhealthyBindings() { return unhealthyBindings; }
        public long getKerberosReloginFailures() { return kerberosReloginFailures; }

        public double getCommitRate() {
            long total = totalCommits + totalRollbacks;
            return total == 0 ? 1.0 : (double) totalCommits / total;
        }
    }
}
