package com.hcsc.datalake.mqintake.core.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for MetricsRegistry.
 *
 * <p>From DESIGN.md §14: Metrics dimensioned by binding_id.
 * Kerberos relogin failures at JVM level.
 */
class MetricsRegistryTest {

    private MetricsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetricsRegistry();
    }

    @Test
    void forBindingCreatesNewMetrics() {
        BindingMetrics metrics = registry.forBinding("rms");

        assertThat(metrics).isNotNull();
        assertThat(metrics.getBindingId()).isEqualTo("rms");
    }

    @Test
    void forBindingReturnsSameInstanceForSameId() {
        BindingMetrics first = registry.forBinding("rms");
        BindingMetrics second = registry.forBinding("rms");

        assertThat(first).isSameAs(second);
    }

    @Test
    void differentBindingsGetDifferentMetrics() {
        BindingMetrics rms = registry.forBinding("rms");
        BindingMetrics claims = registry.forBinding("claims");

        assertThat(rms).isNotSameAs(claims);
        assertThat(rms.getBindingId()).isEqualTo("rms");
        assertThat(claims.getBindingId()).isEqualTo("claims");
    }

    @Test
    void getAllBindingMetricsReturnsAllRegistered() {
        registry.forBinding("rms");
        registry.forBinding("claims");
        registry.forBinding("other");

        assertThat(registry.getAllBindingMetrics()).hasSize(3);
    }

    @Test
    void getBindingMetricsReturnsNullIfNotRegistered() {
        assertThat(registry.getBindingMetrics("nonexistent")).isNull();
    }

    @Test
    void hasBindingReturnsTrueIfRegistered() {
        registry.forBinding("rms");

        assertThat(registry.hasBinding("rms")).isTrue();
        assertThat(registry.hasBinding("nonexistent")).isFalse();
    }

    @Test
    void kerberosReloginFailuresStartAtZero() {
        assertThat(registry.getKerberosReloginFailures()).isEqualTo(0);
    }

    @Test
    void recordKerberosReloginFailureIncrementsCount() {
        registry.recordKerberosReloginFailure();
        registry.recordKerberosReloginFailure();
        registry.recordKerberosReloginFailure();

        assertThat(registry.getKerberosReloginFailures()).isEqualTo(3);
    }

    @Test
    void kerberosReloginFailureSupplierOverridesCounter() {
        registry.recordKerberosReloginFailure(); // This will be ignored
        registry.setKerberosReloginFailureSupplier(() -> 42L);

        assertThat(registry.getKerberosReloginFailures()).isEqualTo(42);
    }

    @Test
    void snapshotCapturesAllBindings() {
        registry.forBinding("rms").recordCommit();
        registry.forBinding("claims").recordRollback();

        var snapshot = registry.snapshot();

        assertThat(snapshot.getBindingSnapshots()).hasSize(2);
        assertThat(snapshot.getBindingSnapshots().get("rms").getCommitCount()).isEqualTo(1);
        assertThat(snapshot.getBindingSnapshots().get("claims").getRollbackCount()).isEqualTo(1);
    }

    @Test
    void snapshotCapturesKerberosFailures() {
        registry.recordKerberosReloginFailure();

        var snapshot = registry.snapshot();

        assertThat(snapshot.getKerberosReloginFailures()).isEqualTo(1);
    }

    @Test
    void aggregateStatsComputesTotals() {
        BindingMetrics rms = registry.forBinding("rms");
        rms.recordCommit();
        rms.recordCommit();
        rms.recordMessagesWritten(100, 50000);

        BindingMetrics claims = registry.forBinding("claims");
        claims.recordCommit();
        claims.recordRollback();
        claims.recordMessagesWritten(200, 100000);

        var stats = registry.aggregateStats();

        assertThat(stats.getTotalBindings()).isEqualTo(2);
        assertThat(stats.getTotalCommits()).isEqualTo(3);
        assertThat(stats.getTotalRollbacks()).isEqualTo(1);
        assertThat(stats.getTotalMessages()).isEqualTo(300);
        assertThat(stats.getTotalBytes()).isEqualTo(150000);
    }

    @Test
    void aggregateStatsCountsHealthyDegradedUnhealthy() {
        BindingMetrics healthy = registry.forBinding("healthy");
        healthy.setHealthy(true);

        BindingMetrics degraded = registry.forBinding("degraded");
        degraded.setHealthy(true);
        degraded.recordDegradedModeEntry();

        BindingMetrics unhealthy = registry.forBinding("unhealthy");
        unhealthy.setHealthy(false);

        var stats = registry.aggregateStats();

        assertThat(stats.getHealthyBindings()).isEqualTo(1);
        assertThat(stats.getDegradedBindings()).isEqualTo(1);
        assertThat(stats.getUnhealthyBindings()).isEqualTo(1);
    }

    @Test
    void commitRateCalculation() {
        BindingMetrics metrics = registry.forBinding("test");
        metrics.recordCommit();
        metrics.recordCommit();
        metrics.recordCommit();
        metrics.recordRollback();

        var stats = registry.aggregateStats();

        assertThat(stats.getCommitRate()).isEqualTo(0.75); // 3 commits / 4 total
    }

    @Test
    void commitRateIsOneWithNoTransactions() {
        registry.forBinding("test"); // No commits or rollbacks

        var stats = registry.aggregateStats();

        assertThat(stats.getCommitRate()).isEqualTo(1.0);
    }
}
