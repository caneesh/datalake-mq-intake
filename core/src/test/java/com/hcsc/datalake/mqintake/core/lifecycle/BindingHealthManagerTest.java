package com.hcsc.datalake.mqintake.core.lifecycle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for BindingHealthManager.
 *
 * <p>From DESIGN.md §14: A failure in one binding must not propagate
 * to another's threads. The service stays up serving healthy bindings
 * and reports the failed one unhealthy.
 */
class BindingHealthManagerTest {

    private BindingHealthManager healthManager;

    @BeforeEach
    void setUp() {
        healthManager = new BindingHealthManager();
    }

    @Test
    void newBindingStatusIsUnknown() {
        assertThat(healthManager.getStatus("new-binding"))
                .isEqualTo(BindingHealthManager.HealthStatus.UNKNOWN);
    }

    @Test
    void recordHealthySetsHealthyStatus() {
        healthManager.recordHealthy("rms");

        assertThat(healthManager.getStatus("rms"))
                .isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);
    }

    @Test
    void recordUnhealthySetsUnhealthyStatus() {
        healthManager.recordUnhealthy("rms", new RuntimeException("Test error"));

        assertThat(healthManager.getStatus("rms"))
                .isEqualTo(BindingHealthManager.HealthStatus.UNHEALTHY);
    }

    @Test
    void recordDegradedSetsDegradedStatus() {
        healthManager.recordDegraded("rms", "Bisecting batch");

        assertThat(healthManager.getStatus("rms"))
                .isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
    }

    // --- Stalled listeners (infrastructure rollback loops) ---

    @Test
    void aStalledListenerMakesTheBindingDegraded() {
        healthManager.recordHealthy("rms");

        healthManager.recordListenerStalled("rms", "recv-rms-1", "HDFS unwritable");

        assertThat(healthManager.getStatus("rms")).isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(healthManager.getStalledListenerCount("rms")).isEqualTo(1);
        assertThat(healthManager.getHealthSnapshot("rms").getDegradedReason())
                .contains("HDFS unwritable");
    }

    @Test
    void oneListenerRecoveringDoesNotClearItsSiblingsStall() {
        healthManager.recordListenerStalled("rms", "recv-rms-1", "HDFS unwritable");
        healthManager.recordListenerStalled("rms", "recv-rms-2", "HDFS unwritable");

        healthManager.recordListenerProgressing("rms", "recv-rms-1");

        // The binding is still committing nothing on listener 2. A single flag
        // would have read healthy here.
        assertThat(healthManager.getStatus("rms")).isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(healthManager.getStalledListenerCount("rms")).isEqualTo(1);

        healthManager.recordListenerProgressing("rms", "recv-rms-2");

        assertThat(healthManager.getStatus("rms")).isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);
        assertThat(healthManager.getStalledListenerCount("rms")).isZero();
    }

    @Test
    void progressFromAListenerThatWasNeverStalledLeavesHealthAlone() {
        // Every committed batch calls this. Without the guard it would clear a
        // DEGRADED state that degraded batch mode set, on the very next commit.
        healthManager.recordDegraded("rms", "degraded batch mode: isolating a poison message");

        healthManager.recordListenerProgressing("rms", "recv-rms-1");

        assertThat(healthManager.getStatus("rms")).isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(healthManager.getHealthSnapshot("rms").getDegradedReason())
                .contains("poison message");
    }

    @Test
    void stallsAreTrackedPerBindingNotGlobally() {
        healthManager.recordHealthy("claims");
        healthManager.recordListenerStalled("rms", "recv-rms-1", "tracker queue full");

        assertThat(healthManager.getStatus("rms")).isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(healthManager.getStatus("claims")).isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);
        assertThat(healthManager.getStalledListenerCount("claims")).isZero();
    }

    @Test
    void recordStoppedSetsStoppedStatus() {
        healthManager.recordStopped("rms");

        assertThat(healthManager.getStatus("rms"))
                .isEqualTo(BindingHealthManager.HealthStatus.STOPPED);
    }

    @Test
    void bindingIsolation_oneUnhealthyDoesNotAffectOther() {
        healthManager.recordHealthy("rms");
        healthManager.recordUnhealthy("claims", new RuntimeException("Claims failed"));

        assertThat(healthManager.getStatus("rms"))
                .isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);
        assertThat(healthManager.getStatus("claims"))
                .isEqualTo(BindingHealthManager.HealthStatus.UNHEALTHY);
    }

    @Test
    void hasUnhealthyBindingsReturnsTrueWhenAnyUnhealthy() {
        healthManager.recordHealthy("rms");
        healthManager.recordUnhealthy("claims", new RuntimeException("Error"));

        assertThat(healthManager.hasUnhealthyBindings()).isTrue();
    }

    @Test
    void hasUnhealthyBindingsReturnsFalseWhenAllHealthy() {
        healthManager.recordHealthy("rms");
        healthManager.recordHealthy("claims");

        assertThat(healthManager.hasUnhealthyBindings()).isFalse();
    }

    @Test
    void allBindingsHealthyOrStoppedHandlesMixedStates() {
        healthManager.recordHealthy("rms");
        healthManager.recordStopped("claims");

        assertThat(healthManager.allBindingsHealthyOrStopped()).isTrue();
    }

    @Test
    void allBindingsHealthyOrStoppedReturnsFalseWithUnhealthy() {
        healthManager.recordHealthy("rms");
        healthManager.recordUnhealthy("claims", new RuntimeException("Error"));

        assertThat(healthManager.allBindingsHealthyOrStopped()).isFalse();
    }

    @Test
    void consecutiveFailuresTracked() {
        healthManager.recordUnhealthy("rms", new RuntimeException("Error 1"));
        healthManager.recordUnhealthy("rms", new RuntimeException("Error 2"));
        healthManager.recordUnhealthy("rms", new RuntimeException("Error 3"));

        var snapshot = healthManager.getHealthSnapshot("rms");
        assertThat(snapshot.getConsecutiveFailures()).isEqualTo(3);
    }

    @Test
    void healthyResetsConsecutiveFailures() {
        healthManager.recordUnhealthy("rms", new RuntimeException("Error"));
        healthManager.recordUnhealthy("rms", new RuntimeException("Error"));
        healthManager.recordHealthy("rms");

        var snapshot = healthManager.getHealthSnapshot("rms");
        assertThat(snapshot.getConsecutiveFailures()).isEqualTo(0);
    }

    @Test
    void snapshotCapturesLastError() {
        RuntimeException error = new RuntimeException("Test error message");
        healthManager.recordUnhealthy("rms", error);

        var snapshot = healthManager.getHealthSnapshot("rms");
        assertThat(snapshot.getLastError()).isEqualTo(error);
    }

    @Test
    void snapshotCapturesDegradedReason() {
        healthManager.recordDegraded("claims", "Bisecting to isolate poison");

        var snapshot = healthManager.getHealthSnapshot("claims");
        assertThat(snapshot.getDegradedReason()).isEqualTo("Bisecting to isolate poison");
    }

    @Test
    void getAllStatusesReturnsAllBindings() {
        healthManager.recordHealthy("rms");
        healthManager.recordDegraded("claims", "test");
        healthManager.recordUnhealthy("other", new RuntimeException("error"));

        Map<String, BindingHealthManager.HealthStatus> statuses = healthManager.getAllStatuses();

        assertThat(statuses).hasSize(3);
        assertThat(statuses.get("rms")).isEqualTo(BindingHealthManager.HealthStatus.HEALTHY);
        assertThat(statuses.get("claims")).isEqualTo(BindingHealthManager.HealthStatus.DEGRADED);
        assertThat(statuses.get("other")).isEqualTo(BindingHealthManager.HealthStatus.UNHEALTHY);
    }

    @Test
    void getHealthSnapshotReturnsNullForUnknownBinding() {
        assertThat(healthManager.getHealthSnapshot("nonexistent")).isNull();
    }
}
