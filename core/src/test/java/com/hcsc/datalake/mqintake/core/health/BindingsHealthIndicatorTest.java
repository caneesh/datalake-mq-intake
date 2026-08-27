package com.hcsc.datalake.mqintake.core.health;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Aggregate health semantics — specifically the blast radius.
 *
 * <p>DOWN maps to HTTP 503, which orchestrators treat as restart-the-pod. For
 * a service whose whole point is binding isolation, one failed binding must
 * never read as a dead service: restarting the pod over an isolated Claims
 * failure would interrupt the healthy RMS binding to fix a problem that was
 * already contained.
 */
class BindingsHealthIndicatorTest {

    private BindingHealthManager manager;
    private BindingsHealthIndicator indicator;

    @BeforeEach
    void setUp() {
        manager = new BindingHealthManager();
        indicator = new BindingsHealthIndicator(manager);
    }

    @Test
    void allHealthyIsUp() {
        manager.recordHealthy("rms");
        manager.recordHealthy("claims");

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void oneUnhealthyAmongHealthyIsPartialOutageNotDown() {
        // The finding this test exists for: this used to return DOWN, handing
        // the orchestrator a 503 over a contained, single-binding failure.
        manager.recordHealthy("rms");
        manager.recordUnhealthy("claims", new RuntimeException("claims broke"));

        Health health = indicator.health();

        assertThat(health.getStatus().getCode())
                .isEqualTo(BindingsHealthIndicator.PARTIAL_OUTAGE);
        assertThat(health.getDetails()).containsKeys("rms", "claims");
    }

    @Test
    void everyBindingUnhealthyIsDown() {
        // The one case where a restart could actually help: nothing consumes.
        manager.recordUnhealthy("rms", new RuntimeException("x"));
        manager.recordUnhealthy("claims", new RuntimeException("y"));

        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void degradedWithoutUnhealthyIsDegradedNotOutOfService() {
        // OUT_OF_SERVICE also maps to 503 by default; a binding at reduced
        // batch size is impaired, not gone.
        manager.recordHealthy("rms");
        manager.recordDegraded("claims", "bisecting a poison message");

        assertThat(indicator.health().getStatus().getCode())
                .isEqualTo(BindingsHealthIndicator.DEGRADED);
    }

    @Test
    void unhealthyOutranksDegraded() {
        manager.recordDegraded("rms", "reduced batch");
        manager.recordUnhealthy("claims", new RuntimeException("z"));

        assertThat(indicator.health().getStatus().getCode())
                .isEqualTo(BindingsHealthIndicator.PARTIAL_OUTAGE);
    }

    @Test
    void noBindingsIsUnknown() {
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void stoppedBindingsDoNotDragTheStatusDown() {
        manager.recordHealthy("rms");
        manager.recordStopped("claims");

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }
}
