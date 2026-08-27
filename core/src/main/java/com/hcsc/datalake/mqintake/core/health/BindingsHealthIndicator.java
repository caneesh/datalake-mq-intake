package com.hcsc.datalake.mqintake.core.health;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager.BindingHealthSnapshot;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager.HealthStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Spring Boot Actuator HealthIndicator for MQ intake bindings.
 *
 * <p>Reports aggregate health based on all binding statuses:
 * <ul>
 *   <li>UP: all bindings HEALTHY or STOPPED</li>
 *   <li>DOWN: <em>every</em> binding UNHEALTHY — the process genuinely has
 *       nothing consuming, and a restart is justified</li>
 *   <li>PARTIAL_OUTAGE: some bindings UNHEALTHY while others still work</li>
 *   <li>DEGRADED: no binding UNHEALTHY, but some DEGRADED or RECOVERING</li>
 * </ul>
 *
 * <p><strong>Why one failed binding must not read as DOWN:</strong> DOWN maps
 * to HTTP 503, which orchestrators treat as restart-the-pod. Binding isolation
 * is a core design property — a Claims failure is contained at the processing
 * layer, and reporting it as a whole-service outage would get the pod
 * restarted, interrupting the healthy RMS binding to "fix" a problem that was
 * already isolated. PARTIAL_OUTAGE and DEGRADED are mapped to HTTP 200 in
 * application.yml; alerting reads the status string and the per-binding
 * metrics, not the HTTP code.
 *
 * <p>Individual binding status is included in the details.
 *
 * <p>Endpoint: /actuator/health/bindings (when actuator is enabled)
 */
@Component("bindingsHealthIndicator")
public class BindingsHealthIndicator implements HealthIndicator {

    /** Some bindings are down, others are still consuming. HTTP 200 by mapping. */
    public static final String PARTIAL_OUTAGE = "PARTIAL_OUTAGE";

    /** Reduced capacity (degraded batch size, lost listener), still consuming. */
    public static final String DEGRADED = "DEGRADED";

    private final BindingHealthManager healthManager;

    public BindingsHealthIndicator(BindingHealthManager healthManager) {
        this.healthManager = healthManager;
    }

    @Override
    public Health health() {
        Map<String, HealthStatus> statuses = healthManager.getAllStatuses();

        if (statuses.isEmpty()) {
            return Health.unknown()
                    .withDetail("message", "No bindings registered")
                    .build();
        }

        // Build details map with per-binding status
        Map<String, Object> details = new LinkedHashMap<>();
        boolean hasUnhealthy = false;
        boolean hasDegradedOrRecovering = false;

        for (Map.Entry<String, HealthStatus> entry : statuses.entrySet()) {
            String bindingId = entry.getKey();
            HealthStatus status = entry.getValue();

            Map<String, Object> bindingDetails = new LinkedHashMap<>();
            bindingDetails.put("status", status.name());

            BindingHealthSnapshot snapshot = healthManager.getHealthSnapshot(bindingId);
            if (snapshot != null) {
                if (snapshot.getLastHealthyTime() != null) {
                    bindingDetails.put("lastHealthy", snapshot.getLastHealthyTime().toString());
                }
                if (snapshot.getConsecutiveFailures() > 0) {
                    bindingDetails.put("consecutiveFailures", snapshot.getConsecutiveFailures());
                }
                if (snapshot.getLastError() != null) {
                    bindingDetails.put("lastError", snapshot.getLastError().getMessage());
                }
                if (snapshot.getDegradedReason() != null &&
                        (status == HealthStatus.DEGRADED || status == HealthStatus.RECOVERING)) {
                    bindingDetails.put("reason", snapshot.getDegradedReason());
                }
            }

            details.put(bindingId, bindingDetails);

            if (status == HealthStatus.UNHEALTHY) {
                hasUnhealthy = true;
            } else if (status == HealthStatus.DEGRADED || status == HealthStatus.RECOVERING) {
                hasDegradedOrRecovering = true;
            }
        }

        // Determine aggregate health. DOWN is reserved for the case where a
        // restart could actually help: nothing at all is consuming.
        boolean allUnhealthy = statuses.values().stream()
                .allMatch(s -> s == HealthStatus.UNHEALTHY);

        if (allUnhealthy) {
            return Health.down()
                    .withDetails(details)
                    .build();
        } else if (hasUnhealthy) {
            return Health.status(PARTIAL_OUTAGE)
                    .withDetails(details)
                    .build();
        } else if (hasDegradedOrRecovering) {
            return Health.status(DEGRADED)
                    .withDetails(details)
                    .build();
        } else {
            return Health.up()
                    .withDetails(details)
                    .build();
        }
    }
}
