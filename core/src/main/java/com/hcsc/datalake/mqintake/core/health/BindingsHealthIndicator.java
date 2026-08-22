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
 *   <li>DOWN: any binding UNHEALTHY</li>
 *   <li>OUT_OF_SERVICE: any binding DEGRADED or RECOVERING (but none UNHEALTHY)</li>
 * </ul>
 *
 * <p>Individual binding status is included in the details.
 *
 * <p>Endpoint: /actuator/health/bindings (when actuator is enabled)
 */
@Component("bindingsHealthIndicator")
public class BindingsHealthIndicator implements HealthIndicator {

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

        // Determine aggregate health
        if (hasUnhealthy) {
            return Health.down()
                    .withDetails(details)
                    .build();
        } else if (hasDegradedOrRecovering) {
            return Health.status("OUT_OF_SERVICE")
                    .withDetails(details)
                    .build();
        } else {
            return Health.up()
                    .withDetails(details)
                    .build();
        }
    }
}
