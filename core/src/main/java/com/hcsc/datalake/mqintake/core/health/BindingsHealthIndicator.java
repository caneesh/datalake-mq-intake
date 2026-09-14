package com.hcsc.datalake.mqintake.core.health;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager.BindingHealthSnapshot;
import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager.HealthStatus;
import com.hcsc.datalake.mqintake.core.security.KerberosManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Spring Boot Actuator HealthIndicator for MQ intake bindings.
 *
 * <p>Reports aggregate health based on all binding statuses:
 * <ul>
 *   <li>UP: all bindings HEALTHY or STOPPED</li>
 *   <li>DOWN: <em>every</em> binding UNHEALTHY — the process genuinely has
 *       nothing consuming, and a restart is justified — or the Kerberos
 *       login can no longer be renewed, which will stop every binding from
 *       landing once the ticket lapses</li>
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
 * <p><strong>Why Kerberos is here:</strong> a relogin failure is not
 * binding-scoped. The keytab is shared, the ticket is shared, and once it
 * expires every write fails with the same infrastructure error — batches roll
 * back, nothing reaches the backout queue, and the queue backs up. Until now
 * the only signal was an hourly ERROR line and a gauge nothing scraped.
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
    private final KerberosManager kerberosManager;   // null when Kerberos is disabled

    public BindingsHealthIndicator(BindingHealthManager healthManager) {
        this(healthManager, null);
    }

    @Autowired
    public BindingsHealthIndicator(BindingHealthManager healthManager,
                                   @Autowired(required = false) KerberosManager kerberosManager) {
        this.healthManager = healthManager;
        this.kerberosManager = kerberosManager;
    }

    @Override
    public Health health() {
        Map<String, HealthStatus> statuses = healthManager.getAllStatuses();

        if (statuses.isEmpty()) {
            return withKerberos(Health.unknown()
                    .withDetail("message", "No bindings registered"));
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

        Health.Builder builder;
        if (allUnhealthy) {
            builder = Health.down();
        } else if (hasUnhealthy) {
            builder = Health.status(PARTIAL_OUTAGE);
        } else if (hasDegradedOrRecovering) {
            builder = Health.status(DEGRADED);
        } else {
            builder = Health.up();
        }
        return withKerberos(builder.withDetails(details));
    }

    /**
     * Overrides the aggregate to DOWN when the Kerberos login cannot be
     * renewed. Ticket expiry stops every binding from landing, so this is
     * the one process-wide failure a restart (with a fixed keytab) actually
     * addresses.
     */
    private Health withKerberos(Health.Builder builder) {
        if (kerberosManager == null) {
            return builder.build();
        }
        Map<String, Object> kerberos = new LinkedHashMap<>();
        boolean healthy = kerberosManager.isHealthy();
        kerberos.put("status", healthy ? "UP" : "DOWN");
        kerberos.put("principal", kerberosManager.getPrincipal());
        kerberos.put("reloginFailures", kerberosManager.getReloginFailureCount());
        long last = kerberosManager.getLastSuccessfulRelogin();
        kerberos.put("lastSuccessfulRelogin",
                last > 0 ? Instant.ofEpochMilli(last).toString() : "never");
        if (!healthy) {
            kerberos.put("reason", "TGT could not be renewed from the keytab within two "
                    + "relogin intervals; HDFS writes will fail once the ticket expires");
            builder.down();
        }
        return builder.withDetail("kerberos", kerberos).build();
    }
}
