package com.hcsc.datalake.mqintake.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The single source of truth for whether the service is running in production.
 *
 * <p>Production mode turns the safety gates from warnings into startup
 * failures — the placeholder serializer gate (§9.1) and the RMS tracker
 * contract gate (§20.4). Getting the answer wrong in the permissive direction
 * means shipping non-contractual data to downstream consumers, so it is worth
 * having exactly one place that decides it.
 *
 * <p>It is enabled when <em>either</em>:
 * <ul>
 *   <li>an active Spring profile is {@code prod} or {@code production}, or</li>
 *   <li>the environment variable {@code MQ_INTAKE_PRODUCTION} is
 *       {@code true} (any case) or {@code 1}</li>
 * </ul>
 *
 * <p>Both were already documented as valid, but the gates only ever consulted
 * the environment variable. An application launched with
 * {@code --spring.profiles.active=prod} and no env var therefore ran with
 * every production check silently disabled — the exact combination most likely
 * in a container platform where profiles come from the deployment manifest.
 *
 * <p>The OR is deliberate. Either signal alone is enough to enable the gates,
 * and neither can switch them back off, so adding a profile can only ever make
 * the service stricter.
 */
@Component
public class ProductionMode {

    private static final Logger log = LoggerFactory.getLogger(ProductionMode.class);

    static final String ENV_PRODUCTION_MODE = "MQ_INTAKE_PRODUCTION";
    static final Set<String> PRODUCTION_PROFILES = Set.of("prod", "production");

    private final boolean enabled;
    private final String reason;

    @Autowired
    public ProductionMode(Environment environment) {
        this(environment == null ? new String[0] : environment.getActiveProfiles(),
                System.getenv(ENV_PRODUCTION_MODE));
    }

    /**
     * Visible for testing: the environment variable cannot be set from inside
     * a running JVM, so tests supply both inputs directly.
     */
    ProductionMode(String[] activeProfiles, String envValue) {
        Set<String> matched = new LinkedHashSet<>();
        if (activeProfiles != null) {
            Arrays.stream(activeProfiles)
                    .filter(p -> p != null && PRODUCTION_PROFILES.contains(p.trim().toLowerCase()))
                    .forEach(matched::add);
        }
        boolean byEnv = isTruthy(envValue);

        this.enabled = !matched.isEmpty() || byEnv;

        if (!enabled) {
            this.reason = "no production profile active and " + ENV_PRODUCTION_MODE + " not set";
        } else if (!matched.isEmpty() && byEnv) {
            this.reason = "Spring profile " + matched + " active and "
                    + ENV_PRODUCTION_MODE + "=" + envValue;
        } else if (!matched.isEmpty()) {
            this.reason = "Spring profile " + matched + " active";
        } else {
            this.reason = ENV_PRODUCTION_MODE + "=" + envValue;
        }

        if (enabled) {
            log.info("PRODUCTION MODE ENABLED ({}) — placeholder and contract gates will fail "
                    + "startup rather than warn", reason);
        } else {
            log.info("Production mode is off ({}) — safety gates warn instead of failing", reason);
        }
    }

    private static boolean isTruthy(String value) {
        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    /** Test factory for an explicitly production context. */
    public static ProductionMode enabled() {
        return new ProductionMode(new String[]{"prod"}, null);
    }

    /** Test factory for an explicitly non-production context. */
    public static ProductionMode disabled() {
        return new ProductionMode(new String[0], null);
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** Human-readable explanation of the decision, for startup logs and errors. */
    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ProductionMode{enabled=" + enabled + ", reason='" + reason + "'}";
    }
}
