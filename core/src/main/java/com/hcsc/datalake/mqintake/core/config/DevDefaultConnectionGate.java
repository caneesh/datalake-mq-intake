package com.hcsc.datalake.mqintake.core.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Refuses production startup on the known dev-placeholder MQ connection
 * values.
 *
 * <p>The YAML defaults are {@code ${MQ_HOST:localhost}},
 * {@code ${MQ_QUEUE_MANAGER:QM1}}, {@code ${MQ_CHANNEL:DEV.APP.SVRCONN}} —
 * convenient locally, and exactly what a production manifest resolves to when
 * someone forgets to set the variables. The sanity rules only reject
 * <em>blank</em> values, so that mistake used to sail through validation and
 * the service would attempt a production run against a local/dev queue
 * manager over IBM MQ's built-in unauthenticated dev channel, instead of
 * failing loudly. Same posture as the placeholder-serializer and
 * tracker-contract gates: dev conveniences are fine anywhere except a
 * production-mode boot.
 */
public final class DevDefaultConnectionGate {

    private static final Set<String> DEV_HOSTS = Set.of("localhost", "127.0.0.1");
    private static final Set<String> DEV_QUEUE_MANAGERS = Set.of("QM1");
    private static final Set<String> DEV_CHANNELS = Set.of("DEV.APP.SVRCONN", "DEV.ADMIN.SVRCONN");

    private DevDefaultConnectionGate() {
    }

    /**
     * @throws IllegalStateException in production mode when any connection
     *         still carries a dev-placeholder host, queue manager, or channel
     */
    public static void failOnDevDefaults(ProductionMode productionMode,
                                         Map<String, MqConnectionConfig> connections) {
        if (!productionMode.isEnabled()) {
            return;
        }

        List<String> offenders = new ArrayList<>();
        for (Map.Entry<String, MqConnectionConfig> entry : connections.entrySet()) {
            MqConnectionConfig config = entry.getValue();
            if (config.getHost() != null
                    && DEV_HOSTS.contains(config.getHost().toLowerCase(Locale.ROOT))) {
                offenders.add(entry.getKey() + ".host = " + config.getHost());
            }
            if (config.getQueueManager() != null
                    && DEV_QUEUE_MANAGERS.contains(config.getQueueManager())) {
                offenders.add(entry.getKey() + ".queue-manager = " + config.getQueueManager());
            }
            if (config.getChannel() != null
                    && DEV_CHANNELS.contains(config.getChannel().toUpperCase(Locale.ROOT))) {
                offenders.add(entry.getKey() + ".channel = " + config.getChannel());
            }
        }

        if (!offenders.isEmpty()) {
            throw new IllegalStateException(
                    "Production mode is enabled but MQ connection settings still carry "
                            + "dev-placeholder defaults — the deployment manifest is probably "
                            + "missing MQ_HOST / MQ_QUEUE_MANAGER / MQ_CHANNEL:\n  - "
                            + String.join("\n  - ", offenders));
        }
    }
}
