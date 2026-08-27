package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.mq.CredentialProvider;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Configures MQ connections for the intake service.
 *
 * <p>Uses named connections from {@code intake.mq-connections} map.
 * Each binding references a named connection via {@code mq-connection}.
 *
 * <p>JMS Connection is thread-safe and shared. Each listener thread
 * creates its own transacted Session.
 */
@Configuration
public class MqConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MqConfiguration.class);

    @Bean(destroyMethod = "close")
    public MqConnectionManager mqConnectionManager(IntakeProperties properties,
                                                    CredentialProvider credentialProvider) {
        Map<String, MqConnectionConfig> connections = properties.getMqConnections();

        if (connections.isEmpty()) {
            log.warn("No MQ connections configured in intake.mq-connections");
        } else {
            log.info("Configured {} MQ connection(s): {}", connections.size(), connections.keySet());
        }

        validateConnectionIds(connections);

        return new MqConnectionManager(connections, credentialProvider);
    }

    @Bean
    public BindingConfigurationValidator bindingConfigurationValidator(IntakeProperties properties,
                                                                        MqConnectionManager connectionManager) {
        return new BindingConfigurationValidator(properties, connectionManager);
    }

    private void validateConnectionIds(Map<String, MqConnectionConfig> connections) {
        Set<String> ids = new HashSet<>();
        for (Map.Entry<String, MqConnectionConfig> entry : connections.entrySet()) {
            String mapKey = entry.getKey();
            MqConnectionConfig config = entry.getValue();

            if (config.getId() == null || config.getId().isBlank()) {
                config.setId(mapKey);
            }

            String id = config.getId();
            if (!ids.add(id)) {
                throw new IllegalStateException("Duplicate MQ connection id: " + id);
            }
        }
    }

    /**
     * Validates binding configurations at startup.
     */
    public static class BindingConfigurationValidator {

        private final IntakeProperties properties;
        private final MqConnectionManager connectionManager;

        public BindingConfigurationValidator(IntakeProperties properties,
                                              MqConnectionManager connectionManager) {
            this.properties = properties;
            this.connectionManager = connectionManager;
        }

        @PostConstruct
        public void validate() {
            for (BindingConfig binding : properties.getBindings()) {
                validateBinding(binding);
            }
        }

        private void validateBinding(BindingConfig binding) {
            String mqConn = binding.getMqConnection();
            if (mqConn == null || mqConn.isBlank()) {
                throw new IllegalStateException(
                        "Binding '" + binding.getId() + "' must specify mq-connection");
            }

            if (!connectionManager.hasConnection(mqConn)) {
                throw new IllegalStateException(
                        "Binding '" + binding.getId() + "' references unknown mq-connection: " + mqConn);
            }

            if (binding.getMode() == BindingMode.TRACKED) {
                if (binding.getTracker().getQueue() == null || binding.getTracker().getQueue().isBlank()) {
                    throw new IllegalStateException(
                            "TRACKED binding '" + binding.getId() + "' requires tracker.queue");
                }
            }

            log.debug("Validated binding '{}' → mq-connection '{}' (mode={})",
                    binding.getId(), mqConn, binding.getMode());
        }
    }
}
