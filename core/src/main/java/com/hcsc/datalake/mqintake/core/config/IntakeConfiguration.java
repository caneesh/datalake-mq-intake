package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Enables intake configuration properties.
 */
@Configuration
@EnableConfigurationProperties(IntakeProperties.class)
public class IntakeConfiguration {

    /**
     * Refuses to start when a pre-grouping binding key is still configured
     * anywhere (YAML, system property, environment variable). Runs at context
     * refresh, before any listener starts, so a stale override fails loudly
     * instead of being silently ignored.
     */
    @Autowired
    void detectLegacyBindingKeys(ConfigurableEnvironment environment) {
        LegacyBindingKeyDetector.failOnLegacyKeys(environment);
    }

    /**
     * Single binding health manager shared by the runtime (which records
     * transitions) and the actuator health indicator (which reports them).
     */
    @Bean
    public BindingHealthManager bindingHealthManager() {
        return new BindingHealthManager();
    }
}
