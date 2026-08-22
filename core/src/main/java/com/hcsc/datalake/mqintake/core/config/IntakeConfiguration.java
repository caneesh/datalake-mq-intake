package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Enables intake configuration properties.
 */
@Configuration
@EnableConfigurationProperties(IntakeProperties.class)
public class IntakeConfiguration {

    /**
     * Single binding health manager shared by the runtime (which records
     * transitions) and the actuator health indicator (which reports them).
     */
    @Bean
    public BindingHealthManager bindingHealthManager() {
        return new BindingHealthManager();
    }
}
