package com.hcsc.datalake.mqintake.core.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Enables intake configuration properties.
 */
@Configuration
@EnableConfigurationProperties(IntakeProperties.class)
public class IntakeConfiguration {
}
