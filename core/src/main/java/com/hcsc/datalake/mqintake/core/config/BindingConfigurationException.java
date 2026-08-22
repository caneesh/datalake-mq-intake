package com.hcsc.datalake.mqintake.core.config;

/**
 * Thrown when binding configuration validation fails.
 * This is a startup-blocking error — no listeners should start.
 */
public class BindingConfigurationException extends RuntimeException {

    public BindingConfigurationException(String message) {
        super(message);
    }

    public BindingConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
