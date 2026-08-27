package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Every configured MQ connection must actually be connectable-shaped.
 *
 * <p>Nothing validated these before. A blank host, queue manager or channel —
 * the natural result of an unset environment variable whose interpolation
 * default is empty — sailed through every startup check and only failed when
 * {@code MQConnectionFactory} was handed the blanks at first real connect,
 * long after "configuration validated successfully" had been logged.
 *
 * <p>{@code receive-timeout-ms} is the sharpest of these: per the JMS spec,
 * {@code receive(0)} means <em>wait forever</em>, not "no wait". The receive
 * loop's idle branch — the one that notices partition boundaries on a quiet
 * queue — only runs when {@code receive()} returns null, which with a zero
 * timeout is never. An operator setting 0 with the obvious intuition would
 * silently starve the partition flush.
 */
public class MqConnectionSanityRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();

        for (Map.Entry<String, MqConnectionConfig> entry
                : properties.getMqConnections().entrySet()) {
            String name = entry.getKey();
            MqConnectionConfig config = entry.getValue();
            String prefix = "mq-connection '" + name + "'";

            requireText(config.getHost(), prefix, "host", errors);
            requireText(config.getQueueManager(), prefix, "queue-manager", errors);
            requireText(config.getChannel(), prefix, "channel", errors);

            if (config.getReceiveTimeoutMs() <= 0) {
                errors.add(prefix + " receive-timeout-ms must be positive: receive(0) blocks "
                        + "forever per the JMS spec, which starves the partition-boundary "
                        + "flush on a quiet queue");
            }
            if (config.getReconnectAttempts() <= 0) {
                errors.add(prefix + " reconnect-attempts must be positive");
            }
            if (config.getReconnectDelayMs() < 0) {
                errors.add(prefix + " reconnect-delay-ms must not be negative");
            }
        }
        return errors;
    }

    private void requireText(String value, String prefix, String field, List<String> errors) {
        if (value == null || value.isBlank()) {
            errors.add(prefix + " missing required field: " + field
                    + " (an unset environment variable with an empty default produces "
                    + "exactly this)");
        }
    }
}
