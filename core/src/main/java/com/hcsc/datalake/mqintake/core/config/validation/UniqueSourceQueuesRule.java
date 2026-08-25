package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Two bindings must not consume the same queue on the same queue manager.
 *
 * <p>A source queue is identified by (mq-connection, queue name), not by name
 * alone. The same queue name legitimately exists on more than one queue
 * manager — a feed spread across an HA or load-shared pair presents the same
 * queue on each, and both must be consumed. Keying on the name alone would
 * reject that topology at startup as a false duplicate.
 */
public class UniqueSourceQueuesRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (BindingConfig binding : properties.getBindings()) {
            if (binding.getSourceQueue() == null) {
                continue;
            }
            String key = binding.getMqConnection() + "::" + binding.getSourceQueue();
            if (!seen.add(key)) {
                errors.add("Duplicate source queue: " + binding.getSourceQueue() +
                        " on mq-connection '" + binding.getMqConnection() +
                        "' (binding: " + binding.getId() + ")");
            }
        }
        return errors;
    }
}
