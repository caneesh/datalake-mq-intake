package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.List;

/** A backout queue is useless without a positive threshold to trigger it. */
public class BackoutQueueConsistencyRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        for (BindingConfig binding : properties.getBindings()) {
            boolean hasBackoutQueue = binding.getBackoutQueue() != null
                    && !binding.getBackoutQueue().isBlank();

            if (hasBackoutQueue && binding.getBackoutThreshold() <= 0) {
                errors.add("Binding '" + binding.getId()
                        + "' has backout_queue configured but backout_threshold is not positive");
            }
            if (binding.getSuccessesRequiredToRestore() <= 0) {
                errors.add("Binding '" + binding.getId()
                        + "' successes_required_to_restore must be positive");
            }
        }
        return errors;
    }
}
