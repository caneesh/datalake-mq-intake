package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * The tracker queue must match the binding's mode.
 *
 * <p>A TRACKED binding without one cannot fulfil its contract; a LAND_ONLY
 * binding with one is a configuration mistake that would otherwise be silently
 * ignored, leaving an operator believing trackers are being sent.
 */
public class TrackerQueueConsistencyRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        for (BindingConfig binding : properties.getBindings()) {
            if (binding.getMode() == null) {
                continue;   // RequiredFieldsRule reports the missing mode
            }

            boolean hasTrackerQueue = binding.getTrackerQueue() != null
                    && !binding.getTrackerQueue().isBlank();

            if (binding.getMode() == BindingMode.TRACKED && !hasTrackerQueue) {
                errors.add("TRACKED binding '" + binding.getId() + "' requires a tracker_queue");
            }
            if (binding.getMode() == BindingMode.LAND_ONLY && hasTrackerQueue) {
                errors.add("LAND_ONLY binding '" + binding.getId()
                        + "' must not configure a tracker_queue");
            }
        }
        return errors;
    }
}
