package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Binding ids must be unique: they key metrics, health, temp paths and filenames. */
public class UniqueBindingIdsRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (BindingConfig binding : properties.getBindings()) {
            if (binding.getId() != null && !seen.add(binding.getId())) {
                errors.add("Duplicate binding id: " + binding.getId());
            }
        }
        return errors;
    }
}
