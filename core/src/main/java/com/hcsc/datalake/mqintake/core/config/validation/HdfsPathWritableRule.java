package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.HdfsPathValidator;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Every binding's landing path must be writable before any message is consumed. */
public class HdfsPathWritableRule implements BindingConfigRule {

    private final HdfsPathValidator pathValidator;

    public HdfsPathWritableRule(HdfsPathValidator pathValidator) {
        this.pathValidator = Objects.requireNonNull(pathValidator, "pathValidator required");
    }

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();

        for (BindingConfig binding : properties.getBindings()) {
            if (binding.getHdfsBasePath() == null || binding.getHdfsBasePath().isBlank()) {
                continue;   // RequiredFieldsRule reports the missing path
            }

            HdfsPathValidator.PathValidationResult result =
                    pathValidator.validatePath(binding.getHdfsBasePath());

            if (!result.isValid()) {
                errors.add("Binding '" + binding.getId() + "' HDFS path not writable: "
                        + result.getError());
            }
        }
        return errors;
    }
}
