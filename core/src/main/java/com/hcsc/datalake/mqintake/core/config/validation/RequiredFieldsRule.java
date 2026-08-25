package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Fields without which a binding cannot run, and numeric fields whose value
 * would be meaningless.
 *
 * <p>Errors are prefixed with the binding id where there is one, and with the
 * list index where there is not — a config missing an id is exactly the case
 * where "which binding?" is hardest to answer.
 */
public class RequiredFieldsRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        List<BindingConfig> bindings = properties.getBindings();

        for (int i = 0; i < bindings.size(); i++) {
            BindingConfig binding = bindings.get(i);
            String prefix = "Binding[" + i + "]";

            if (binding.getId() == null || binding.getId().isBlank()) {
                errors.add(prefix + " missing required field: id");
            } else {
                prefix = "Binding '" + binding.getId() + "'";
            }

            requireText(binding.getSourceQueue(), prefix, "source_queue", errors);
            requireText(binding.getHdfsBasePath(), prefix, "hdfs_base_path", errors);

            if (binding.getMode() == null) {
                errors.add(prefix + " missing required field: mode");
            }

            if (binding.getBatchBytes() <= 0) {
                errors.add(prefix + " batch_bytes must be positive");
            }
            if (binding.getListenerThreads() <= 0) {
                errors.add(prefix + " listener_threads must be positive");
            }

            // 0 disables the fixed timer, leaving the partition boundary as the
            // only time-based flush trigger (§7.1 cadence, matching the legacy
            // writer's roll-on-path-change behaviour). Negative is meaningless.
            if (binding.getBatchIntervalMs() < 0) {
                errors.add(prefix + " batch_interval_ms must not be negative "
                        + "(0 disables the fixed timer; the partition boundary still flushes)");
            }

            // 0 disables backout-depth sampling. Allowed, but it silently
            // removes the alert DESIGN §14 nominates as the pager condition,
            // so it should be a deliberate choice rather than a typo.
            if (binding.getBackoutDepthPollIntervalMs() < 0) {
                errors.add(prefix + " backout_depth_poll_interval_ms must not be negative "
                        + "(0 disables backout-depth monitoring and its alert)");
            }
        }
        return errors;
    }

    private void requireText(String value, String prefix, String field, List<String> errors) {
        if (value == null || value.isBlank()) {
            errors.add(prefix + " missing required field: " + field);
        }
    }
}
