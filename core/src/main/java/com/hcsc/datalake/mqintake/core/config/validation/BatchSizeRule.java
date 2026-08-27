package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch size must fit inside the queue manager's uncommitted-message limit.
 *
 * <p>A TRACKED binding's unit of work is 2N — every source get is paired with
 * a tracker put — so it may only use half of MAXUMSGS. Exceeding the limit
 * fails at commit under load, not at startup, which is the worst time to find
 * out.
 */
public class BatchSizeRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        int maxumsgs = properties.getMaxumsgs();

        for (BindingConfig binding : properties.getBindings()) {
            if (binding.getMode() == null) {
                continue;
            }

            int maxAllowed = binding.getMaxBatchSizeFor(maxumsgs);
            if (binding.getBatch().getSize() > maxAllowed) {
                String modeExplanation = binding.getMode() == BindingMode.TRACKED
                        ? "MAXUMSGS/2 = " + maxAllowed + " (unit of work is 2N)"
                        : "MAXUMSGS = " + maxAllowed;
                errors.add("Binding '" + binding.getId() + "' batch_size "
                        + binding.getBatch().getSize() + " exceeds " + modeExplanation);
            }

            if (binding.getBatch().getSize() <= 0) {
                errors.add("Binding '" + binding.getId() + "' batch_size must be positive");
            }
        }
        return errors;
    }
}
