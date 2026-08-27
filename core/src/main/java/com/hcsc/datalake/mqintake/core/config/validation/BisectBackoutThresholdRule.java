package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;

import java.util.ArrayList;
import java.util.List;

/**
 * BISECT and BOTHRESH must be compatible, or clean messages get misrouted.
 *
 * <p>While a poison message is bisected toward isolation, the clean messages
 * sharing its failing batches accumulate backout count too. A clean message can
 * share up to ceil(log2(batch_size)) failing deliveries before the poison is
 * alone, so its delivery count can legitimately reach
 * ceil(log2(batch_size)) + 1. If the threshold sits at or below that, isolating
 * one bad message sends good ones to the backout queue with it.
 */
public class BisectBackoutThresholdRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();

        for (BindingConfig binding : properties.getBindings()) {
            if (binding.getDegradation().getStrategy() != DegradationStrategy.BISECT) {
                continue;
            }
            boolean hasBackoutQueue = binding.getBackout().getQueue() != null
                    && !binding.getBackout().getQueue().isBlank();
            if (!hasBackoutQueue || binding.getBatch().getSize() <= 1) {
                continue;
            }

            int minThreshold = minimumThresholdFor(binding.getBatch().getSize());
            if (binding.getBackout().getThreshold() < minThreshold) {
                errors.add("Binding '" + binding.getId() + "' uses BISECT with batch_size "
                        + binding.getBatch().getSize() + " but backout_threshold "
                        + binding.getBackout().getThreshold() + " < required minimum " + minThreshold
                        + " (ceil(log2(batch_size)) + 1) — clean messages sharing failing batches "
                        + "with a poison message would be misrouted to the backout queue");
            }
        }
        return errors;
    }

    /** ceil(log2(batchSize)) + 1 — the deepest a clean message can be dragged. */
    public static int minimumThresholdFor(int batchSize) {
        int bisectDepth = 32 - Integer.numberOfLeadingZeros(batchSize - 1);
        return bisectDepth + 1;
    }
}
