package com.hcsc.datalake.mqintake.core.orchestration;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;

/**
 * Factory for creating TrackerMessageBuilder instances.
 *
 * <p>Implementations are provided by binding-specific modules (rms).
 * Only TRACKED mode bindings require a tracker builder.
 */
@FunctionalInterface
public interface TrackerMessageBuilderFactory {

    /**
     * Creates a TrackerMessageBuilder for the given binding.
     *
     * @param config the binding configuration
     * @return the builder
     */
    TrackerMessageBuilder create(BindingConfig config);
}
