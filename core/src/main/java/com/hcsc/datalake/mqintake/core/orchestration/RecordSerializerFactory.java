package com.hcsc.datalake.mqintake.core.orchestration;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;

/**
 * Factory for creating RecordSerializer instances.
 *
 * <p>Implementations are provided by binding-specific modules (rms, claims).
 * The core module has no knowledge of specific serialization formats.
 */
@FunctionalInterface
public interface RecordSerializerFactory {

    /**
     * Creates a RecordSerializer for the given binding.
     *
     * @param config the binding configuration
     * @return the serializer
     */
    RecordSerializer create(BindingConfig config);
}
