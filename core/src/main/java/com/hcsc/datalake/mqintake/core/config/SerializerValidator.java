package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Validates that serializers are production-ready.
 *
 * <p>From DESIGN.md §9.1: Placeholder serializers must NOT be used in production.
 * They produce files with non-contractual output that downstream consumers
 * cannot rely on.
 *
 * <p>Production mode is decided by {@link ProductionMode} — an active
 * {@code prod}/{@code production} Spring profile, or
 * {@code MQ_INTAKE_PRODUCTION=true}.
 *
 * <p>When production mode is enabled, any binding using a placeholder serializer
 * causes startup to fail with a clear error message.
 */
public class SerializerValidator {

    private static final Logger log = LoggerFactory.getLogger(SerializerValidator.class);

    private final RecordSerializerFactory serializerFactory;
    private final boolean productionMode;

    public SerializerValidator(RecordSerializerFactory serializerFactory, boolean productionMode) {
        this.serializerFactory = Objects.requireNonNull(serializerFactory, "serializerFactory required");
        this.productionMode = productionMode;
    }

    /**
     * Creates a validator from the application's {@link ProductionMode}, which
     * is the only thing that decides this — see that class for why the answer
     * is not read from the environment here.
     */
    public SerializerValidator(RecordSerializerFactory serializerFactory,
                               ProductionMode productionMode) {
        this(serializerFactory,
                Objects.requireNonNull(productionMode, "productionMode required").isEnabled());
    }

    /**
     * Validates serializers for all bindings.
     *
     * @param bindings the bindings to validate
     * @return list of validation errors (empty if all pass)
     */
    public List<String> validateBindings(List<BindingConfig> bindings) {
        List<String> errors = new ArrayList<>();

        for (BindingConfig binding : bindings) {
            RecordSerializer serializer = serializerFactory.create(binding);
            String bindingId = binding.getId();

            if (serializer instanceof PlaceholderSerializer) {
                PlaceholderSerializer placeholder = (PlaceholderSerializer) serializer;
                String reason = placeholder.getPlaceholderReason();

                if (productionMode && binding.isAcceptPlaceholderSerializer()) {
                    // Accepted deliberately, for this binding only, with every
                    // other production gate still armed. Logged at error level
                    // on purpose: it belongs in whatever an operator reads
                    // first, not in the noise below it.
                    log.error("Binding '{}' is running in production with PLACEHOLDER serializer "
                                    + "{} because accept-placeholder-serializer is set: {}. Data "
                                    + "landed in a format that later changes must be reprocessed, "
                                    + "and without an identity field it cannot be reconciled or "
                                    + "de-duplicated.",
                            bindingId, serializer.getClass().getSimpleName(), reason);
                } else if (productionMode) {
                    String error = String.format(
                            "Binding '%s' uses placeholder serializer %s in production mode: %s. "
                                    + "Finalise the serializer, or set "
                                    + "intake.bindings[].accept-placeholder-serializer=true to "
                                    + "accept the consequences for this binding — which is a "
                                    + "narrower thing to do than disarming production mode.",
                            bindingId, serializer.getClass().getSimpleName(), reason);
                    errors.add(error);
                    log.error(error);
                } else {
                    log.warn("Binding '{}' uses PLACEHOLDER serializer {} — NOT FOR PRODUCTION: {}",
                            bindingId, serializer.getClass().getSimpleName(), reason);
                }
            } else {
                log.info("Binding '{}' uses production serializer: {}",
                        bindingId, serializer.getClass().getSimpleName());
            }
        }

        return errors;
    }

    /**
     * Validates serializers and throws if any validation fails in production mode.
     *
     * @param bindings the bindings to validate
     * @throws SerializerValidationException if placeholder serializers used in production
     */
    public void validateOrFail(List<BindingConfig> bindings) throws SerializerValidationException {
        List<String> errors = validateBindings(bindings);

        if (!errors.isEmpty()) {
            String message = "Serializer validation failed (production mode):\n  - " +
                    String.join("\n  - ", errors);
            log.error(message);
            throw new SerializerValidationException(message, errors);
        }

        if (productionMode) {
            log.info("Serializer validation passed for {} bindings in PRODUCTION mode", bindings.size());
        } else {
            log.info("Serializer validation passed for {} bindings in non-production mode", bindings.size());
        }
    }

    public boolean isProductionMode() {
        return productionMode;
    }

    /**
     * Exception thrown when serializer validation fails.
     */
    public static class SerializerValidationException extends Exception {
        private final List<String> errors;

        public SerializerValidationException(String message, List<String> errors) {
            super(message);
            this.errors = List.copyOf(errors);
        }

        public List<String> getErrors() {
            return errors;
        }
    }
}
