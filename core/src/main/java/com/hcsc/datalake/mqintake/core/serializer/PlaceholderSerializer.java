package com.hcsc.datalake.mqintake.core.serializer;

/**
 * Marker interface for placeholder/non-contractual serializers.
 *
 * <p>From DESIGN.md §9.1: Placeholder serializers are for development and testing
 * only. They produce files that MUST NOT be used by downstream consumers.
 *
 * <p>Any serializer implementing this interface will be rejected at startup
 * when running in production mode. Production mode is determined by:
 * <ul>
 *   <li>Spring profile "production" is active, OR</li>
 *   <li>Environment variable MQ_INTAKE_PRODUCTION=true</li>
 * </ul>
 *
 * <p>To use a placeholder serializer in test/dev environments:
 * <ul>
 *   <li>Do NOT activate the "production" profile</li>
 *   <li>Ensure MQ_INTAKE_PRODUCTION is not set or is false</li>
 * </ul>
 */
public interface PlaceholderSerializer {

    /**
     * Returns a description of why this serializer is a placeholder.
     * Used in error messages when startup fails.
     */
    default String getPlaceholderReason() {
        return "Serializer is marked as placeholder - contract not finalized";
    }
}
