package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.core.config.SerializerValidator;
import com.hcsc.datalake.mqintake.core.config.TrackerBodyMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.rms.serializer.RmsRecordSerializer;
import com.hcsc.datalake.mqintake.rms.tracker.RmsTrackerMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RMS-specific configuration for record serialization and tracker messages.
 *
 * <p><strong>Tracker contract gate (§20.4):</strong> The legacy
 * MessageHeaderDetails rewrite is not fully captured (tagList, root-end
 * constants, before/after fixture). Because RMS is TRACKED, production
 * startup FAILS while {@link RmsTrackerMessageBuilder#isTrackerContractReady()}
 * is false. Non-production environments run the placeholder rewrite with a
 * loud warning.
 */
@Configuration
public class RmsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(RmsConfiguration.class);

    private final boolean productionMode;

    public RmsConfiguration() {
        this(SerializerValidator.isProductionModeFromEnvironment());
    }

    /**
     * Visible-for-testing constructor with explicit production mode.
     */
    public RmsConfiguration(boolean productionMode) {
        this.productionMode = productionMode;
    }

    @Bean
    public RecordSerializerFactory recordSerializerFactory() {
        return config -> new RmsRecordSerializer();
    }

    @Bean
    public TrackerMessageBuilderFactory trackerMessageBuilderFactory() {
        validateTrackerContract();

        return config -> {
            TrackerBodyMode bodyMode = config.getTrackerBodyMode() != null
                    ? config.getTrackerBodyMode()
                    : TrackerBodyMode.FULL_COPY;

            RmsTrackerMessageBuilder.TrackerFields fields;
            if (config.getTrackerFields() != null) {
                fields = new RmsTrackerMessageBuilder.TrackerFields(
                        config.getTrackerFields().getReportingSystem(),
                        config.getTrackerFields().getSourceSystem(),
                        config.getTrackerFields().getMessageStatus(),
                        config.getTrackerFields().getDestinationStatus()
                );
            } else {
                fields = RmsTrackerMessageBuilder.TrackerFields.defaultRms();
            }

            return new RmsTrackerMessageBuilder(bodyMode, fields);
        };
    }

    /**
     * Enforces the §20.4 tracker contract gate: RMS TRACKED production startup
     * must fail while the legacy header rewrite is incomplete.
     *
     * @throws IllegalStateException in production mode when the contract is
     *                               incomplete
     */
    void validateTrackerContract() {
        if (RmsTrackerMessageBuilder.isTrackerContractReady()) {
            log.info("RMS tracker contract is complete — production tracker rewrite enabled");
            return;
        }

        String gaps = String.join("; ", RmsTrackerMessageBuilder.trackerContractGaps());

        if (productionMode) {
            throw new IllegalStateException(
                    "RMS tracker contract is INCOMPLETE — production startup blocked (§20.4). " +
                    "Missing legacy artifacts: " + gaps);
        }

        log.warn("RMS tracker contract INCOMPLETE — running PLACEHOLDER header rewrite. " +
                "NOT FOR PRODUCTION. Missing: {}", gaps);
    }
}
