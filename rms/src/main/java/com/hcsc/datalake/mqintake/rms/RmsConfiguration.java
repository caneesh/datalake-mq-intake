package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.TrackerBodyMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import com.hcsc.datalake.mqintake.rms.serializer.RmsRecordSerializer;
import com.hcsc.datalake.mqintake.rms.tracker.RmsTrackerMessageBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RMS-specific configuration for record serialization and tracker messages.
 */
@Configuration
public class RmsConfiguration {

    @Bean
    public RecordSerializerFactory recordSerializerFactory() {
        return config -> new RmsRecordSerializer();
    }

    @Bean
    public TrackerMessageBuilderFactory trackerMessageBuilderFactory() {
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
}
