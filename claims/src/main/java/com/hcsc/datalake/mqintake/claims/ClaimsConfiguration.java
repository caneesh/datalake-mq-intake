package com.hcsc.datalake.mqintake.claims;

import com.hcsc.datalake.mqintake.claims.serializer.ClaimsRecordSerializer;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Claims-specific configuration for record serialization.
 *
 * <p>LAND_ONLY mode — no TrackerMessageBuilderFactory needed.
 */
@Configuration
public class ClaimsConfiguration {

    @Bean
    public RecordSerializerFactory recordSerializerFactory() {
        return config -> new ClaimsRecordSerializer();
    }
}
