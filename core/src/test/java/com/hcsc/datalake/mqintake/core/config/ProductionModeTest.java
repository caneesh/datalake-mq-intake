package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import javax.jms.Message;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for production-mode detection.
 *
 * <p>The gates previously consulted only {@code MQ_INTAKE_PRODUCTION}, so an
 * application started with {@code --spring.profiles.active=prod} ran with every
 * production check silently disabled. Both signals must now enable it.
 */
class ProductionModeTest {

    @Test
    void prodProfileEnablesProductionMode() {
        ProductionMode mode = new ProductionMode(new String[]{"prod"}, null);

        assertThat(mode.isEnabled()).isTrue();
        assertThat(mode.getReason()).contains("prod");
    }

    @Test
    void productionProfileEnablesProductionMode() {
        ProductionMode mode = new ProductionMode(new String[]{"production"}, null);

        assertThat(mode.isEnabled()).isTrue();
        assertThat(mode.getReason()).contains("production");
    }

    @Test
    void productionProfileIsRecognisedAlongsideOtherProfiles() {
        ProductionMode mode = new ProductionMode(new String[]{"kerberos", "prod", "metrics"}, null);

        assertThat(mode.isEnabled()).isTrue();
    }

    @Test
    void profileMatchIsCaseInsensitive() {
        assertThat(new ProductionMode(new String[]{"PROD"}, null).isEnabled()).isTrue();
        assertThat(new ProductionMode(new String[]{"Production"}, null).isEnabled()).isTrue();
    }

    @Test
    void environmentVariableEnablesProductionMode() {
        assertThat(new ProductionMode(new String[0], "true").isEnabled()).isTrue();
        assertThat(new ProductionMode(new String[0], "TRUE").isEnabled()).isTrue();
        assertThat(new ProductionMode(new String[0], "1").isEnabled()).isTrue();
    }

    @Test
    void eitherSignalAloneIsEnoughAndNeitherCanTurnItOff() {
        // The OR is deliberate: adding a signal can only make the service
        // stricter, never re-enable a gate that the other signal disabled.
        assertThat(new ProductionMode(new String[]{"prod"}, "false").isEnabled()).isTrue();
        assertThat(new ProductionMode(new String[]{"dev"}, "true").isEnabled()).isTrue();
        assertThat(new ProductionMode(new String[]{"prod"}, "true").isEnabled()).isTrue();
    }

    @Test
    void normalDevAndTestModeIsNotProduction() {
        assertThat(new ProductionMode(new String[0], null).isEnabled()).isFalse();
        assertThat(new ProductionMode(new String[]{"dev"}, null).isEnabled()).isFalse();
        assertThat(new ProductionMode(new String[]{"test"}, "false").isEnabled()).isFalse();
        assertThat(new ProductionMode(new String[]{"uat"}, "no").isEnabled()).isFalse();
    }

    @Test
    void readsActiveProfilesFromTheSpringEnvironment() {
        MockEnvironment environment = new MockEnvironment();
        environment.setActiveProfiles("prod");

        // Constructed the way Spring will construct it
        assertThat(new ProductionMode(environment).isEnabled()).isTrue();

        assertThat(new ProductionMode(new MockEnvironment()).isEnabled()).isFalse();
    }

    @Test
    void productionModeMakesPlaceholderValidationFail() {
        RecordSerializerFactory factory = config -> new TestPlaceholder();
        BindingConfig binding = new BindingConfig();
        binding.setId("some-binding");

        SerializerValidator gated = new SerializerValidator(
                factory, new ProductionMode(new String[]{"prod"}, null));
        assertThatThrownBy(() -> gated.validateOrFail(List.of(binding)))
                .isInstanceOf(SerializerValidator.SerializerValidationException.class);

        // Same serializer, non-production: warns rather than fails
        SerializerValidator ungated = new SerializerValidator(
                factory, new ProductionMode(new String[]{"dev"}, null));
        assertThat(ungated.validateBindings(List.of(binding))).isEmpty();
    }

    @Test
    void nullEnvironmentDoesNotBlowUp() {
        // Defensive: a context with no Environment must not fail construction
        assertThat(new ProductionMode((org.springframework.core.env.Environment) null).isEnabled())
                .isFalse();
    }

    private static class TestPlaceholder implements RecordSerializer, PlaceholderSerializer {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata) {
            return new SerializedRecord(new LongWritable(0), new Text(""));
        }

        @Override
        public Class<? extends Writable> getKeyClass() {
            return LongWritable.class;
        }

        @Override
        public Class<? extends Writable> getValueClass() {
            return Text.class;
        }

        @Override
        public String getPlaceholderReason() {
            return "test placeholder";
        }
    }
}
