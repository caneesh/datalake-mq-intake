package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer.SerializedRecord;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.*;

import javax.jms.Message;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for SerializerValidator.
 */
class SerializerValidatorTest {

    @Test
    void productionModeRejectsPlaceholderSerializer() {
        RecordSerializerFactory factory = config -> new TestPlaceholderSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        BindingConfig config = createTestConfig("test-binding");

        List<String> errors = validator.validateBindings(List.of(config));

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("placeholder");
        assertThat(errors.get(0)).contains("test-binding");
        assertThat(errors.get(0)).contains("production mode");
    }

    @Test
    void theRefusalNamesTheNarrowerWayOut() {
        // An operator told only "refused" reaches for MQ_INTAKE_PRODUCTION=false,
        // which also switches off the local-filesystem refusal — trading a
        // format limitation for the risk of landing data on the wrong disk.
        RecordSerializerFactory factory = config -> new TestPlaceholderSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        List<String> errors = validator.validateBindings(List.of(createTestConfig("test-binding")));

        assertThat(errors.get(0)).contains("accept-placeholder-serializer");
    }

    @Test
    void anExplicitlyAcceptedPlaceholderIsAllowedInProduction() {
        RecordSerializerFactory factory = config -> new TestPlaceholderSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        BindingConfig config = createTestConfig("test-binding");
        config.setAcceptPlaceholderSerializer(true);

        assertThat(validator.validateBindings(List.of(config))).isEmpty();
    }

    @Test
    void acceptanceAppliesToOneBindingOnly() {
        // The escape is per binding on purpose. A second binding that never
        // asked for it must still be refused, or one team's accepted
        // limitation silently becomes everyone's.
        RecordSerializerFactory factory = config -> new TestPlaceholderSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        BindingConfig accepted = createTestConfig("accepted");
        accepted.setAcceptPlaceholderSerializer(true);
        BindingConfig other = createTestConfig("other");

        List<String> errors = validator.validateBindings(List.of(accepted, other));

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("other");
    }

    @Test
    void acceptanceDoesNothingForAProductionSerializer() {
        // Setting it on a binding that does not need it must not become a
        // habit that hides a later regression to a placeholder.
        RecordSerializerFactory factory = config -> new TestProductionSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        BindingConfig config = createTestConfig("test-binding");
        config.setAcceptPlaceholderSerializer(true);

        assertThat(validator.validateBindings(List.of(config))).isEmpty();
    }

    @Test
    void nonProductionModeAllowsPlaceholderSerializer() {
        RecordSerializerFactory factory = config -> new TestPlaceholderSerializer();
        SerializerValidator validator = new SerializerValidator(factory, false);

        BindingConfig config = createTestConfig("test-binding");

        List<String> errors = validator.validateBindings(List.of(config));

        assertThat(errors).isEmpty();
    }

    @Test
    void productionModeAcceptsProductionSerializer() {
        RecordSerializerFactory factory = config -> new TestProductionSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        BindingConfig config = createTestConfig("test-binding");

        List<String> errors = validator.validateBindings(List.of(config));

        assertThat(errors).isEmpty();
    }

    @Test
    void validateOrFailThrowsOnPlaceholderInProduction() {
        RecordSerializerFactory factory = config -> new TestPlaceholderSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        BindingConfig config = createTestConfig("test-binding");

        assertThatThrownBy(() -> validator.validateOrFail(List.of(config)))
                .isInstanceOf(SerializerValidator.SerializerValidationException.class)
                .hasMessageContaining("placeholder")
                .hasMessageContaining("production mode");
    }

    @Test
    void validateOrFailPassesOnProductionSerializer() throws Exception {
        RecordSerializerFactory factory = config -> new TestProductionSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        BindingConfig config = createTestConfig("test-binding");

        assertThatCode(() -> validator.validateOrFail(List.of(config)))
                .doesNotThrowAnyException();
    }

    @Test
    void multipleBindingsReportAllErrors() {
        RecordSerializerFactory factory = config -> new TestPlaceholderSerializer();
        SerializerValidator validator = new SerializerValidator(factory, true);

        List<BindingConfig> configs = List.of(
                createTestConfig("binding1"),
                createTestConfig("binding2"),
                createTestConfig("binding3")
        );

        List<String> errors = validator.validateBindings(configs);

        assertThat(errors).hasSize(3);
        assertThat(errors.get(0)).contains("binding1");
        assertThat(errors.get(1)).contains("binding2");
        assertThat(errors.get(2)).contains("binding3");
    }

    @Test
    void mixedSerializersReportsOnlyPlaceholders() {
        RecordSerializerFactory factory = config -> {
            if (config.getId().contains("placeholder")) {
                return new TestPlaceholderSerializer();
            }
            return new TestProductionSerializer();
        };
        SerializerValidator validator = new SerializerValidator(factory, true);

        List<BindingConfig> configs = List.of(
                createTestConfig("production-binding"),
                createTestConfig("placeholder-binding"),
                createTestConfig("another-production")
        );

        List<String> errors = validator.validateBindings(configs);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("placeholder-binding");
    }

    @Test
    void isProductionModeReturnsTrueWhenSet() {
        SerializerValidator validator = new SerializerValidator(config -> new TestProductionSerializer(), true);
        assertThat(validator.isProductionMode()).isTrue();
    }

    @Test
    void isProductionModeReturnsFalseWhenNotSet() {
        SerializerValidator validator = new SerializerValidator(config -> new TestProductionSerializer(), false);
        assertThat(validator.isProductionMode()).isFalse();
    }

    private BindingConfig createTestConfig(String id) {
        BindingConfig config = new BindingConfig();
        config.setId(id);
        config.setSourceQueue("TEST.QUEUE");
        config.setMode(BindingMode.LAND_ONLY);
        config.getHdfs().setBasePath("/test/path");
        config.getBatch().setSize(100);
        config.getBatch().setBytes(1024);
        config.getBatch().setIntervalMs(1000);
        config.setListenerThreads(1);
        return config;
    }

    private static class TestPlaceholderSerializer implements RecordSerializer, PlaceholderSerializer {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata) {
            return new SerializedRecord(new Text("key"), new BytesWritable());
        }

        @Override
        public Class<? extends Writable> getKeyClass() {
            return Text.class;
        }

        @Override
        public Class<? extends Writable> getValueClass() {
            return BytesWritable.class;
        }

        @Override
        public String getPlaceholderReason() {
            return "Test placeholder serializer";
        }
    }

    private static class TestProductionSerializer implements RecordSerializer {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata) {
            return new SerializedRecord(new Text("key"), new BytesWritable());
        }

        @Override
        public Class<? extends Writable> getKeyClass() {
            return Text.class;
        }

        @Override
        public Class<? extends Writable> getValueClass() {
            return BytesWritable.class;
        }
    }
}
