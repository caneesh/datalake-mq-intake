package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.SerializerValidator;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import javax.jms.Message;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves RMS can actually start with production mode enabled.
 *
 * <p>RmsRecordSerializer used to carry the {@link PlaceholderSerializer}
 * marker, which {@link SerializerValidator} rejects in production mode. The
 * effect was that {@code MQ_INTAKE_PRODUCTION=true} failed startup outright —
 * while READINESS_REVIEW.md claimed the RMS production gate was resolved. The
 * marker was removed once the SequenceFile contract it was gating (byte-offset
 * LongWritable key, normalised Text value) was confirmed against the MDB.
 *
 * <p>The second test is the one that keeps this honest: the gate must still
 * reject a genuine placeholder, so this is not simply the check being switched
 * off.
 */
class RmsProductionModeGateTest {

    @Test
    void rmsSerializerIsAcceptedInProductionMode() {
        SerializerValidator validator = new SerializerValidator(
                config -> new RmsRecordSerializer(), true);

        assertThat(validator.isProductionMode()).isTrue();
        assertThat(validator.validateBindings(List.of(rmsBinding()))).isEmpty();
        assertThatCode(() -> validator.validateOrFail(List.of(rmsBinding())))
                .doesNotThrowAnyException();
    }

    @Test
    void aGenuinePlaceholderIsStillRejectedInProductionMode() {
        RecordSerializerFactory factory = config -> new StillAPlaceholder();
        SerializerValidator validator = new SerializerValidator(factory, true);

        assertThat(validator.validateBindings(List.of(rmsBinding())))
                .singleElement().asString()
                .contains("rms")
                .contains("placeholder serializer");

        assertThatThrownBy(() -> validator.validateOrFail(List.of(rmsBinding())))
                .isInstanceOf(SerializerValidator.SerializerValidationException.class);
    }

    @Test
    void rmsSerializerNoLongerCarriesThePlaceholderMarker() {
        assertThat(new RmsRecordSerializer()).isNotInstanceOf(PlaceholderSerializer.class);
    }

    @Test
    void rmsSerializerStillDeclaresTheProductionKeyAndValueTypes() {
        RmsRecordSerializer serializer = new RmsRecordSerializer();

        // The types the placeholder marker was gating. If these ever drift,
        // removing the marker was wrong.
        assertThat(serializer.getKeyClass()).isEqualTo(LongWritable.class);
        assertThat(serializer.getValueClass()).isEqualTo(Text.class);
    }

    private BindingConfig rmsBinding() {
        BindingConfig config = new BindingConfig();
        config.setId("rms");
        config.setMode(BindingMode.TRACKED);
        return config;
    }

    /** A serializer that really is incomplete — the gate must still catch it. */
    private static class StillAPlaceholder implements RecordSerializer, PlaceholderSerializer {
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
            return "deliberately incomplete, for this test";
        }
    }
}
