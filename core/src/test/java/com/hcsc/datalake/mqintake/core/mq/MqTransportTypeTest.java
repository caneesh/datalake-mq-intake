package com.hcsc.datalake.mqintake.core.mq;

import com.ibm.msg.client.wmq.WMQConstants;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Transport resolution, previously a private if/else chain inside the
 * connection manager's inner class and reachable only by connecting.
 */
class MqTransportTypeTest {

    @Test
    void clientIsTheDefaultForAbsentConfiguration() {
        assertThat(MqTransportType.fromConfig(null)).isEqualTo(MqTransportType.CLIENT);
        assertThat(MqTransportType.fromConfig("")).isEqualTo(MqTransportType.CLIENT);
        assertThat(MqTransportType.fromConfig("   ")).isEqualTo(MqTransportType.CLIENT);
    }

    @Test
    void recognisedValuesMapToTheirWmqConstants() {
        assertThat(MqTransportType.fromConfig("CLIENT").wmqConstant())
                .isEqualTo(WMQConstants.WMQ_CM_CLIENT);
        assertThat(MqTransportType.fromConfig("BINDINGS").wmqConstant())
                .isEqualTo(WMQConstants.WMQ_CM_BINDINGS);
    }

    @Test
    void resolutionIsCaseAndWhitespaceInsensitive() {
        assertThat(MqTransportType.fromConfig("client")).isEqualTo(MqTransportType.CLIENT);
        assertThat(MqTransportType.fromConfig("  Bindings  ")).isEqualTo(MqTransportType.BINDINGS);
    }

    @Test
    void anUnknownValueFallsBackToClient() {
        // Preserves the previous behaviour. Worth knowing it is a fallback and
        // not a rejection: BINDINGS vs CLIENT is a local queue manager versus a
        // network hop, so a typo yields a working connection of the wrong kind.
        assertThat(MqTransportType.fromConfig("TLS")).isEqualTo(MqTransportType.CLIENT);
        assertThat(MqTransportType.fromConfig("BINDINGZ")).isEqualTo(MqTransportType.CLIENT);
    }

    @Test
    void bindingsAndClientAreDistinct() {
        assertThat(MqTransportType.CLIENT.wmqConstant())
                .isNotEqualTo(MqTransportType.BINDINGS.wmqConstant());
    }
}
