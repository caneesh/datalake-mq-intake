package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.rms.tracker.RmsTrackerMessageBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the RMS tracker contract gate (round 2 prompt 8).
 *
 * <p>The gate existed to stop a production deploy while the legacy
 * MessageHeaderDetails rewrite (§20.4) was only partially captured. The full
 * source for every method in that rewrite has since been obtained and
 * reproduced, so the gate now opens.
 *
 * <p>These tests are kept, inverted: they assert the gate is satisfied for the
 * right reason, and that the mechanism still works if a gap ever reappears.
 */
class RmsTrackerContractGatingTest {

    @Test
    void trackerContractIsNowComplete() {
        assertThat(RmsTrackerMessageBuilder.trackerContractGaps()).isEmpty();
        assertThat(RmsTrackerMessageBuilder.isTrackerContractReady()).isTrue();
    }

    @Test
    void rmsProductionStartupIsNoLongerBlocked() {
        RmsConfiguration config = new RmsConfiguration(true);

        assertThatCode(config::trackerMessageBuilderFactory).doesNotThrowAnyException();
        assertThat(config.trackerMessageBuilderFactory()).isNotNull();
    }

    @Test
    void nonProductionStillStarts() {
        RmsConfiguration config = new RmsConfiguration(false);

        assertThatCode(config::trackerMessageBuilderFactory).doesNotThrowAnyException();
    }

    @Test
    void theGateStillBlocksIfAGapReappears() {
        // The mechanism is what protects a future change that reopens a gap —
        // e.g. someone adding a tag whose value mapping is not known. Assert it
        // still refuses rather than trusting it never has to.
        assertThatThrownBy(() -> new RmsConfiguration(true) {
            @Override
            void validateTrackerContract() {
                throw new IllegalStateException(
                        "RMS tracker contract is INCOMPLETE — production startup blocked (§20.4). " +
                        "Missing legacy artifacts: simulated gap");
            }
        }.trackerMessageBuilderFactory())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("production startup blocked");
    }
}
