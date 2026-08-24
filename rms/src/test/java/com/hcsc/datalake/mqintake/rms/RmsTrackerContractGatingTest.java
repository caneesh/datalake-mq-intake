package com.hcsc.datalake.mqintake.rms;

import com.hcsc.datalake.mqintake.rms.tracker.RmsTrackerMessageBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the RMS tracker contract gate (round 2 prompt 8).
 *
 * <p>The legacy MessageHeaderDetails rewrite is not fully captured (§20.4),
 * so RMS TRACKED production startup must fail until the contract is complete.
 */
class RmsTrackerContractGatingTest {

    @Test
    void trackerContractIsCurrentlyIncomplete() {
        // Evidence-derived: TAG_LIST is empty, root-end constants unverified,
        // no golden-master fixture. If this test starts failing, the contract
        // was completed — remove the production gate expectations accordingly.
        assertThat(RmsTrackerMessageBuilder.isTrackerContractReady()).isFalse();

        // tagList and the root-end constants ARE now captured from EJBHelper,
        // so those gaps are gone. What remains is the tag -> value mapping
        // inside buildResultData, which the call site does not reveal: all four
        // values are passed on every iteration, so which tag receives which
        // cannot be inferred. Guessing would emit well-formed tracker messages
        // carrying wrong values.
        assertThat(RmsTrackerMessageBuilder.trackerContractGaps())
                .isNotEmpty()
                .noneSatisfy(gap -> assertThat(gap).contains("tagList contents"))
                .anySatisfy(gap -> assertThat(gap).contains("buildResultData"));
    }

    @Test
    void incompleteContractBlocksRmsProductionStartup() {
        RmsConfiguration config = new RmsConfiguration(true);

        assertThatThrownBy(config::trackerMessageBuilderFactory)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("production startup blocked")
                .hasMessageContaining("buildResultData");
    }

    @Test
    void incompleteContractAllowedInNonProduction() {
        RmsConfiguration config = new RmsConfiguration(false);

        // Non-production: placeholder rewrite runs with a warning
        assertThatCode(config::trackerMessageBuilderFactory)
                .doesNotThrowAnyException();
        assertThat(config.trackerMessageBuilderFactory()).isNotNull();
    }
}
