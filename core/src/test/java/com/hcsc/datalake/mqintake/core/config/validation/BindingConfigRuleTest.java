package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingConfigValidator;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Each configuration rule, exercised on its own.
 *
 * <p>Previously impossible: the checks were private methods on the validator,
 * so the only way to test one was to build a whole configuration, run all nine,
 * and infer which had fired from the message text. Now a rule is a value.
 */
class BindingConfigRuleTest {

    @Test
    void uniqueBindingIdsRuleReportsOnlyDuplicates() {
        BindingConfigRule rule = new UniqueBindingIdsRule();

        assertThat(rule.validate(propertiesWith(binding("a"), binding("b")))).isEmpty();
        assertThat(rule.validate(propertiesWith(binding("a"), binding("a"))))
                .singleElement().asString().contains("Duplicate binding id: a");
    }

    @Test
    void sameQueueNameOnDifferentQueueManagersIsNotADuplicate() {
        // An HA or load-shared pair presents the same queue on each queue
        // manager, and both must be consumed. Keying on name alone would
        // reject that topology at startup.
        BindingConfigRule rule = new UniqueSourceQueuesRule();

        BindingConfig first = binding("a");
        first.setMqConnection("qm1");
        first.setSourceQueue("SHARED.QUEUE");
        BindingConfig second = binding("b");
        second.setMqConnection("qm2");
        second.setSourceQueue("SHARED.QUEUE");

        assertThat(rule.validate(propertiesWith(first, second))).isEmpty();

        second.setMqConnection("qm1");
        assertThat(rule.validate(propertiesWith(first, second)))
                .singleElement().asString().contains("Duplicate source queue");
    }

    @Test
    void trackerQueueMustMatchTheMode() {
        BindingConfigRule rule = new TrackerQueueConsistencyRule();

        BindingConfig trackedWithout = binding("rms");
        trackedWithout.setMode(BindingMode.TRACKED);
        assertThat(rule.validate(propertiesWith(trackedWithout)))
                .singleElement().asString().contains("requires a tracker_queue");

        BindingConfig landOnlyWith = binding("claims");
        landOnlyWith.setMode(BindingMode.LAND_ONLY);
        landOnlyWith.setTrackerQueue("SOME.TRACKER");
        assertThat(rule.validate(propertiesWith(landOnlyWith)))
                .singleElement().asString().contains("must not configure a tracker_queue");
    }

    @Test
    void aMissingModeIsLeftToTheRequiredFieldsRule() {
        // Rules must not duplicate each other's findings, or one mistake is
        // reported several times and the real list gets harder to read.
        BindingConfig noMode = binding("x");
        noMode.setMode(null);

        assertThat(new TrackerQueueConsistencyRule().validate(propertiesWith(noMode))).isEmpty();
        assertThat(new BatchSizeRule().validate(propertiesWith(noMode))).isEmpty();
        assertThat(new RequiredFieldsRule().validate(propertiesWith(noMode)))
                .anySatisfy(e -> assertThat(e).contains("missing required field: mode"));
    }

    @Test
    void trackedBindingsMayOnlyUseHalfOfMaxumsgs() {
        // A TRACKED unit of work is 2N: each source get is paired with a
        // tracker put.
        BindingConfigRule rule = new BatchSizeRule();

        BindingConfig tracked = binding("rms");
        tracked.setMode(BindingMode.TRACKED);
        tracked.setTrackerQueue("T");
        tracked.setBatchSize(6000);

        IntakeProperties properties = propertiesWith(tracked);
        properties.setMaxumsgs(10_000);

        assertThat(rule.validate(properties))
                .singleElement().asString().contains("MAXUMSGS/2");

        tracked.setBatchSize(5000);
        assertThat(rule.validate(properties)).isEmpty();
    }

    @Test
    void bisectThresholdMinimumIsCeilLog2PlusOne() {
        assertThat(BisectBackoutThresholdRule.minimumThresholdFor(16)).isEqualTo(5);
        assertThat(BisectBackoutThresholdRule.minimumThresholdFor(8000)).isEqualTo(14);
        assertThat(BisectBackoutThresholdRule.minimumThresholdFor(2)).isEqualTo(2);
    }

    @Test
    void bisectWithTooLowAThresholdWouldMisrouteCleanMessages() {
        BindingConfigRule rule = new BisectBackoutThresholdRule();

        BindingConfig claims = binding("claims");
        claims.setDegradationStrategy(DegradationStrategy.BISECT);
        claims.setBackoutQueue("BOQ");
        claims.setBatchSize(8000);
        claims.setBackoutThreshold(5);

        assertThat(rule.validate(propertiesWith(claims)))
                .singleElement().asString()
                .contains("required minimum 14")
                .contains("misrouted to the backout queue");

        claims.setBackoutThreshold(14);
        assertThat(rule.validate(propertiesWith(claims))).isEmpty();
    }

    @Test
    void bisectRuleIgnoresBindingsWithoutABackoutQueue() {
        BindingConfigRule rule = new BisectBackoutThresholdRule();

        BindingConfig noBoq = binding("x");
        noBoq.setDegradationStrategy(DegradationStrategy.BISECT);
        noBoq.setBatchSize(8000);
        noBoq.setBackoutThreshold(1);

        assertThat(rule.validate(propertiesWith(noBoq))).isEmpty();
    }

    @Test
    void aggregateMemoryCeilingIsDerivedFromTheHeapWhenNotConfigured() {
        // 1 GB heap -> 512 MB default ceiling
        BindingConfigRule rule = new AggregateMemoryRule(() -> 1_073_741_824L);

        BindingConfig big = binding("big");
        big.setBatchBytes(400L * 1024 * 1024);
        big.setListenerThreads(2);           // 800 MB > 512 MB

        assertThat(rule.validate(propertiesWith(big)))
                .singleElement().asString()
                .contains("Aggregate batch memory")
                .contains("800.00 MB")
                .contains("512.00 MB");
    }

    @Test
    void aConfiguredCeilingTheHeapCannotHonourIsRejected() {
        // Worse than no ceiling: it reports "validated" and then OOMs.
        BindingConfigRule rule = new AggregateMemoryRule(() -> 1_073_741_824L);

        IntakeProperties properties = propertiesWith(binding("a"));
        properties.setAggregateMemoryCeilingBytes(1_000_000_000L);   // >70% of 1 GB

        assertThat(rule.validate(properties))
                .singleElement().asString()
                .contains("exceeds 70% of JVM max heap");
    }

    @Test
    void memoryErrorNamesEachBindingsContribution() {
        BindingConfigRule rule = new AggregateMemoryRule(() -> 1_073_741_824L);

        BindingConfig a = binding("rms");
        a.setBatchBytes(300L * 1024 * 1024);
        a.setListenerThreads(1);
        BindingConfig b = binding("claims");
        b.setBatchBytes(300L * 1024 * 1024);
        b.setListenerThreads(1);

        assertThat(rule.validate(propertiesWith(a, b)))
                .singleElement().asString()
                .contains("rms=").contains("claims=")
                .contains("reduce batch_bytes or listener_threads");
    }

    @Test
    void byteFormatIsReadable() {
        assertThat(ByteFormat.format(512)).isEqualTo("512 bytes");
        assertThat(ByteFormat.format(2048)).isEqualTo("2.00 KB");
        assertThat(ByteFormat.format(134_217_728L)).isEqualTo("128.00 MB");
        assertThat(ByteFormat.format(1_073_741_824L)).isEqualTo("1.00 GB");
    }

    @Test
    void everyRuleReportsAllProblemsRatherThanStoppingAtTheFirst() {
        // An operator should see every problem in one pass, not rediscover
        // them one restart at a time.
        BindingConfig broken = binding("broken");
        broken.setSourceQueue(null);
        broken.setHdfsBasePath(null);
        broken.setBatchBytes(0);
        broken.setListenerThreads(0);

        assertThat(new RequiredFieldsRule().validate(propertiesWith(broken)))
                .hasSizeGreaterThanOrEqualTo(4);
    }

    @Test
    void blankMqConnectionFieldsAreCaughtAtStartupNotAtFirstConnect() {
        // The natural result of an unset env var with an empty default. These
        // used to pass every check and fail only inside MQConnectionFactory.
        BindingConfigRule rule = new MqConnectionSanityRule();
        IntakeProperties properties = propertiesWith(binding("a"));

        com.hcsc.datalake.mqintake.core.config.MqConnectionConfig conn =
                new com.hcsc.datalake.mqintake.core.config.MqConnectionConfig();
        conn.setId("primary");
        conn.setHost("");            // blank host
        conn.setQueueManager("QM1");
        conn.setChannel(null);       // missing channel
        properties.setMqConnections(new java.util.LinkedHashMap<>(
                java.util.Map.of("primary", conn)));

        assertThat(rule.validate(properties))
                .anySatisfy(e -> assertThat(e).contains("host"))
                .anySatisfy(e -> assertThat(e).contains("channel"));
    }

    @Test
    void zeroReceiveTimeoutIsRejectedBecauseItBlocksForever() {
        // receive(0) means wait forever per the JMS spec, not "no wait" —
        // and the idle branch that flushes at partition boundaries only runs
        // when receive() returns null, which then never happens.
        BindingConfigRule rule = new MqConnectionSanityRule();
        IntakeProperties properties = propertiesWith(binding("a"));

        com.hcsc.datalake.mqintake.core.config.MqConnectionConfig conn =
                new com.hcsc.datalake.mqintake.core.config.MqConnectionConfig();
        conn.setId("primary");
        conn.setHost("mq.host");
        conn.setQueueManager("QM1");
        conn.setChannel("APP.SVRCONN");
        conn.setReceiveTimeoutMs(0);
        properties.setMqConnections(new java.util.LinkedHashMap<>(
                java.util.Map.of("primary", conn)));

        assertThat(rule.validate(properties))
                .singleElement().asString()
                .contains("receive-timeout-ms")
                .contains("blocks");
    }

    @Test
    void aWellFormedConnectionPasses() {
        BindingConfigRule rule = new MqConnectionSanityRule();
        IntakeProperties properties = propertiesWith(binding("a"));

        com.hcsc.datalake.mqintake.core.config.MqConnectionConfig conn =
                new com.hcsc.datalake.mqintake.core.config.MqConnectionConfig();
        conn.setId("primary");
        conn.setHost("mq.host");
        conn.setQueueManager("QM1");
        conn.setChannel("APP.SVRCONN");
        properties.setMqConnections(new java.util.LinkedHashMap<>(
                java.util.Map.of("primary", conn)));

        assertThat(rule.validate(properties)).isEmpty();
    }

    @Test
    void aBackoutQueueThatIsAlsoASourceQueueIsAFeedbackLoop() {
        BindingConfigRule rule = new QueueCollisionRule();

        BindingConfig a = binding("a");
        BindingConfig b = binding("b");
        b.setBackoutQueue(a.getSourceQueue());   // same connection

        assertThat(rule.validate(propertiesWith(a, b)))
                .singleElement().asString()
                .contains("feedback loop");

        // On a DIFFERENT connection the same name is a different queue
        b.setMqConnection("other-qm");
        assertThat(rule.validate(propertiesWith(a, b))).isEmpty();
    }

    @Test
    void twoBindingsSharingATrackerQueueAreRejected() {
        BindingConfigRule rule = new QueueCollisionRule();

        BindingConfig a = binding("a");
        a.setMode(BindingMode.TRACKED);
        a.setTrackerQueue("SHARED.TRACKER");
        BindingConfig b = binding("b");
        b.setMode(BindingMode.TRACKED);
        b.setTrackerQueue("SHARED.TRACKER");

        assertThat(rule.validate(propertiesWith(a, b)))
                .singleElement().asString()
                .contains("interleave");
    }

    @Test
    void aTrackerQueueThatIsAlsoASourceQueueIsRejected() {
        BindingConfigRule rule = new QueueCollisionRule();

        BindingConfig a = binding("a");
        BindingConfig b = binding("b");
        b.setMode(BindingMode.TRACKED);
        b.setTrackerQueue(a.getSourceQueue());

        assertThat(rule.validate(propertiesWith(a, b)))
                .singleElement().asString()
                .contains("consume its own tracker");
    }

    @Test
    void theValidatorAcceptsACustomRuleSet() {
        // The seam: policy is replaceable without editing the validator.
        BindingConfigValidator alwaysFails = new BindingConfigValidator(
                List.of(properties -> List.of("nope")));

        assertThatThrownBy(() -> alwaysFails.validate(propertiesWith(binding("a"))))
                .isInstanceOf(com.hcsc.datalake.mqintake.core.config.BindingConfigurationException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void theDefaultValidatorRunsEveryRule() {
        BindingConfigValidator validator = new BindingConfigValidator(
                path -> com.hcsc.datalake.mqintake.core.config.HdfsPathValidator
                        .PathValidationResult.success());

        assertThat(validator.rules()).hasSize(11);
    }

    // --- helpers ---

    private IntakeProperties propertiesWith(BindingConfig... bindings) {
        IntakeProperties properties = new IntakeProperties();
        properties.setBindings(new ArrayList<>(List.of(bindings)));
        return properties;
    }

    private BindingConfig binding(String id) {
        BindingConfig config = new BindingConfig();
        config.setId(id);
        config.setMqConnection("primary");
        config.setSourceQueue("QUEUE." + id);
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath("/data/" + id);
        config.setBatchSize(100);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(1000);
        config.setListenerThreads(1);
        return config;
    }
}
