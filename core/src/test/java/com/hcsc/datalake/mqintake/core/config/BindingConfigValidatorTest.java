package com.hcsc.datalake.mqintake.core.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

class BindingConfigValidatorTest {

    private BindingConfigValidator validator;
    private HdfsPathValidator alwaysValidPathValidator;
    private IntakeProperties properties;

    @BeforeEach
    void setUp() {
        alwaysValidPathValidator = path -> HdfsPathValidator.PathValidationResult.success();
        validator = new BindingConfigValidator(alwaysValidPathValidator);
        properties = new IntakeProperties();
        properties.setMaxumsgs(10_000);
        properties.setAggregateMemoryCeilingBytes(1_073_741_824L); // 1 GB
    }

    @Test
    void validConfigurationPasses() {
        properties.setBindings(Arrays.asList(
                createValidTrackedBinding("rms"),
                createValidLandOnlyBinding("claims")
        ));

        validator.validate(properties);
    }

    @Test
    void emptyBindingsListFails() {
        properties.setBindings(Collections.emptyList());

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("No bindings configured");
    }

    @Test
    void nullBindingsListFails() {
        properties.setBindings(null);

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("No bindings configured");
    }

    @Test
    void duplicateBindingIdFails() {
        BindingConfig binding1 = createValidTrackedBinding("duplicate-id");
        BindingConfig binding2 = createValidLandOnlyBinding("duplicate-id");
        binding2.setSourceQueue("queue2"); // Different queue to avoid that error

        properties.setBindings(Arrays.asList(binding1, binding2));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("Duplicate binding id: duplicate-id");
    }

    @Test
    void duplicateSourceQueueOnSameConnectionFails() {
        BindingConfig binding1 = createValidTrackedBinding("binding1");
        binding1.setSourceQueue("SAME.QUEUE");
        binding1.setMqConnection("qm1");

        BindingConfig binding2 = createValidLandOnlyBinding("binding2");
        binding2.setSourceQueue("SAME.QUEUE");
        binding2.setMqConnection("qm1");

        properties.setBindings(Arrays.asList(binding1, binding2));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("Duplicate source queue: SAME.QUEUE");
    }

    @Test
    void sameQueueNameOnDifferentQueueManagersIsAllowed() {
        // A feed spread across an HA / load-shared queue-manager pair presents
        // the same queue name on each, and both must be consumed. Keying
        // uniqueness on the name alone would reject a valid deployment at
        // startup — which is the real topology of at least one feed, where the
        // same queue name is presented on two independent queue managers.
        BindingConfig onQm1 = createValidLandOnlyBinding("feed-qm1");
        onQm1.setSourceQueue("SHARED.QUEUE.NAME");
        onQm1.setMqConnection("qm1");

        BindingConfig onQm2 = createValidLandOnlyBinding("feed-qm2");
        onQm2.setSourceQueue("SHARED.QUEUE.NAME");
        onQm2.setMqConnection("qm2");

        properties.setBindings(Arrays.asList(onQm1, onQm2));

        assertThatCode(() -> validator.validate(properties)).doesNotThrowAnyException();
    }

    @Test
    void trackedBindingWithoutTrackerQueueFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setTrackerQueue(null);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("TRACKED binding 'rms' requires a tracker_queue");
    }

    @Test
    void trackedBindingWithBlankTrackerQueueFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setTrackerQueue("   ");

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("TRACKED binding 'rms' requires a tracker_queue");
    }

    @Test
    void landOnlyBindingWithTrackerQueueFails() {
        BindingConfig binding = createValidLandOnlyBinding("claims");
        binding.setTrackerQueue("UNEXPECTED.TRACKER.QUEUE");

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("LAND_ONLY binding 'claims' must not configure a tracker_queue");
    }

    @Test
    void trackedBindingBatchSizeExceedsHalfMaxumsgsFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBatchSize(6000); // > 10000/2 = 5000

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("batch_size 6000 exceeds MAXUMSGS/2 = 5000");
    }

    @Test
    void trackedBindingBatchSizeAtExactlyHalfMaxumsgsPasses() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBatchSize(5000); // Exactly 10000/2

        properties.setBindings(Collections.singletonList(binding));

        validator.validate(properties);
    }

    @Test
    void landOnlyBindingBatchSizeExceedsMaxumsgsFails() {
        BindingConfig binding = createValidLandOnlyBinding("claims");
        binding.setBatchSize(11000); // > 10000

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("batch_size 11000 exceeds MAXUMSGS = 10000");
    }

    @Test
    void landOnlyBindingBatchSizeAtExactlyMaxumsgsPasses() {
        BindingConfig binding = createValidLandOnlyBinding("claims");
        binding.setBatchSize(10000); // Exactly MAXUMSGS

        properties.setBindings(Collections.singletonList(binding));

        validator.validate(properties);
    }

    @Test
    void zeroBatchSizeFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBatchSize(0);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("batch_size must be positive");
    }

    @Test
    void negativeBatchSizeFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBatchSize(-1);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("batch_size must be positive");
    }

    @Test
    void aggregateMemoryExceedsCeilingFails() {
        properties.setAggregateMemoryCeilingBytes(100_000_000L); // 100 MB

        BindingConfig binding1 = createValidTrackedBinding("rms");
        binding1.setBatchBytes(60_000_000L); // 60 MB
        binding1.setListenerThreads(1);

        BindingConfig binding2 = createValidLandOnlyBinding("claims");
        binding2.setBatchBytes(60_000_000L); // 60 MB
        binding2.setListenerThreads(1);
        // Total: 120 MB > 100 MB ceiling

        properties.setBindings(Arrays.asList(binding1, binding2));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("Aggregate batch memory")
                .hasMessageContaining("exceeds ceiling");
    }

    @Test
    void aggregateMemoryAccountsForListenerThreads() {
        properties.setAggregateMemoryCeilingBytes(100_000_000L); // 100 MB

        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBatchBytes(30_000_000L); // 30 MB per thread
        binding.setListenerThreads(4); // 4 threads = 120 MB total

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("Aggregate batch memory")
                .hasMessageContaining("exceeds ceiling");
    }

    @Test
    void missingBindingIdFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setId(null);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("missing required field: id");
    }

    @Test
    void blankBindingIdFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setId("   ");

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("missing required field: id");
    }

    @Test
    void missingSourceQueueFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setSourceQueue(null);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("missing required field: source_queue");
    }

    @Test
    void missingModeFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setMode(null);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("missing required field: mode");
    }

    @Test
    void missingHdfsBasePathFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setHdfsBasePath(null);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("missing required field: hdfs_base_path");
    }

    @Test
    void zeroBatchBytesFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBatchBytes(0);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("batch_bytes must be positive");
    }

    @Test
    void zeroBatchIntervalMsIsAllowedAndDisablesTheFixedTimer() {
        BindingConfig binding = createValidTrackedBinding("rms");
        // 0 means "no fixed timer" — the partition boundary remains an
        // unconditional flush trigger, so a batch is still bounded in time.
        binding.setBatchIntervalMs(0);

        properties.setBindings(Collections.singletonList(binding));

        assertThatCode(() -> validator.validate(properties))
                .doesNotThrowAnyException();
    }

    @Test
    void negativeBatchIntervalMsFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBatchIntervalMs(-1);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("batch_interval_ms must not be negative");
    }

    @Test
    void zeroListenerThreadsFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setListenerThreads(0);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("listener_threads must be positive");
    }

    @Test
    void hdfsPathNotWritableFails() {
        HdfsPathValidator failingValidator = path ->
                HdfsPathValidator.PathValidationResult.failure("Permission denied: " + path);
        validator = new BindingConfigValidator(failingValidator);

        BindingConfig binding = createValidTrackedBinding("rms");
        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("HDFS path not writable")
                .hasMessageContaining("Permission denied");
    }

    @Test
    void multipleErrorsAreAllReported() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setTrackerQueue(null); // Error 1: TRACKED without tracker queue
        binding.setBatchSize(6000);    // Error 2: exceeds MAXUMSGS/2

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("TRACKED binding 'rms' requires a tracker_queue")
                .hasMessageContaining("batch_size 6000 exceeds");
    }

    @Test
    void trackerBodyModeDefaultsToFullCopy() {
        BindingConfig binding = new BindingConfig();
        assertThat(binding.getTrackerBodyMode()).isEqualTo(TrackerBodyMode.FULL_COPY);
    }

    @Test
    void backoutQueueWithInvalidThresholdFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBackoutQueue("MQ.BACKOUT.RMS");
        binding.setBackoutThreshold(0);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("backout_queue")
                .hasMessageContaining("backout_threshold");
    }

    @Test
    void zeroSuccessesRequiredToRestoreFails() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setSuccessesRequiredToRestore(0);

        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> validator.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("successes_required_to_restore must be positive");
    }

    @Test
    void validBackoutQueueConfigurationPasses() {
        BindingConfig binding = createValidTrackedBinding("rms");
        binding.setBackoutQueue("MQ.BACKOUT.RMS");
        binding.setBackoutThreshold(5);
        binding.setSuccessesRequiredToRestore(10);

        properties.setBindings(Collections.singletonList(binding));

        validator.validate(properties); // Should not throw
    }

    private BindingConfig createValidTrackedBinding(String id) {
        BindingConfig binding = new BindingConfig();
        binding.setId(id);
        binding.setSourceQueue("MQ.SOURCE." + id.toUpperCase());
        binding.setMode(BindingMode.TRACKED);
        binding.setTrackerQueue("MQ.TRACKER." + id.toUpperCase());
        binding.setTrackerBodyMode(TrackerBodyMode.FULL_COPY);
        binding.setTrackerFields(new TrackerFields("DMIH/DL", "IIB", "RCVD", ""));
        binding.setHdfsBasePath("/data/raw/" + id);
        binding.setBatchSize(4000);
        binding.setBatchBytes(134_217_728L); // 128 MB
        binding.setBatchIntervalMs(30_000);
        binding.setListenerThreads(4);
        return binding;
    }

    private BindingConfig createValidLandOnlyBinding(String id) {
        BindingConfig binding = new BindingConfig();
        binding.setId(id);
        binding.setSourceQueue("MQ.SOURCE." + id.toUpperCase());
        binding.setMode(BindingMode.LAND_ONLY);
        binding.setTrackerQueue(null); // No tracker queue for LAND_ONLY
        binding.setHdfsBasePath("/data/raw/" + id);
        binding.setBatchSize(8000);
        binding.setBatchBytes(134_217_728L); // 128 MB
        binding.setBatchIntervalMs(30_000);
        binding.setListenerThreads(4);
        return binding;
    }

    // --- Heap-derived ceiling ---

    @Test
    void ceilingIsDerivedFromMaxHeapWhenUnset() {
        // 1 GB heap -> 512 MB derived ceiling, so 600 MB of batches must fail.
        BindingConfigValidator heapAware =
                new BindingConfigValidator(alwaysValidPathValidator, () -> 1_073_741_824L);

        properties.setAggregateMemoryCeilingBytes(0); // unset -> derive
        BindingConfig binding = createValidLandOnlyBinding("claims");
        binding.setBatchBytes(600_000_000L);
        binding.setListenerThreads(1);
        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> heapAware.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("Aggregate batch memory")
                .hasMessageContaining("max heap");
    }

    @Test
    void derivedCeilingAcceptsAConfigThatFitsTheHeap() {
        // The same 600 MB passes on an 8 GB heap. A fixed ceiling cannot adapt
        // like this — it approves or rejects regardless of the actual -Xmx.
        BindingConfigValidator heapAware =
                new BindingConfigValidator(alwaysValidPathValidator, () -> 8L * 1_073_741_824L);

        properties.setAggregateMemoryCeilingBytes(0);
        BindingConfig binding = createValidLandOnlyBinding("claims");
        binding.setBatchBytes(600_000_000L);
        binding.setListenerThreads(1);
        properties.setBindings(Collections.singletonList(binding));

        assertThatCode(() -> heapAware.validate(properties)).doesNotThrowAnyException();
    }

    @Test
    void configuredCeilingLargerThanTheHeapIsRejected() {
        // The dangerous case the old fixed default allowed: a 1 GB ceiling on a
        // 512 MB heap passed validation, then OOMed under load.
        BindingConfigValidator heapAware =
                new BindingConfigValidator(alwaysValidPathValidator, () -> 536_870_912L);

        properties.setAggregateMemoryCeilingBytes(1_073_741_824L);
        BindingConfig binding = createValidLandOnlyBinding("claims");
        binding.setBatchBytes(1_000_000L);
        binding.setListenerThreads(1);
        properties.setBindings(Collections.singletonList(binding));

        assertThatThrownBy(() -> heapAware.validate(properties))
                .isInstanceOf(BindingConfigurationException.class)
                .hasMessageContaining("exceeds 70% of JVM max heap")
                .hasMessageContaining("Raise -Xmx");
    }

    @Test
    void twoBindingsPerFeedFitOnAnAdequateHeap() {
        // The two-queue-manager topology: one logical feed as two bindings, so
        // batch_bytes x listener_threads is counted twice.
        BindingConfigValidator heapAware =
                new BindingConfigValidator(alwaysValidPathValidator, () -> 4L * 1_073_741_824L);

        properties.setAggregateMemoryCeilingBytes(0);
        BindingConfig qm1 = createValidLandOnlyBinding("feed-qm1");
        qm1.setMqConnection("qm1");
        qm1.setBatchBytes(134_217_728L);
        qm1.setListenerThreads(4);

        BindingConfig qm2 = createValidLandOnlyBinding("feed-qm2");
        qm2.setMqConnection("qm2");
        qm2.setBatchBytes(134_217_728L);
        qm2.setListenerThreads(4);

        properties.setBindings(Arrays.asList(qm1, qm2));

        // 2 x 512 MB = 1 GB against a 2 GB derived ceiling
        assertThatCode(() -> heapAware.validate(properties)).doesNotThrowAnyException();
    }
}
