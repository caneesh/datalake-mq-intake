package com.hcsc.datalake.mqintake.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates binding configuration at startup.
 * All validations run before any listener starts — fail fast.
 */
@Component
public class BindingConfigValidator {

    private static final Logger log = LoggerFactory.getLogger(BindingConfigValidator.class);

    /**
     * Fraction of max heap used as the batch-memory ceiling when none is
     * configured. Deliberately conservative: {@code batch_bytes} counts
     * estimated payload size, while the retained cost of the JMS messages
     * holding it — object overhead, UTF-16 strings, MQ client buffers — is
     * materially higher. The unused half is that headroom, plus room for
     * everything else in the process.
     */
    private static final double DEFAULT_HEAP_FRACTION = 0.5;

    /**
     * An explicitly configured ceiling above this fraction of max heap is
     * treated as a misconfiguration rather than an intention: batches alone
     * would leave the process no room to run.
     */
    private static final double MAX_SAFE_HEAP_FRACTION = 0.7;

    private final HdfsPathValidator hdfsPathValidator;
    private final java.util.function.LongSupplier maxHeapSupplier;

    @org.springframework.beans.factory.annotation.Autowired
    public BindingConfigValidator(HdfsPathValidator hdfsPathValidator) {
        this(hdfsPathValidator, () -> Runtime.getRuntime().maxMemory());
    }

    /** Visible for testing: lets a test supply a heap size. */
    BindingConfigValidator(HdfsPathValidator hdfsPathValidator,
                           java.util.function.LongSupplier maxHeapSupplier) {
        this.hdfsPathValidator = hdfsPathValidator;
        this.maxHeapSupplier = maxHeapSupplier;
    }

    /**
     * Validates all bindings. Throws BindingConfigurationException on any failure.
     *
     * @param properties the intake properties to validate
     * @throws BindingConfigurationException if any validation fails
     */
    public void validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        List<BindingConfig> bindings = properties.getBindings();

        if (bindings == null || bindings.isEmpty()) {
            throw new BindingConfigurationException("No bindings configured");
        }

        validateNoDuplicateBindingIds(bindings, errors);
        validateNoDuplicateSourceQueues(bindings, errors);
        validateTrackerQueueConsistency(bindings, errors);
        validateBackoutQueueConsistency(bindings, errors);
        validateBatchSizes(bindings, properties.getMaxumsgs(), errors);
        validateAggregateMemory(bindings, properties.getAggregateMemoryCeilingBytes(), errors);
        validateRequiredFields(bindings, errors);
        validateBisectBackoutThreshold(bindings, errors);
        validateHdfsPaths(bindings, errors);

        if (!errors.isEmpty()) {
            String message = "Binding configuration validation failed:\n  - " +
                    String.join("\n  - ", errors);
            log.error(message);
            throw new BindingConfigurationException(message);
        }

        log.info("Binding configuration validated successfully: {} binding(s)", bindings.size());
    }

    private void validateNoDuplicateBindingIds(List<BindingConfig> bindings, List<String> errors) {
        Set<String> seen = new HashSet<>();
        for (BindingConfig binding : bindings) {
            if (binding.getId() != null && !seen.add(binding.getId())) {
                errors.add("Duplicate binding id: " + binding.getId());
            }
        }
    }

    /**
     * A source queue is identified by (mq-connection, queue name), not by name
     * alone. The same queue name legitimately exists on more than one queue
     * manager — a feed spread across an HA/load-shared pair presents the same
     * queue on each, and both must be consumed. Keying on the name alone would
     * reject that configuration at startup as a false duplicate.
     */
    private void validateNoDuplicateSourceQueues(List<BindingConfig> bindings, List<String> errors) {
        Set<String> seen = new HashSet<>();
        for (BindingConfig binding : bindings) {
            if (binding.getSourceQueue() == null) {
                continue;
            }
            String key = binding.getMqConnection() + "::" + binding.getSourceQueue();
            if (!seen.add(key)) {
                errors.add("Duplicate source queue: " + binding.getSourceQueue() +
                        " on mq-connection '" + binding.getMqConnection() +
                        "' (binding: " + binding.getId() + ")");
            }
        }
    }

    private void validateTrackerQueueConsistency(List<BindingConfig> bindings, List<String> errors) {
        for (BindingConfig binding : bindings) {
            if (binding.getMode() == null) {
                continue; // Will be caught by required fields validation
            }

            boolean hasTrackerQueue = binding.getTrackerQueue() != null &&
                    !binding.getTrackerQueue().isBlank();

            if (binding.getMode() == BindingMode.TRACKED && !hasTrackerQueue) {
                errors.add("TRACKED binding '" + binding.getId() +
                        "' requires a tracker_queue");
            }

            if (binding.getMode() == BindingMode.LAND_ONLY && hasTrackerQueue) {
                errors.add("LAND_ONLY binding '" + binding.getId() +
                        "' must not configure a tracker_queue");
            }
        }
    }

    private void validateBackoutQueueConsistency(List<BindingConfig> bindings, List<String> errors) {
        for (BindingConfig binding : bindings) {
            boolean hasBackoutQueue = binding.getBackoutQueue() != null &&
                    !binding.getBackoutQueue().isBlank();
            int backoutThreshold = binding.getBackoutThreshold();

            if (hasBackoutQueue && backoutThreshold <= 0) {
                errors.add("Binding '" + binding.getId() +
                        "' has backout_queue configured but backout_threshold is not positive");
            }

            if (binding.getSuccessesRequiredToRestore() <= 0) {
                errors.add("Binding '" + binding.getId() +
                        "' successes_required_to_restore must be positive");
            }
        }
    }

    private void validateBatchSizes(List<BindingConfig> bindings, int maxumsgs, List<String> errors) {
        for (BindingConfig binding : bindings) {
            if (binding.getMode() == null) {
                continue;
            }

            int maxAllowed = binding.getMaxBatchSizeFor(maxumsgs);
            if (binding.getBatchSize() > maxAllowed) {
                String modeExplanation = binding.getMode() == BindingMode.TRACKED
                        ? "MAXUMSGS/2 = " + maxAllowed + " (unit of work is 2N)"
                        : "MAXUMSGS = " + maxAllowed;
                errors.add("Binding '" + binding.getId() + "' batch_size " +
                        binding.getBatchSize() + " exceeds " + modeExplanation);
            }

            if (binding.getBatchSize() <= 0) {
                errors.add("Binding '" + binding.getId() + "' batch_size must be positive");
            }
        }
    }

    private void validateAggregateMemory(List<BindingConfig> bindings,
                                         long configuredCeiling, List<String> errors) {
        long maxHeap = maxHeapSupplier.getAsLong();
        long safeCeiling = (long) (maxHeap * MAX_SAFE_HEAP_FRACTION);

        long ceiling;
        if (configuredCeiling > 0) {
            ceiling = configuredCeiling;
            // A ceiling the heap cannot honour is worse than no ceiling: it
            // reports "validated" and then OOMs under load.
            if (ceiling > safeCeiling) {
                errors.add("aggregate_memory_ceiling_bytes " + formatBytes(ceiling) +
                        " exceeds " + (int) (MAX_SAFE_HEAP_FRACTION * 100) +
                        "% of JVM max heap (" + formatBytes(maxHeap) + "). Batches alone " +
                        "would leave the process no room to run. Raise -Xmx, or lower the " +
                        "ceiling and the bindings' batch_bytes / listener_threads.");
                return;
            }
        } else {
            ceiling = (long) (maxHeap * DEFAULT_HEAP_FRACTION);
            log.info("aggregate_memory_ceiling_bytes not set — derived {} from {}% of max heap {}",
                    formatBytes(ceiling), (int) (DEFAULT_HEAP_FRACTION * 100), formatBytes(maxHeap));
        }

        long total = 0;
        for (BindingConfig binding : bindings) {
            total += binding.getMemoryFootprint();
        }

        if (total > ceiling) {
            StringBuilder detail = new StringBuilder();
            detail.append("Aggregate batch memory ").append(formatBytes(total))
                  .append(" exceeds ceiling ").append(formatBytes(ceiling));
            if (configuredCeiling <= 0) {
                detail.append(" (").append((int) (DEFAULT_HEAP_FRACTION * 100))
                      .append("% of max heap ").append(formatBytes(maxHeap)).append(")");
            }
            detail.append(". Per binding (batch_bytes × listener_threads): ");
            for (BindingConfig binding : bindings) {
                detail.append(binding.getId()).append("=")
                      .append(formatBytes(binding.getBatchBytes())).append("×")
                      .append(binding.getListenerThreads()).append("=")
                      .append(formatBytes(binding.getMemoryFootprint())).append(" ");
            }
            detail.append("— reduce batch_bytes or listener_threads, or raise -Xmx.");
            errors.add(detail.toString().trim());
        }
    }

    private void validateRequiredFields(List<BindingConfig> bindings, List<String> errors) {
        for (int i = 0; i < bindings.size(); i++) {
            BindingConfig binding = bindings.get(i);
            String prefix = "Binding[" + i + "]";

            if (binding.getId() == null || binding.getId().isBlank()) {
                errors.add(prefix + " missing required field: id");
            } else {
                prefix = "Binding '" + binding.getId() + "'";
            }

            if (binding.getSourceQueue() == null || binding.getSourceQueue().isBlank()) {
                errors.add(prefix + " missing required field: source_queue");
            }

            if (binding.getMode() == null) {
                errors.add(prefix + " missing required field: mode");
            }

            if (binding.getHdfsBasePath() == null || binding.getHdfsBasePath().isBlank()) {
                errors.add(prefix + " missing required field: hdfs_base_path");
            }

            if (binding.getBatchBytes() <= 0) {
                errors.add(prefix + " batch_bytes must be positive");
            }

            // 0 disables the fixed timer, leaving the partition boundary as the
            // only time-based flush trigger (§7.1 cadence, matching the legacy
            // writer's roll-on-path-change behaviour). Negative is meaningless.
            if (binding.getBatchIntervalMs() < 0) {
                errors.add(prefix + " batch_interval_ms must not be negative " +
                        "(0 disables the fixed timer; the partition boundary still flushes)");
            }

            if (binding.getListenerThreads() <= 0) {
                errors.add(prefix + " listener_threads must be positive");
            }
        }
    }

    /**
     * BISECT + backout interplay (§6.1): while a poison message is bisected
     * toward isolation, clean messages sharing its failing batches accumulate
     * backout count too. A clean message can share up to ceil(log2(batch_size))
     * failing deliveries before the poison is alone, so its delivery count can
     * legitimately reach ceil(log2(batch_size)) + 1. If BOTHRESH is at or below
     * that, clean messages get misrouted to the BOQ. Enforce
     * backout_threshold >= ceil(log2(batch_size)) + 1 for BISECT bindings.
     */
    private void validateBisectBackoutThreshold(List<BindingConfig> bindings, List<String> errors) {
        for (BindingConfig binding : bindings) {
            if (binding.getDegradationStrategy() != com.hcsc.datalake.mqintake.core.failure.DegradationStrategy.BISECT) {
                continue;
            }
            boolean hasBackoutQueue = binding.getBackoutQueue() != null &&
                    !binding.getBackoutQueue().isBlank();
            if (!hasBackoutQueue || binding.getBatchSize() <= 1) {
                continue;
            }

            int bisectDepth = 32 - Integer.numberOfLeadingZeros(binding.getBatchSize() - 1); // ceil(log2)
            int minThreshold = bisectDepth + 1;
            if (binding.getBackoutThreshold() < minThreshold) {
                errors.add("Binding '" + binding.getId() + "' uses BISECT with batch_size " +
                        binding.getBatchSize() + " but backout_threshold " +
                        binding.getBackoutThreshold() + " < required minimum " + minThreshold +
                        " (ceil(log2(batch_size)) + 1) — clean messages sharing failing batches " +
                        "with a poison message would be misrouted to the backout queue");
            }
        }
    }

    private void validateHdfsPaths(List<BindingConfig> bindings, List<String> errors) {
        for (BindingConfig binding : bindings) {
            if (binding.getHdfsBasePath() == null || binding.getHdfsBasePath().isBlank()) {
                continue; // Will be caught by required fields validation
            }

            HdfsPathValidator.PathValidationResult result =
                    hdfsPathValidator.validatePath(binding.getHdfsBasePath());

            if (!result.isValid()) {
                errors.add("Binding '" + binding.getId() + "' HDFS path not writable: " +
                        result.getError());
            }
        }
    }

    private String formatBytes(long bytes) {
        if (bytes >= 1_073_741_824) {
            return String.format("%.2f GB", bytes / 1_073_741_824.0);
        } else if (bytes >= 1_048_576) {
            return String.format("%.2f MB", bytes / 1_048_576.0);
        } else if (bytes >= 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        }
        return bytes + " bytes";
    }
}
