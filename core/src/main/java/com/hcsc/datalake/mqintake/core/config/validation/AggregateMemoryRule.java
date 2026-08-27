package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * The batches every binding can hold at once must fit in the heap.
 *
 * <p>Batch memory is {@code batch_bytes × listener_threads} per binding, all
 * of it live simultaneously. A fixed ceiling in configuration has no
 * relationship to the {@code -Xmx} a container is actually given, so when none
 * is configured the ceiling is derived from the heap instead.
 */
public class AggregateMemoryRule implements BindingConfigRule {

    private static final Logger log = LoggerFactory.getLogger(AggregateMemoryRule.class);

    /**
     * Fraction of max heap used as the ceiling when none is configured.
     * Deliberately conservative: {@code batch_bytes} counts estimated payload
     * size, while the retained cost of the JMS messages holding it — object
     * overhead, UTF-16 strings, MQ client buffers — is materially higher. The
     * unused half is that headroom, plus room for everything else.
     */
    static final double DEFAULT_HEAP_FRACTION = 0.5;

    /**
     * An explicitly configured ceiling above this fraction of max heap is
     * treated as a misconfiguration rather than an intention: batches alone
     * would leave the process no room to run.
     */
    static final double MAX_SAFE_HEAP_FRACTION = 0.7;

    private final LongSupplier maxHeapSupplier;

    public AggregateMemoryRule() {
        this(() -> Runtime.getRuntime().maxMemory());
    }

    public AggregateMemoryRule(LongSupplier maxHeapSupplier) {
        this.maxHeapSupplier = maxHeapSupplier;
    }

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();
        List<BindingConfig> bindings = properties.getBindings();
        long configuredCeiling = properties.getAggregateMemoryCeilingBytes();

        long maxHeap = maxHeapSupplier.getAsLong();
        long safeCeiling = (long) (maxHeap * MAX_SAFE_HEAP_FRACTION);

        long ceiling;
        if (configuredCeiling > 0) {
            ceiling = configuredCeiling;
            // A ceiling the heap cannot honour is worse than no ceiling: it
            // reports "validated" and then OOMs under load.
            if (ceiling > safeCeiling) {
                errors.add("aggregate_memory_ceiling_bytes " + ByteFormat.format(ceiling)
                        + " exceeds " + (int) (MAX_SAFE_HEAP_FRACTION * 100)
                        + "% of JVM max heap (" + ByteFormat.format(maxHeap) + "). Batches alone "
                        + "would leave the process no room to run. Raise -Xmx, or lower the "
                        + "ceiling and the bindings' batch_bytes / listener_threads.");
                return errors;
            }
        } else {
            ceiling = (long) (maxHeap * DEFAULT_HEAP_FRACTION);
            log.info("aggregate_memory_ceiling_bytes not set — derived {} from {}% of max heap {}",
                    ByteFormat.format(ceiling), (int) (DEFAULT_HEAP_FRACTION * 100),
                    ByteFormat.format(maxHeap));
        }

        long total = 0;
        for (BindingConfig binding : bindings) {
            total += binding.getMemoryFootprint();
        }

        if (total > ceiling) {
            errors.add(overBudgetMessage(bindings, total, ceiling, configuredCeiling, maxHeap));
        }
        return errors;
    }

    /** Names every binding's contribution, so the operator can see what to cut. */
    private String overBudgetMessage(List<BindingConfig> bindings, long total, long ceiling,
                                     long configuredCeiling, long maxHeap) {
        StringBuilder detail = new StringBuilder();
        detail.append("Aggregate batch memory ").append(ByteFormat.format(total))
              .append(" exceeds ceiling ").append(ByteFormat.format(ceiling));
        if (configuredCeiling <= 0) {
            detail.append(" (").append((int) (DEFAULT_HEAP_FRACTION * 100))
                  .append("% of max heap ").append(ByteFormat.format(maxHeap)).append(")");
        }
        detail.append(". Per binding (batch_bytes × listener_threads): ");
        for (BindingConfig binding : bindings) {
            detail.append(binding.getId()).append("=")
                  .append(ByteFormat.format(binding.getBatch().getBytes())).append("×")
                  .append(binding.getListenerThreads()).append("=")
                  .append(ByteFormat.format(binding.getMemoryFootprint())).append(" ");
        }
        detail.append("— reduce batch_bytes or listener_threads, or raise -Xmx.");
        return detail.toString().trim();
    }
}
