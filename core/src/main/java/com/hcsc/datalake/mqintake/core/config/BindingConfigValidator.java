package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.config.validation.AggregateMemoryRule;
import com.hcsc.datalake.mqintake.core.config.validation.BackoutQueueConsistencyRule;
import com.hcsc.datalake.mqintake.core.config.validation.BatchSizeRule;
import com.hcsc.datalake.mqintake.core.config.validation.BindingConfigRule;
import com.hcsc.datalake.mqintake.core.config.validation.BisectBackoutThresholdRule;
import com.hcsc.datalake.mqintake.core.config.validation.HdfsPathWritableRule;
import com.hcsc.datalake.mqintake.core.config.validation.RequiredFieldsRule;
import com.hcsc.datalake.mqintake.core.config.validation.TrackerQueueConsistencyRule;
import com.hcsc.datalake.mqintake.core.config.validation.UniqueBindingIdsRule;
import com.hcsc.datalake.mqintake.core.config.validation.UniqueSourceQueuesRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Runs every configuration rule and reports everything that is wrong.
 *
 * <p>This class used to be the rules as well as the runner: nine private
 * methods, each appending to a shared mutable error list, none reachable from a
 * test on its own. It now composes {@link BindingConfigRule}s, so each check is
 * a small class with one reason to change and adding a check does not mean
 * editing this file.
 *
 * <p>All rules run even after one fails. An operator fixing a bad configuration
 * should see every problem at once rather than rediscovering them one restart
 * at a time.
 */
@Component
public class BindingConfigValidator {

    private static final Logger log = LoggerFactory.getLogger(BindingConfigValidator.class);

    private final List<BindingConfigRule> rules;

    @Autowired
    public BindingConfigValidator(HdfsPathValidator hdfsPathValidator) {
        this(hdfsPathValidator, () -> Runtime.getRuntime().maxMemory());
    }

    /** Visible for testing: lets a test supply a heap size. */
    BindingConfigValidator(HdfsPathValidator hdfsPathValidator, LongSupplier maxHeapSupplier) {
        this(defaultRules(hdfsPathValidator, maxHeapSupplier));
    }

    /** Any rule set — the seam that makes the policy replaceable. */
    public BindingConfigValidator(List<BindingConfigRule> rules) {
        Objects.requireNonNull(rules, "rules required");
        this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
    }

    private static List<BindingConfigRule> defaultRules(HdfsPathValidator hdfsPathValidator,
                                                        LongSupplier maxHeapSupplier) {
        return List.of(
                new UniqueBindingIdsRule(),
                new UniqueSourceQueuesRule(),
                new TrackerQueueConsistencyRule(),
                new BackoutQueueConsistencyRule(),
                new BatchSizeRule(),
                new AggregateMemoryRule(maxHeapSupplier),
                new RequiredFieldsRule(),
                new BisectBackoutThresholdRule(),
                new HdfsPathWritableRule(hdfsPathValidator));
    }

    /**
     * Validates all bindings.
     *
     * @throws BindingConfigurationException if any rule reports a problem
     */
    public void validate(IntakeProperties properties) {
        List<BindingConfig> bindings = properties.getBindings();

        if (bindings == null || bindings.isEmpty()) {
            throw new BindingConfigurationException("No bindings configured");
        }

        List<String> errors = new ArrayList<>();
        for (BindingConfigRule rule : rules) {
            errors.addAll(rule.validate(properties));
        }

        if (!errors.isEmpty()) {
            String message = "Binding configuration validation failed:\n  - "
                    + String.join("\n  - ", errors);
            log.error(message);
            throw new BindingConfigurationException(message);
        }

        log.info("Binding configuration validated successfully: {} binding(s)", bindings.size());
    }

    /** The rules in the order they run, for diagnostics and tests. */
    public List<BindingConfigRule> rules() {
        return rules;
    }

}
