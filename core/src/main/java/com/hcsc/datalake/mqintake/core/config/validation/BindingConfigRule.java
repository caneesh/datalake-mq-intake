package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.List;

/**
 * One independent check on the intake configuration.
 *
 * <p>Each rule answers a single question about the configuration and returns
 * what is wrong, rather than appending to a shared mutable list. That makes a
 * rule a pure function of the configuration: it can be exercised on its own,
 * and adding a check means adding a class rather than editing a method that
 * every other check also lives in.
 *
 * <p>Rules are independent by design — all of them run and all their findings
 * are reported together, so an operator fixing a bad config sees every problem
 * at once instead of rediscovering them one restart at a time.
 */
@FunctionalInterface
public interface BindingConfigRule {

    /**
     * @return the problems this rule found; empty when the configuration
     *         satisfies it
     */
    List<String> validate(IntakeProperties properties);

    default String name() {
        return getClass().getSimpleName();
    }
}
