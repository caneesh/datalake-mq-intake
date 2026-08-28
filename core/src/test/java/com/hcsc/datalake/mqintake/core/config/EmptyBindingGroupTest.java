package com.hcsc.datalake.mqintake.core.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An explicit-but-empty binding group in configuration — typically a group
 * header left behind after its contents were commented out.
 *
 * <p>Before the converters in {@link IntakeConfiguration}, the binder tried
 * to convert the blank scalar into the group class and failed with a raw
 * {@code ConverterNotFoundException} stack trace. Now a blank group means
 * "use the group's defaults", after which the ordinary validation rules
 * produce this codebase's clear errors; a non-blank scalar still fails, with
 * a message that says what a group is.
 */
class EmptyBindingGroupTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(IntakeConfiguration.class);

    @Test
    void aBlankGroupBindsToItsDefaults() {
        runner.withPropertyValues(
                        "intake.bindings[0].id=rms",
                        "intake.bindings[0].tracker=",
                        "intake.bindings[0].backout=",
                        "intake.bindings[0].audit=")
                .run(context -> {
                    assertThat(context.getStartupFailure()).isNull();
                    IntakeProperties properties = context.getBean(IntakeProperties.class);
                    BindingConfig binding = properties.getBindings().get(0);
                    assertThat(binding.getTracker().getQueue()).isNull();
                    assertThat(binding.getBackout().getThreshold()).isEqualTo(5);
                    assertThat(binding.getAudit().isFailBatchOnError()).isTrue();
                    assertThat(binding.getAudit().isBalanceCheckEnabled()).isFalse();
                });
    }

    @Test
    void aNonBlankScalarWhereAGroupBelongsFailsWithAClearMessage() {
        runner.withPropertyValues(
                        "intake.bindings[0].id=rms",
                        "intake.bindings[0].tracker=oops")
                .run(context -> assertThat(context.getStartupFailure())
                        .isNotNull()
                        .hasStackTraceContaining("must be a nested group of properties"));
    }

    @Test
    void nestedGroupPropertiesStillBindNormally() {
        runner.withPropertyValues(
                        "intake.bindings[0].id=rms",
                        "intake.bindings[0].tracker.queue=MQ.TRACKER",
                        "intake.bindings[0].audit.balance-check-enabled=true")
                .run(context -> {
                    assertThat(context.getStartupFailure()).isNull();
                    BindingConfig binding =
                            context.getBean(IntakeProperties.class).getBindings().get(0);
                    assertThat(binding.getTracker().getQueue()).isEqualTo("MQ.TRACKER");
                    assertThat(binding.getAudit().isBalanceCheckEnabled()).isTrue();
                });
    }
}
