package com.hcsc.datalake.mqintake.core.config;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.function.Supplier;

/**
 * Enables intake configuration properties.
 */
@Configuration
@EnableConfigurationProperties(IntakeProperties.class)
public class IntakeConfiguration {

    /**
     * Refuses to start when a pre-grouping binding key is still configured
     * anywhere (YAML, system property, environment variable). Runs at context
     * refresh, before any listener starts, so a stale override fails loudly
     * instead of being silently ignored.
     */
    @Autowired
    void detectLegacyBindingKeys(ConfigurableEnvironment environment) {
        LegacyBindingKeyDetector.failOnLegacyKeys(environment);
    }

    /**
     * Single binding health manager shared by the runtime (which records
     * transitions) and the actuator health indicator (which reports them).
     */
    @Bean
    public BindingHealthManager bindingHealthManager() {
        return new BindingHealthManager();
    }

    // --- Empty binding-group handling -----------------------------------
    //
    // YAML like `tracker:` with nothing under it (typically the contents were
    // commented out) reaches the binder as a blank scalar, which used to fail
    // with a raw ConverterNotFoundException stack trace. These converters
    // accept the blank scalar as "use the group's defaults", after which the
    // ordinary validation rules produce the codebase's clear, actionable
    // errors (e.g. a TRACKED binding without tracker.queue). A NON-blank
    // scalar is still a genuine mistake and says so.

    @Bean
    @ConfigurationPropertiesBinding
    public Converter<String, BindingConfig.Batch> emptyBatchGroup() {
        return new BlankGroupConverter<BindingConfig.Batch>(BindingConfig.Batch::new, "batch") {
        };
    }

    @Bean
    @ConfigurationPropertiesBinding
    public Converter<String, BindingConfig.Hdfs> emptyHdfsGroup() {
        return new BlankGroupConverter<BindingConfig.Hdfs>(BindingConfig.Hdfs::new, "hdfs") {
        };
    }

    @Bean
    @ConfigurationPropertiesBinding
    public Converter<String, BindingConfig.Tracker> emptyTrackerGroup() {
        return new BlankGroupConverter<BindingConfig.Tracker>(
                BindingConfig.Tracker::new, "tracker") {
        };
    }

    @Bean
    @ConfigurationPropertiesBinding
    public Converter<String, BindingConfig.Backout> emptyBackoutGroup() {
        return new BlankGroupConverter<BindingConfig.Backout>(
                BindingConfig.Backout::new, "backout") {
        };
    }

    @Bean
    @ConfigurationPropertiesBinding
    public Converter<String, BindingConfig.Audit> emptyAuditGroup() {
        return new BlankGroupConverter<BindingConfig.Audit>(BindingConfig.Audit::new, "audit") {
        };
    }

    @Bean
    @ConfigurationPropertiesBinding
    public Converter<String, BindingConfig.Degradation> emptyDegradationGroup() {
        return new BlankGroupConverter<BindingConfig.Degradation>(
                BindingConfig.Degradation::new, "degradation") {
        };
    }

    /**
     * Shared implementation. Each bean returns an anonymous subclass with the
     * target type spelled out, because Spring resolves a converter's
     * {@code <S, T>} from the class's generic supertype — a lambda erases
     * them and fails at bind time with "unable to determine source type".
     */
    private abstract static class BlankGroupConverter<T> implements Converter<String, T> {
        private final Supplier<T> defaults;
        private final String group;

        BlankGroupConverter(Supplier<T> defaults, String group) {
            this.defaults = defaults;
            this.group = group;
        }

        @Override
        public T convert(String source) {
            if (source.isBlank()) {
                return defaults.get();
            }
            throw new IllegalArgumentException(
                    "Binding group '" + group + "' must be a nested group of properties "
                            + "(e.g. '" + group + ".…'), not the value '" + source + "'. "
                            + "Remove the key to use defaults, or supply its fields.");
        }
    }
}
