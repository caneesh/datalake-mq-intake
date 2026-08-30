package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.config.InstanceId;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionProvider;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Wires preflight and runs it instead of the service.
 *
 * <p>Deliberately in-context rather than a standalone tool: the value is in
 * probing exactly the wiring production uses — the same properties, the same
 * connection manager, the same {@code FileSystem} with the same Kerberos
 * identity. A separate utility with its own configuration would prove
 * something adjacent to the truth.
 *
 * <p>The process exits with a non-zero status when any check fails, so a
 * deployment pipeline can gate on it.
 */
@Configuration
@ConditionalOnProperty(prefix = "intake.preflight", name = "enabled", havingValue = "true")
public class PreflightConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PreflightConfiguration.class);

    @Bean
    public PreflightRunner preflightRunner(IntakeProperties properties,
                                           ProductionMode productionMode,
                                           InstanceId instanceId,
                                           FileSystem fileSystem,
                                           MqConnectionProvider mqConnections,
                                           RecordSerializerFactory serializerFactory,
                                           @Autowired(required = false)
                                           TrackerMessageBuilderFactory trackerFactory,
                                           @Autowired(required = false)
                                           com.hcsc.datalake.mqintake.core.security.KerberosManager
                                                   kerberosManager) {
        List<PreflightCheck> checks = new ArrayList<>();
        // A null manager with Kerberos enabled means the login failed and was
        // deferred so this report could be printed at all.
        boolean kerberosLoggedIn = kerberosManager != null;
        checks.addAll(AppChecks.forAllBindings(
                properties, productionMode, serializerFactory, trackerFactory, kerberosLoggedIn));
        checks.addAll(MqChecks.forAllBindings(properties, mqConnections));
        checks.addAll(HdfsChecks.forAllBindings(properties, fileSystem, instanceId.value()));
        return new PreflightRunner(checks, properties.getPreflight().getCheckTimeoutMs());
    }

    @Bean
    public ApplicationRunner preflightApplicationRunner(PreflightRunner runner,
                                                        IntakeProperties properties,
                                                        ApplicationContext context) {
        return (ApplicationArguments args) -> {
            Set<String> groups = new LinkedHashSet<>(properties.getPreflight().getOnly());
            // --preflight=mq is the form an operator will reach for; the
            // property form stays available for manifests.
            groups.addAll(args.getOptionValues("preflight") == null
                    ? List.of() : args.getOptionValues("preflight"));

            PreflightReport report = runner.run(groups);
            // System.out, not the log: this is the tool's output, and it must
            // be readable without a logging config that happens to cooperate.
            System.out.println(report.render());

            int exitCode = report.hasFailures() ? 1 : 0;
            log.info("Preflight complete: {} failure(s), exiting with status {}",
                    report.count(CheckOutcome.Status.FAIL), exitCode);
            System.exit(SpringApplication.exit(context, () -> exitCode));
        };
    }
}
