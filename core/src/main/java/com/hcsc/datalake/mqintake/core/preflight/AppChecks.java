package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import com.hcsc.datalake.mqintake.core.orchestration.TrackerMessageBuilderFactory;
import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Probes the parts that are neither MQ nor HDFS: which safety gates this
 * process is running with, and whether the binding's own components can be
 * built at all.
 *
 * <p>The gate summary is not a test — it is the answer to "what mode am I
 * actually in", which decides whether a misconfiguration would have been
 * refused at startup or silently accepted.
 */
public final class AppChecks {

    private AppChecks() {
    }

    public static List<PreflightCheck> forAllBindings(IntakeProperties properties,
                                                      ProductionMode productionMode,
                                                      RecordSerializerFactory serializerFactory,
                                                      TrackerMessageBuilderFactory trackerFactory) {
        return forAllBindings(properties, productionMode, serializerFactory, trackerFactory, true);
    }

    public static List<PreflightCheck> forAllBindings(IntakeProperties properties,
                                                      ProductionMode productionMode,
                                                      RecordSerializerFactory serializerFactory,
                                                      TrackerMessageBuilderFactory trackerFactory,
                                                      boolean kerberosLoggedIn) {
        List<PreflightCheck> checks = new ArrayList<>();
        checks.add(productionGates(productionMode));
        checks.add(kerberosIdentity(properties, kerberosLoggedIn));
        for (BindingConfig binding : properties.getBindings()) {
            checks.add(serializerBuildable(binding, serializerFactory, productionMode));
            checks.add(trackerBuildable(binding, trackerFactory));
            checks.add(controlSummary(binding));
        }
        return checks;
    }

    private static PreflightCheck productionGates(ProductionMode productionMode) {
        return new MqChecks.AbstractCheck("app", "production-mode",
                "whether the startup safety gates are armed in this process") {
            @Override
            public CheckOutcome run() {
                if (productionMode.isEnabled()) {
                    return CheckOutcome.pass("ARMED — dev-default connections, placeholder "
                            + "serializers and incomplete tracker contracts all refuse startup");
                }
                return CheckOutcome.pass("not armed (development posture) — gates are advisory "
                        + "only; set the prod profile or MQ_INTAKE_PRODUCTION=true to arm them");
            }
        };
    }

    /**
     * The identity every HDFS operation will run as.
     *
     * <p>Checked here rather than left to the first write because the two
     * things most likely to be wrong on a first deployment — a keytab path
     * that does not exist, and one that exists but is unreadable by the
     * account this service runs as — are both silent until something tries to
     * authenticate, and then surface as a stack trace rather than a sentence.
     */
    private static PreflightCheck kerberosIdentity(IntakeProperties properties,
                                                   boolean loggedIn) {
        return new MqChecks.AbstractCheck("app", "kerberos",
                "the service can authenticate as the identity it will write with") {
            @Override
            public CheckOutcome run() {
                IntakeProperties.KerberosProperties kerberos = properties.getKerberos();
                if (!kerberos.isEnabled()) {
                    return CheckOutcome.skip("disabled — the service will use simple "
                            + "authentication, which a secured cluster rejects");
                }
                if (kerberos.getPrincipal() == null || kerberos.getPrincipal().isBlank()) {
                    return CheckOutcome.fail("enabled but no principal configured",
                            "Set intake.kerberos.principal (KERBEROS_PRINCIPAL).");
                }
                String keytabPath = kerberos.getKeytabPath();
                if (keytabPath == null || keytabPath.isBlank()) {
                    return CheckOutcome.fail("enabled but no keytab configured",
                            "Set intake.kerberos.keytab-path (KERBEROS_KEYTAB_PATH).");
                }
                java.io.File keytab = new java.io.File(keytabPath);
                if (!keytab.exists()) {
                    return CheckOutcome.fail("keytab does not exist: " + keytab.getAbsolutePath(),
                            "Check the path. If it is right, the file may live on a host this "
                                    + "process is not running on.");
                }
                if (!keytab.canRead()) {
                    return CheckOutcome.fail("keytab is not readable: " + keytab.getAbsolutePath(),
                            "It is readable by someone — commonly the account that owns the "
                                    + "application it was issued for, which may not be the "
                                    + "account running this service. Grant read to this account "
                                    + "or run as that one.");
                }
                if (!loggedIn) {
                    return CheckOutcome.fail("login failed for " + kerberos.getPrincipal(),
                            "The keytab is present and readable, so the principal or the realm "
                                    + "is the problem: confirm the spelling against "
                                    + "'klist -kt " + keytab.getAbsolutePath() + "', and that "
                                    + "the KDC is reachable from this host.");
                }
                return CheckOutcome.pass("logged in as " + kerberos.getPrincipal()
                        + " from " + keytab.getAbsolutePath());
            }
        };
    }

    private static PreflightCheck serializerBuildable(BindingConfig binding,
                                                      RecordSerializerFactory factory,
                                                      ProductionMode productionMode) {
        return new MqChecks.AbstractCheck("app", binding.getId() + ".serializer",
                "the binding's record serializer can be built and is production-eligible") {
            @Override
            public CheckOutcome run() {
                try {
                    RecordSerializer serializer = factory.create(binding);
                    String type = serializer.getClass().getSimpleName();
                    String layout = serializer.getKeyClass().getSimpleName()
                            + " / " + serializer.getValueClass().getSimpleName();
                    if (serializer instanceof PlaceholderSerializer) {
                        String reason = ((PlaceholderSerializer) serializer).getPlaceholderReason();
                        if (productionMode.isEnabled() && binding.isAcceptPlaceholderSerializer()) {
                            // Deliberately not a silent pass: an accepted
                            // limitation that stops being visible is one nobody
                            // revisits.
                            return CheckOutcome.pass(type + " is a PLACEHOLDER, ACCEPTED for "
                                    + "production by configuration — " + reason);
                        }
                        if (productionMode.isEnabled()) {
                            return CheckOutcome.fail(type + " is a PLACEHOLDER: " + reason,
                                    "Production mode refuses to start with it. Finalise the "
                                            + "serializer, or set accept-placeholder-serializer "
                                            + "on this binding to accept the consequences — "
                                            + "narrower than disarming production mode, which "
                                            + "also switches off the local-filesystem refusal.");
                        }
                        return CheckOutcome.skip(type + " is a placeholder (" + reason
                                + ") — allowed outside production mode");
                    }
                    return CheckOutcome.pass(type + ", writing " + layout);
                } catch (Exception e) {
                    return CheckOutcome.fail("serializer could not be built", e, null);
                }
            }
        };
    }

    private static PreflightCheck trackerBuildable(BindingConfig binding,
                                                   TrackerMessageBuilderFactory factory) {
        return new MqChecks.AbstractCheck("app", binding.getId() + ".tracker-builder",
                "a TRACKED binding can build its tracker message builder") {
            @Override
            public CheckOutcome run() {
                if (binding.getMode() != BindingMode.TRACKED) {
                    return CheckOutcome.skip("LAND_ONLY binding — no tracker");
                }
                if (factory == null) {
                    return CheckOutcome.fail("no TrackerMessageBuilderFactory available",
                            "A TRACKED binding cannot start without one — the module must "
                                    + "provide the bean.");
                }
                try {
                    return CheckOutcome.pass(factory.create(binding).getClass().getSimpleName()
                            + " built; body-mode " + binding.getTracker().getBodyMode());
                } catch (Exception e) {
                    return CheckOutcome.fail("tracker builder could not be built", e,
                            "A contract-gap failure here is the production gate refusing an "
                                    + "incomplete legacy header rewrite.");
                }
            }
        };
    }

    private static PreflightCheck controlSummary(BindingConfig binding) {
        return new MqChecks.AbstractCheck("app", binding.getId() + ".controls",
                "which delivery and accounting controls this binding runs with") {
            @Override
            public CheckOutcome run() {
                String summary = String.format(
                        "mode=%s threads=%d batch=%d/%dB hsync=%s index=%s abc-balance=%s "
                                + "audit-fail-closed=%s backout-threshold=%d gated=%s",
                        binding.getMode(),
                        binding.getListenerThreads(),
                        binding.getBatch().getSize(),
                        binding.getBatch().getBytes(),
                        binding.getHdfs().isHsyncOnFlush(),
                        binding.getHdfs().isRecordIndexEnabled(),
                        binding.getAudit().isBalanceCheckEnabled(),
                        binding.getAudit().isFailBatchOnError(),
                        binding.getBackout().getThreshold(),
                        binding.getBackout().isRouteOnlyOnDataFailures());
                return CheckOutcome.pass(summary);
            }
        };
    }
}
