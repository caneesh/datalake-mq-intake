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
        List<PreflightCheck> checks = new ArrayList<>();
        checks.add(productionGates(productionMode));
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
                        if (productionMode.isEnabled()) {
                            return CheckOutcome.fail(type + " is a PLACEHOLDER: " + reason,
                                    "Production mode refuses to start with it. This binding "
                                            + "cannot be promoted until the serializer is final.");
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
