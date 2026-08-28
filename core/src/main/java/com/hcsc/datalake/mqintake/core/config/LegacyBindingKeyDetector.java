package com.hcsc.datalake.mqintake.core.config;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fails startup when a pre-grouping binding property key is still present.
 *
 * <p>The 2026-08 migration nested {@link BindingConfig}'s flat keys into
 * groups ({@code batch-size} → {@code batch.size}, {@code backout-queue} →
 * {@code backout.queue}, …). Spring silently ignores keys that no longer bind,
 * which is the worst possible failure mode for this migration: an
 * environment-specific override of, say, the backout queue would simply stop
 * applying, and the binding would run with the checked-in default. This
 * detector turns that silence into a refusal to start that names each stale
 * key and its replacement.
 *
 * <p>Two forms are scanned:
 * <ul>
 *   <li><strong>Dotted keys</strong> ({@code intake.bindings[0].batch-size})
 *       from YAML, properties files, and {@code -D} overrides — every legacy
 *       flat key is caught, since none of them bind any more.</li>
 *   <li><strong>Environment-variable keys</strong>
 *       ({@code INTAKE_BINDINGS_0_…}) — only the five whose relaxed-binding
 *       form no longer maps are caught. The rest
 *       ({@code INTAKE_BINDINGS_0_BATCH_SIZE}, {@code …_BACKOUT_QUEUE}, …)
 *       still bind, because underscores map to both {@code -} and {@code .}
 *       and the group/leaf split preserves the word boundaries.</li>
 * </ul>
 */
public final class LegacyBindingKeyDetector {

    /** Legacy dotted leaf (relaxed forms) → replacement, for the error message. */
    private static final Map<String, String> DOTTED_REPLACEMENTS = new LinkedHashMap<>();

    static {
        DOTTED_REPLACEMENTS.put("batch-size|batchSize|batch_size", "batch.size");
        DOTTED_REPLACEMENTS.put("batch-bytes|batchBytes|batch_bytes", "batch.bytes");
        DOTTED_REPLACEMENTS.put("batch-interval-ms|batchIntervalMs|batch_interval_ms", "batch.interval-ms");
        DOTTED_REPLACEMENTS.put("hdfs-base-path|hdfsBasePath|hdfs_base_path", "hdfs.base-path");
        DOTTED_REPLACEMENTS.put("record-index-enabled|recordIndexEnabled|record_index_enabled", "hdfs.record-index-enabled");
        DOTTED_REPLACEMENTS.put("hsync-on-flush|hsyncOnFlush|hsync_on_flush", "hdfs.hsync-on-flush");
        DOTTED_REPLACEMENTS.put("tracker-queue|trackerQueue|tracker_queue", "tracker.queue");
        DOTTED_REPLACEMENTS.put("tracker-body-mode|trackerBodyMode|tracker_body_mode", "tracker.body-mode");
        DOTTED_REPLACEMENTS.put("tracker-fields|trackerFields|tracker_fields", "tracker.fields");
        DOTTED_REPLACEMENTS.put("fail-batch-on-tracker-error|failBatchOnTrackerError|fail_batch_on_tracker_error",
                "tracker.fail-batch-on-error");
        DOTTED_REPLACEMENTS.put("backout-queue|backoutQueue|backout_queue", "backout.queue");
        DOTTED_REPLACEMENTS.put("backout-threshold|backoutThreshold|backout_threshold", "backout.threshold");
        DOTTED_REPLACEMENTS.put("backout-depth-poll-interval-ms|backoutDepthPollIntervalMs|backout_depth_poll_interval_ms",
                "backout.depth-poll-interval-ms");
        DOTTED_REPLACEMENTS.put("fail-batch-on-audit-error|failBatchOnAuditError|fail_batch_on_audit_error",
                "audit.fail-batch-on-error");
        DOTTED_REPLACEMENTS.put("degradation-strategy|degradationStrategy|degradation_strategy", "degradation.strategy");
        DOTTED_REPLACEMENTS.put("successes-required-to-restore|successesRequiredToRestore|successes_required_to_restore",
                "degradation.successes-required-to-restore");
    }

    private static final Pattern DOTTED = buildDottedPattern();

    /** Env-var forms whose relaxed mapping broke, with their replacement var. */
    private static final Map<String, String> ENV_REPLACEMENTS = Map.of(
            "RECORD_INDEX_ENABLED", "HDFS_RECORD_INDEX_ENABLED",
            "HSYNC_ON_FLUSH", "HDFS_HSYNC_ON_FLUSH",
            "FAIL_BATCH_ON_TRACKER_ERROR", "TRACKER_FAIL_BATCH_ON_ERROR",
            "FAIL_BATCH_ON_AUDIT_ERROR", "AUDIT_FAIL_BATCH_ON_ERROR",
            "SUCCESSES_REQUIRED_TO_RESTORE", "DEGRADATION_SUCCESSES_REQUIRED_TO_RESTORE");

    // CASE_INSENSITIVE: Spring's environment binding matches env-var names
    // case-insensitively, so a lowercase legacy variable binds nowhere just
    // like its uppercase form — and must be flagged just like it.
    private static final Pattern ENV = Pattern.compile(
            "^INTAKE_BINDINGS_(\\d+)_(" + String.join("|", ENV_REPLACEMENTS.keySet()) + ")$",
            Pattern.CASE_INSENSITIVE);

    private static Pattern buildDottedPattern() {
        StringBuilder alternatives = new StringBuilder();
        for (String legacyForms : DOTTED_REPLACEMENTS.keySet()) {
            if (alternatives.length() > 0) {
                alternatives.append('|');
            }
            alternatives.append(legacyForms);
        }
        // tracker-fields is itself an object, so match its sub-keys too.
        return Pattern.compile(
                "^intake\\.bindings\\[(\\d+)\\]\\.(" + alternatives + ")(\\..*)?$");
    }

    private LegacyBindingKeyDetector() {
    }

    /**
     * Scans every enumerable property source and throws when a legacy binding
     * key is found, listing each offender with its replacement.
     *
     * @throws IllegalStateException naming every stale key, its source, and
     *                               the key that replaces it
     */
    public static void failOnLegacyKeys(ConfigurableEnvironment environment) {
        List<String> offenders = new ArrayList<>();

        for (PropertySource<?> source : environment.getPropertySources()) {
            if (!(source instanceof EnumerablePropertySource)) {
                continue;
            }
            for (String name : ((EnumerablePropertySource<?>) source).getPropertyNames()) {
                Matcher dotted = DOTTED.matcher(name);
                if (dotted.matches()) {
                    offenders.add(String.format("%s (in %s) -> intake.bindings[%s].%s",
                            name, source.getName(), dotted.group(1),
                            replacementForDotted(dotted.group(2))));
                    continue;
                }
                Matcher env = ENV.matcher(name);
                if (env.matches()) {
                    // Uppercased before lookup: the pattern matches
                    // case-insensitively, the replacement map's keys do not.
                    offenders.add(String.format("%s (in %s) -> INTAKE_BINDINGS_%s_%s",
                            name, source.getName(), env.group(1),
                            ENV_REPLACEMENTS.get(env.group(2).toUpperCase(java.util.Locale.ROOT))));
                }
            }
        }

        if (!offenders.isEmpty()) {
            throw new IllegalStateException(
                    "Legacy binding configuration keys found. These keys no longer bind — "
                            + "starting anyway would silently run without the override. "
                            + "Rename them:\n  - " + String.join("\n  - ", offenders));
        }
    }

    private static String replacementForDotted(String matchedLeaf) {
        String leaf = matchedLeaf.contains(".")
                ? matchedLeaf.substring(0, matchedLeaf.indexOf('.'))
                : matchedLeaf;
        for (Map.Entry<String, String> entry : DOTTED_REPLACEMENTS.entrySet()) {
            for (String form : entry.getKey().split("\\|")) {
                if (form.equals(leaf)) {
                    return entry.getValue();
                }
            }
        }
        return "(see BindingConfig groups)";
    }
}
