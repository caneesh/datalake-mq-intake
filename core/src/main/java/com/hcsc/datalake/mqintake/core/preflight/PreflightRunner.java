package com.hcsc.datalake.mqintake.core.preflight;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runs preflight checks and renders the report.
 *
 * <p>Every check runs even after one fails: an operator wants the whole
 * picture of a fresh environment in one pass, not the first thing that broke
 * followed by a re-run for the next.
 */
public final class PreflightRunner {

    private static final Logger log = LoggerFactory.getLogger(PreflightRunner.class);

    private final List<PreflightCheck> checks;

    public PreflightRunner(List<PreflightCheck> checks) {
        this.checks = List.copyOf(checks);
    }

    /**
     * @param groups groups to run (mq, hdfs, app); empty runs everything
     */
    public PreflightReport run(Set<String> groups) {
        PreflightReport report = new PreflightReport();
        Set<String> wanted = groups.stream()
                .map(g -> g.trim().toLowerCase(Locale.ROOT))
                .filter(g -> !g.isEmpty())
                .collect(Collectors.toSet());

        for (PreflightCheck check : checks) {
            if (!wanted.isEmpty() && !wanted.contains(check.group())) {
                continue;
            }
            long startedNanos = System.nanoTime();
            CheckOutcome outcome;
            try {
                outcome = check.run();
            } catch (Throwable t) {
                // A check that throws is itself a defect, but it must not take
                // the remaining checks down with it.
                outcome = CheckOutcome.fail("check threw instead of reporting", t,
                        "This is a bug in the check, not necessarily in the environment.");
            }
            report.add(check, outcome.withDuration(
                    Duration.ofNanos(System.nanoTime() - startedNanos)));
        }

        if (report.getEntries().isEmpty()) {
            log.warn("No preflight checks matched groups {} — known groups are mq, hdfs, app",
                    wanted);
        }
        return report;
    }
}
