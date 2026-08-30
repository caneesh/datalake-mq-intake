package com.hcsc.datalake.mqintake.core.preflight;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    /**
     * Long enough for a slow-but-working dependency, short enough that an
     * operator watching a terminal learns something.
     */
    public static final long DEFAULT_CHECK_TIMEOUT_MS = 30_000;

    private final List<PreflightCheck> checks;
    private final long checkTimeoutMs;

    public PreflightRunner(List<PreflightCheck> checks) {
        this(checks, DEFAULT_CHECK_TIMEOUT_MS);
    }

    public PreflightRunner(List<PreflightCheck> checks, long checkTimeoutMs) {
        this.checks = List.copyOf(checks);
        this.checkTimeoutMs = checkTimeoutMs > 0 ? checkTimeoutMs : DEFAULT_CHECK_TIMEOUT_MS;
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
            CheckOutcome outcome = runWithTimeout(check);
            report.add(check, outcome.withDuration(
                    Duration.ofNanos(System.nanoTime() - startedNanos)));
        }

        if (report.getEntries().isEmpty()) {
            log.warn("No preflight checks matched groups {} — known groups are mq, hdfs, app",
                    wanted);
        }
        return report;
    }

    /**
     * Runs one check on its own thread, bounded.
     *
     * <p>An unreachable dependency does not fail quickly. Hadoop's retry
     * handler sleeps and retries a NameNode it cannot resolve, and a JMS
     * connect against a host that drops packets waits on the socket — so the
     * probe blocks for minutes with nothing printed. That inverts the purpose
     * of a diagnostic: it is run precisely when connectivity is in doubt, and
     * hanging is the least useful thing it can do about it.
     *
     * <p>The thread is interrupted on timeout but not waited for. Blocked
     * native or retry-loop code may ignore the interrupt, and the remaining
     * checks matter more than a tidy shutdown of this one — hence a daemon
     * thread, which cannot hold the JVM open once the report is printed.
     */
    private CheckOutcome runWithTimeout(PreflightCheck check) {
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "preflight-" + check.name());
            thread.setDaemon(true);
            return thread;
        });
        try {
            Future<CheckOutcome> future = executor.submit(check::run);
            try {
                return future.get(checkTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                return CheckOutcome.fail(
                        "did not answer within " + (checkTimeoutMs / 1000) + "s",
                        "The dependency accepted the request and never responded, which usually "
                                + "means the address resolves but nothing is listening, or a "
                                + "firewall is dropping packets rather than refusing them. For "
                                + "HDFS, an unresolvable NameNode leaves the client retrying: "
                                + "check the host can resolve and reach every NameNode and "
                                + "DataNode in the cluster's configuration.");
            } catch (ExecutionException e) {
                // A check that throws is itself a defect, but it must not take
                // the remaining checks down with it.
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                return CheckOutcome.fail("check threw instead of reporting", cause,
                        "This is a bug in the check, not necessarily in the environment.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CheckOutcome.fail("interrupted before it could report", e, null);
            }
        } catch (Throwable t) {
            return CheckOutcome.fail("check could not be started", t,
                    "This is a bug in the check, not necessarily in the environment.");
        } finally {
            executor.shutdownNow();
        }
    }
}
