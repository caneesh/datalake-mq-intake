package com.hcsc.datalake.mqintake.core.preflight;

/**
 * One independently runnable probe of a single dependency.
 *
 * <p>Preflight exists because the application's dependencies can only be
 * proven against the real environment, and a failed startup tells you
 * <em>that</em> something is wrong rather than <em>which</em> thing. A check
 * isolates one fact — "the backout queue is reachable on the queue manager we
 * connected to", "a file can be written, hsynced, renamed and read back" — so
 * an operator gets an answer per dependency instead of one stack trace.
 *
 * <p>Checks are read-mostly by contract. Nothing here consumes a message,
 * sends to a queue another system reads, or writes into a data partition:
 * preflight runs against real environments, including ones with live data.
 * Where a check must write, it writes inside {@code _tmp/{instanceId}} and
 * removes what it wrote — the same tree the startup sweep already owns.
 */
public interface PreflightCheck {

    /** Coarse grouping for {@code --preflight=<group>}: mq, hdfs, app. */
    String group();

    /** Stable identifier, e.g. {@code mq.backout-queue.output}. */
    String name();

    /** One line describing what a pass proves. */
    String describes();

    /**
     * Runs the probe.
     *
     * <p>Must not throw: a check that cannot complete reports
     * {@link CheckOutcome#fail} with the reason. Preflight's job is to report
     * every dependency's state in one pass, which an escaping exception would
     * cut short.
     */
    CheckOutcome run();
}
