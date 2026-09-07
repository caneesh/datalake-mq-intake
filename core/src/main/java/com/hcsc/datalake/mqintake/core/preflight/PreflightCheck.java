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

    /**
     * A check built from its three constants and the probe itself.
     *
     * <p>Every check is the same shape: three strings and one method that does
     * the work. Written as anonymous subclasses that was four overrides each,
     * three of them {@code return field;} — fifteen times over, with the probe
     * buried in the middle.
     *
     * <p>It also removes a wart that shape created: the shared base class
     * lived inside {@code MqChecks}, so {@code HdfsChecks} and
     * {@code AppChecks} each referenced the MQ class for a base class and
     * nothing else.
     *
     * @param group     one of mq, hdfs, app — what {@code --preflight=<group>}
     *                  filters on, so a wrong one means the check silently
     *                  does not run under that filter
     * @param name      stable identifier, e.g. {@code mq.backout-queue.output}
     * @param describes one line saying what a pass proves
     * @param probe     the work; must not throw, per {@link #run()}
     */
    static PreflightCheck of(String group, String name, String describes,
                             java.util.function.Supplier<CheckOutcome> probe) {
        return new PreflightCheck() {
            @Override
            public String group() {
                return group;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public String describes() {
                return describes;
            }

            @Override
            public CheckOutcome run() {
                return probe.get();
            }
        };
    }
}
