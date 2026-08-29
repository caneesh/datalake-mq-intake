package com.hcsc.datalake.mqintake.core.preflight;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The runner's contract: run everything asked for, survive a check that
 * misbehaves, and produce a report an operator can act on.
 */
class PreflightRunnerTest {

    private PreflightCheck check(String group, String name, CheckOutcome outcome) {
        return new MqChecks.AbstractCheck(group, name, "proves " + name) {
            @Override
            public CheckOutcome run() {
                return outcome;
            }
        };
    }

    @Test
    void runsEveryCheckEvenAfterOneFails() {
        AtomicInteger ran = new AtomicInteger();
        PreflightCheck counting = new MqChecks.AbstractCheck("hdfs", "later", "runs last") {
            @Override
            public CheckOutcome run() {
                ran.incrementAndGet();
                return CheckOutcome.pass("fine");
            }
        };

        PreflightReport report = new PreflightRunner(List.of(
                check("mq", "broken", CheckOutcome.fail("nope")), counting)).run(Set.of());

        assertThat(ran.get())
                .as("a failure must not abort the pass — an operator wants the whole picture")
                .isEqualTo(1);
        assertThat(report.hasFailures()).isTrue();
        assertThat(report.count(CheckOutcome.Status.PASS)).isEqualTo(1);
    }

    @Test
    void aCheckThatThrowsIsReportedRatherThanEscaping() {
        PreflightCheck exploding = new MqChecks.AbstractCheck("mq", "explodes", "throws") {
            @Override
            public CheckOutcome run() {
                throw new IllegalStateException("kaboom");
            }
        };

        PreflightReport report = new PreflightRunner(List.of(exploding)).run(Set.of());

        assertThat(report.hasFailures()).isTrue();
        assertThat(report.getEntries().get(0).getOutcome().getDetail())
                .contains("check threw").contains("kaboom");
    }

    @Test
    void groupFilterSelectsOnlyThatComponent() {
        PreflightRunner runner = new PreflightRunner(List.of(
                check("mq", "a", CheckOutcome.pass("x")),
                check("hdfs", "b", CheckOutcome.pass("y")),
                check("app", "c", CheckOutcome.pass("z"))));

        assertThat(runner.run(Set.of("hdfs")).getEntries()).hasSize(1);
        assertThat(runner.run(Set.of("hdfs", "mq")).getEntries()).hasSize(2);
        assertThat(runner.run(Set.of()).getEntries())
                .as("no filter means everything").hasSize(3);
        assertThat(runner.run(Set.of("HDFS ")).getEntries())
                .as("case and whitespace tolerated — this comes off a command line").hasSize(1);
    }

    @Test
    void skippedChecksAreNotFailures() {
        PreflightReport report = new PreflightRunner(List.of(
                check("app", "n/a", CheckOutcome.skip("LAND_ONLY binding")))).run(Set.of());

        assertThat(report.hasFailures()).isFalse();
        assertThat(report.count(CheckOutcome.Status.SKIP)).isEqualTo(1);
    }

    @Test
    void theReportLeadsWithTheVerdictAndRepeatsFailuresWithTheirRemedy() {
        PreflightReport report = new PreflightRunner(List.of(
                check("mq", "rms.backout-queue.output",
                        CheckOutcome.fail("MQRC 2085 unknown object name",
                                "define the queue on the connected queue manager")),
                check("hdfs", "rms.landing-path", CheckOutcome.pass("writable"))))
                .run(Set.of());

        String rendered = report.render();

        assertThat(rendered).contains("PREFLIGHT FAILED");
        assertThat(rendered).contains("1 passed, 1 failed, 0 skipped");
        assertThat(rendered).contains("[FAIL]").contains("[ ok ]");
        assertThat(rendered)
                .as("a failure must carry what it proves and what to do about it")
                .contains("proves :").contains("fix    :")
                .contains("define the queue on the connected queue manager");
    }

    @Test
    void aCleanRunSaysSoUnambiguously() {
        String rendered = new PreflightRunner(List.of(
                check("mq", "rms.connection", CheckOutcome.pass("connected"))))
                .run(Set.of()).render();

        assertThat(rendered).contains("PREFLIGHT PASSED");
        assertThat(rendered).doesNotContain("FAILURES");
    }

    @Test
    void failureDetailUnwrapsTheCauseChainWhereTheRealReasonUsuallyIs() {
        CheckOutcome outcome = CheckOutcome.fail("cannot open queue",
                new RuntimeException("wrapper",
                        new IllegalStateException("MQRC 2035 not authorized")),
                "grant access");

        assertThat(outcome.getDetail())
                .contains("cannot open queue")
                .contains("wrapper")
                .contains("MQRC 2035 not authorized");
    }
}
