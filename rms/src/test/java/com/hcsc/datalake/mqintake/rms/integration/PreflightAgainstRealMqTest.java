package com.hcsc.datalake.mqintake.rms.integration;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.mq.CredentialProvider;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.preflight.CheckOutcome;
import com.hcsc.datalake.mqintake.core.preflight.MqChecks;
import com.hcsc.datalake.mqintake.core.preflight.PreflightCheck;
import com.hcsc.datalake.mqintake.core.preflight.PreflightReport;
import com.hcsc.datalake.mqintake.core.preflight.PreflightRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Preflight against a real queue manager.
 *
 * <p>The MQ half of preflight is the half that matters operationally — the
 * deployment guide designates it as the substitute for MQ admin access, so its
 * verdicts stand in for a {@code DISPLAY QLOCAL} nobody can run. Those verdicts
 * come from opening real destinations and reading real MQRC codes, which an
 * embedded broker cannot reproduce: MQRC 2035 for a missing authority and MQRC
 * 2085 for an unknown object name are IBM MQ specifics, and they are what the
 * remedies in the report are written against.
 *
 * <p>Requires the queue manager from {@code docker-compose.yml}:
 *
 * <pre>
 *   docker-compose up -d ibm-mq
 *   export MQ_USER=app MQ_PASSWORD=passw0rd
 * </pre>
 *
 * <p>Skipped without {@code MQ_USER}, like the other real-MQ suites — a green
 * build without it proves less than a green build with it.
 */
@EnabledIfEnvironmentVariable(named = "MQ_USER", matches = ".+")
class PreflightAgainstRealMqTest {

    private static final String CONNECTION_ID = "primary";
    private static final String SOURCE_QUEUE = "MQ.HPS.MEMBERSHIP.IN";
    private static final String TRACKER_QUEUE = "MQ.HPS.MEMBERSHIP.TRACKER";
    private static final String BACKOUT_QUEUE = "MQ.HPS.MEMBERSHIP.BACKOUT";

    private MqConnectionManager connections;

    @BeforeEach
    void setUp() {
        String user = System.getenv("MQ_USER");
        String password = System.getenv("MQ_PASSWORD");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                password != null && !password.isBlank(), "MQ_PASSWORD not set");

        MqConnectionConfig config = new MqConnectionConfig();
        config.setId(CONNECTION_ID);
        config.setHost("localhost");
        config.setPort(1414);
        config.setQueueManager("QM1");
        config.setChannel("DEV.APP.SVRCONN");
        config.setTransportType("CLIENT");
        config.setReconnectAttempts(1);
        config.setReconnectDelayMs(100);

        CredentialProvider credentials = ref -> Optional.of(
                new CredentialProvider.Credentials(user, password));

        connections = new MqConnectionManager(Map.of(CONNECTION_ID, config), credentials);
    }

    @AfterEach
    void tearDown() {
        if (connections != null) {
            connections.close();
        }
    }

    @Test
    void everyMqCheckPassesAgainstTheProvisionedQueueManager() {
        PreflightReport report = runPreflight(binding(SOURCE_QUEUE, TRACKER_QUEUE, BACKOUT_QUEUE));

        // The report is the deliverable, so it goes into the failure message —
        // an operator reads this text, not an assertion.
        assertThat(report.hasFailures())
                .as("preflight against the drill queue manager:%n%s", report.render())
                .isFalse();
        assertThat(report.count(CheckOutcome.Status.PASS)).isPositive();
    }

    @Test
    void connectivityIsProvenSeparatelyFromQueueAccess() {
        // Two distinct findings, because "cannot reach the queue manager" and
        // "reached it but cannot open that queue" have different remedies and
        // different owners.
        PreflightReport report = runPreflight(binding(SOURCE_QUEUE, TRACKER_QUEUE, BACKOUT_QUEUE));

        assertThat(names(report)).contains(
                "rms.connection",
                "rms.source-queue.input",
                "rms.tracker-queue.output",
                "rms.backout-queue.output");
    }

    @Test
    void anUnknownQueueFailsWithTheNameThatWasNotFound() {
        // MQRC 2085. The remedy text distinguishes "the queue exists on a
        // different queue manager" from "the name is wrong", which is the
        // mistake this catches on a first deployment.
        PreflightReport report = runPreflight(
                binding("MQ.DOES.NOT.EXIST", TRACKER_QUEUE, BACKOUT_QUEUE));

        assertThat(report.hasFailures()).isTrue();
        PreflightReport.Entry entry = entry(report, "rms.source-queue.input");
        assertThat(entry.getOutcome().getStatus()).isEqualTo(CheckOutcome.Status.FAIL);
        assertThat(entry.getOutcome().getDetail() + entry.getOutcome().getRemedy())
                .contains("MQ.DOES.NOT.EXIST");
    }

    // Deliberately NOT tested here: that a wrong password fails the connection
    // check. The drill queue manager runs CHCKCLNT(REQDADM), so it checks
    // passwords only for ADMIN users -- 'app' connects with any password at
    // all. An assertion here would be measuring the container's CONNAUTH
    // policy rather than this codebase, and would break against a queue
    // manager configured more strictly, which is the wrong way round.
    //
    // Worth knowing operationally: a rehearsal against this container cannot
    // prove the application credential is correct. Credential fail-closed
    // behaviour -- refusing to connect when a configured credential-ref
    // cannot be resolved -- is covered by MqCredentialFailClosedTest in core,
    // which does not need a queue manager.

    @Test
    void preflightConsumesNothingFromTheSourceQueue() throws Exception {
        // The property the deployment guide promises: safe to run against an
        // environment carrying live data. A consumer opened for INPUT must not
        // take a message off the queue.
        int before = depth();
        runPreflight(binding(SOURCE_QUEUE, TRACKER_QUEUE, BACKOUT_QUEUE));
        assertThat(depth()).isEqualTo(before);
    }

    // --- helpers ---

    private PreflightReport runPreflight(BindingConfig binding) {
        List<PreflightCheck> checks = MqChecks.forAllBindings(props(binding), connections);
        return new PreflightRunner(checks).run(Set.of("mq"));
    }

    private IntakeProperties props(BindingConfig binding) {
        IntakeProperties properties = new IntakeProperties();
        properties.setBindings(List.of(binding));
        properties.setMqConnections(Map.of(CONNECTION_ID, new MqConnectionConfig()));
        return properties;
    }

    private BindingConfig binding(String source, String tracker, String backout) {
        BindingConfig config = new BindingConfig();
        config.setId("rms");
        config.setMqConnection(CONNECTION_ID);
        config.setMode(BindingMode.TRACKED);
        config.setSourceQueue(source);
        config.getTracker().setQueue(tracker);
        config.getBackout().setQueue(backout);
        config.getBackout().setDepthPollIntervalMs(0);   // no browse check here
        config.getHdfs().setBasePath("/data/raw/rms");
        return config;
    }

    private List<String> names(PreflightReport report) {
        List<String> names = new ArrayList<>();
        for (PreflightReport.Entry entry : report.getEntries()) {
            names.add(entry.getCheck().name());
        }
        return names;
    }

    private PreflightReport.Entry entry(PreflightReport report, String name) {
        return report.getEntries().stream()
                .filter(e -> e.getCheck().name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no check named " + name + " in:\n" + report.render()));
    }

    private int depth() throws Exception {
        javax.jms.Connection connection = connections.getConnection(CONNECTION_ID);
        try (javax.jms.Session session =
                     connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.QueueBrowser browser =
                    session.createBrowser(session.createQueue(SOURCE_QUEUE));
            int count = 0;
            java.util.Enumeration<?> messages = browser.getEnumeration();
            while (messages.hasMoreElements() && count < 1000) {
                messages.nextElement();
                count++;
            }
            browser.close();
            return count;
        }
    }

}
