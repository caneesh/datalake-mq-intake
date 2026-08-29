package com.hcsc.datalake.mqintake.core.preflight;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;
import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionManager;
import com.hcsc.datalake.mqintake.core.mq.MqConnectionProvider;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The MQ probes, against a real broker.
 *
 * <p>The behaviour that matters most is what these checks do NOT do: opening
 * the source queue must not consume a message, and opening the tracker or
 * backout queue must not put one. Preflight runs against environments with
 * live data, so a probe with side effects would be worse than no probe.
 */
class MqChecksTest {

    private static final String SOURCE = "PREFLIGHT.SOURCE";
    private static final String BACKOUT = "PREFLIGHT.BOQ";
    private static final String TRACKER = "PREFLIGHT.TRACKER";

    private Connection connection;
    private Session session;
    private IntakeProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        BindingConfig binding = new BindingConfig();
        binding.setId("rms");
        binding.setMqConnection("primary");
        binding.setMode(BindingMode.LAND_ONLY);
        binding.setSourceQueue(SOURCE);
        binding.getBackout().setQueue(BACKOUT);

        MqConnectionConfig connectionConfig = new MqConnectionConfig();
        connectionConfig.setId("primary");
        properties = new IntakeProperties();
        properties.setBindings(List.of(binding));
        properties.setMqConnections(java.util.Map.of("primary", connectionConfig));
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            session.close();
            connection.close();
        } catch (Exception ignored) {
        }
    }

    private MqConnectionProvider provider(Connection connection) {
        return new MqConnectionProvider() {
            @Override
            public Connection getConnection(String connectionId) {
                return connection;
            }

            @Override
            public boolean hasConnection(String connectionId) {
                return true;
            }

            @Override
            public Optional<MqConnectionConfig> getConfig(String connectionId) {
                return Optional.of(properties.getMqConnections().get("primary"));
            }
        };
    }

    private PreflightReport run(MqConnectionProvider provider) {
        return new PreflightRunner(MqChecks.forAllBindings(properties, provider)).run(Set.of());
    }

    private CheckOutcome outcomeOf(PreflightReport report, String nameSuffix) {
        return report.getEntries().stream()
                .filter(e -> e.getCheck().name().endsWith(nameSuffix))
                .findFirst().orElseThrow()
                .getOutcome();
    }

    private int depthOf(String queue) throws Exception {
        try (QueueBrowser browser = session.createBrowser(session.createQueue(queue))) {
            int count = 0;
            for (Enumeration<?> e = browser.getEnumeration(); e.hasMoreElements(); e.nextElement()) {
                count++;
            }
            return count;
        }
    }

    private void send(String queue, int count) throws Exception {
        try (MessageProducer producer = session.createProducer(session.createQueue(queue))) {
            for (int i = 0; i < count; i++) {
                producer.send(session.createTextMessage("preflight-fixture-" + i));
            }
        }
    }

    @Test
    void aReachableBrokerPassesEveryApplicableCheck() {
        PreflightReport report = run(provider(connection));

        assertThat(report.hasFailures()).as(report.render()).isFalse();
        assertThat(outcomeOf(report, ".connection").getDetail()).contains("connected via 'primary'");
        assertThat(outcomeOf(report, "source-queue.input").getDetail())
                .contains("opened for input");
    }

    @Test
    void probingTheSourceQueueConsumesNothing() throws Exception {
        send(SOURCE, 3);

        run(provider(connection));

        assertThat(depthOf(SOURCE))
                .as("preflight runs against live queues; opening for input must not consume")
                .isEqualTo(3);
    }

    @Test
    void probingTheBackoutQueueSendsNothing() throws Exception {
        run(provider(connection));

        assertThat(depthOf(BACKOUT))
                .as("opening for output must not put a message a real consumer would see")
                .isZero();
    }

    @Test
    void aTrackerQueueIsSkippedForALandOnlyBindingAndCheckedForATrackedOne() throws Exception {
        assertThat(outcomeOf(run(provider(connection)), "tracker-queue.output").getStatus())
                .isEqualTo(CheckOutcome.Status.SKIP);

        BindingConfig tracked = properties.getBindings().get(0);
        tracked.setMode(BindingMode.TRACKED);
        tracked.getTracker().setQueue(TRACKER);

        assertThat(outcomeOf(run(provider(connection)), "tracker-queue.output").getStatus())
                .isEqualTo(CheckOutcome.Status.PASS);
        assertThat(depthOf(TRACKER)).isZero();
    }

    @Test
    void anUnreachableQueueFailsAndNamesTheSiblingQueueManagerTrap() throws Exception {
        // A closed connection stands in for a queue that cannot be resolved.
        // The remedy text is the point: the commonest real cause of a
        // resolution failure is a queue defined on a DIFFERENT queue manager
        // than the one this connection reached, which no amount of console
        // inspection reveals.
        connection.close();

        CheckOutcome outcome = outcomeOf(run(provider(connection)), "backout-queue.output");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getRemedy())
                .contains("DIFFERENT queue manager")
                .contains("must be defined on the connected queue manager");
    }

    @Test
    void aBrokenConnectionFailsTheConnectivityCheckWithItsCause() {
        MqConnectionProvider refuses = new MqConnectionProvider() {
            @Override
            public Connection getConnection(String connectionId) {
                throw new MqConnectionManager.MqConnectionException(
                        "MQRC 2035 MQRC_NOT_AUTHORIZED", new IllegalStateException("channel auth"));
            }

            @Override
            public boolean hasConnection(String connectionId) {
                return true;
            }

            @Override
            public Optional<MqConnectionConfig> getConfig(String connectionId) {
                return Optional.empty();
            }
        };

        CheckOutcome outcome = outcomeOf(run(refuses), ".connection");

        assertThat(outcome.isFailure()).isTrue();
        assertThat(outcome.getDetail()).contains("MQRC 2035").contains("channel auth");
        assertThat(outcome.getRemedy()).contains("MQ_CREDENTIAL_REF");
    }

    @Test
    void depthBrowseReportsANonEmptyBackoutQueueBecauseThatIsPageWorthy() throws Exception {
        send(BACKOUT, 2);
        properties.getBindings().get(0).getBackout().setDepthPollIntervalMs(30_000);

        CheckOutcome outcome = outcomeOf(run(provider(connection)), "backout-queue.browse");

        assertThat(outcome.getStatus()).isEqualTo(CheckOutcome.Status.PASS);
        assertThat(outcome.getDetail())
                .contains("depth 2")
                .contains("NOTE: non-empty backout queue");
    }
}
