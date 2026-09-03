package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.jms.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers {@code MqConnectionManager.ManagedConnection} against a real queue
 * manager, through the public API only.
 *
 * <p><strong>No production code was changed to make this testable.</strong>
 * The obvious route — extracting {@code buildConnectionFactory()} behind an
 * interface — would put a seam in the path that establishes every MQ
 * connection for both applications, and this class is reachable without one.
 * Everything below drives {@link MqConnectionManager#getConnection(String)}
 * and varies only configuration.
 *
 * <p>The class read 0% before this, which was a measurement artifact rather
 * than the truth: it is executed by {@code PreflightAgainstRealMqTest}, but
 * that lives in the {@code rms} module and JaCoCo reports per module, so
 * core's report never saw the run and rms's report does not contain core's
 * classes. It was exercised and counted nowhere.
 *
 * <p>Requires the queue manager from {@code docker-compose.yml}:
 *
 * <pre>
 *   docker-compose up -d ibm-mq
 *   export MQ_USER=app MQ_PASSWORD=passw0rd
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "MQ_USER", matches = ".+")
class ManagedConnectionAgainstRealMqTest {

    private static final String ID = "primary";
    private static final int LIVE_PORT = 1414;
    /** Nothing listens here; connection refused is immediate on loopback. */
    private static final int DEAD_PORT = 14149;

    private final List<MqConnectionManager> managers = new ArrayList<>();
    private String user;
    private String password;

    @BeforeEach
    void setUp() {
        user = System.getenv("MQ_USER");
        password = System.getenv("MQ_PASSWORD");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                password != null && !password.isBlank(), "MQ_PASSWORD not set");
    }

    @AfterEach
    void tearDown() {
        // Every manager caches a live connection; leaking them across tests
        // would leave the queue manager holding handles for the whole run.
        managers.forEach(MqConnectionManager::close);
        managers.clear();
    }

    @Test
    void connectsToTheQueueManagerWithTheConfiguredCredential() throws Exception {
        MqConnectionManager manager = manager(config(LIVE_PORT, "QM1", 3), withCredentials());

        Connection connection = manager.getConnection(ID);

        assertThat(connection).isNotNull();
        // start() is called inside connect(); a started connection accepts a
        // session, which is the only externally visible proof of it.
        connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE).close();
    }

    @Test
    void theConnectionIsBuiltOnceAndReusedAfterwards() throws Exception {
        // The caching branch: one Connection per configured id, shared across
        // every listener thread. Rebuilding per caller would multiply channel
        // instances against MAXINST without anything saying so.
        MqConnectionManager manager = manager(config(LIVE_PORT, "QM1", 3), withCredentials());

        Connection first = manager.getConnection(ID);
        Connection second = manager.getConnection(ID);

        assertThat(second).isSameAs(first);
    }

    @Test
    void connectsWithoutCredentialsWhenNoCredentialRefIsConfigured() throws Exception {
        // The unauthenticated branch of createConnection(). Reachable here
        // only because the drill queue manager runs CHCKCLNT(REQDADM) and so
        // does not demand a password from a non-admin user.
        MqConnectionConfig config = config(LIVE_PORT, "QM1", 3);
        config.setCredentialRef("");

        MqConnectionManager manager = manager(config, ref -> Optional.empty());

        assertThat(manager.getConnection(ID)).isNotNull();
    }

    @Test
    void anUnreachableListenerIsRetriedAndThenReportedWithTheAttemptCount() {
        MqConnectionManager manager = manager(config(DEAD_PORT, "QM1", 3), withCredentials());

        assertThatThrownBy(() -> manager.getConnection(ID))
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageContaining("after 3 attempts");
    }

    @Test
    void anUnresolvableCredentialFailsImmediatelyWithoutRetrying() {
        // isConfigurationError()'s one reliable branch: a configured
        // credential-ref that will not resolve is a configuration problem, and
        // retrying cannot fix it. Proven by the attempt count — a retried
        // failure would report "after 3 attempts".
        MqConnectionConfig config = config(LIVE_PORT, "QM1", 3);
        config.setCredentialRef("env:ABSENT_USER,ABSENT_PASSWORD");

        MqConnectionManager manager = manager(config, ref -> Optional.empty());

        assertThatThrownBy(() -> manager.getConnection(ID))
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageNotContaining("after 3 attempts");
    }

    @Test
    void aWrongQueueManagerNameIsRetriedRatherThanFailingFast() {
        // Documents CURRENT behaviour, and it is not what the code intends.
        //
        // isConfigurationError() lists MQRC_Q_MGR_NAME_ERROR among the
        // conditions retrying cannot fix, but it searches only
        // e.getMessage() — and the IBM MQ client puts the MQRC constant in the
        // LINKED exception, leaving the top-level message as "Failed to connect
        // to queue manager 'X'...". So the constant is never seen and the
        // attempt is retried to exhaustion. Measured: ~1.3s against ~0.2s for a
        // refused connection.
        //
        // Left as it is, not fixed. The cost is a bounded delay on a
        // misconfiguration that fails either way, and this is the connection
        // path for both applications. The same class of defect is already
        // recorded for session faults in READINESS_REVIEW.md §D″ item 2;
        // SessionFaultPolicy searches the message, the linked exception AND the
        // error code, which is the shape this would need.
        MqConnectionManager manager = manager(config(LIVE_PORT, "NO.SUCH.QM", 3), withCredentials());

        assertThatThrownBy(() -> manager.getConnection(ID))
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageContaining("after 3 attempts");
    }

    @Test
    void closingTheManagerReleasesTheConnectionAndRefusesFurtherUse() throws Exception {
        MqConnectionManager manager = manager(config(LIVE_PORT, "QM1", 3), withCredentials());
        assertThat(manager.getConnection(ID)).isNotNull();

        manager.close();

        assertThatThrownBy(() -> manager.getConnection(ID))
                .isInstanceOf(MqConnectionManager.MqConnectionException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void closingTwiceIsHarmless() throws Exception {
        // Shutdown runs this path, sometimes after a failed start has already
        // torn things down.
        MqConnectionManager manager = manager(config(LIVE_PORT, "QM1", 3), withCredentials());
        manager.getConnection(ID);

        manager.close();
        manager.close();
    }

    @Test
    void anUnknownConnectionIdIsRejectedBeforeAnythingIsOpened() {
        MqConnectionManager manager = manager(config(LIVE_PORT, "QM1", 3), withCredentials());

        assertThat(manager.hasConnection("not-configured")).isFalse();
        assertThatThrownBy(() -> manager.getConnection("not-configured"))
                .isInstanceOf(MqConnectionManager.MqConnectionException.class);
    }

    // --- helpers ---

    private MqConnectionManager manager(MqConnectionConfig config, CredentialProvider credentials) {
        MqConnectionManager manager =
                new MqConnectionManager(Map.of(ID, config), credentials);
        managers.add(manager);
        return manager;
    }

    private CredentialProvider withCredentials() {
        return ref -> Optional.of(new CredentialProvider.Credentials(user, password));
    }

    private MqConnectionConfig config(int port, String queueManager, int attempts) {
        MqConnectionConfig config = new MqConnectionConfig();
        config.setId(ID);
        config.setHost("localhost");
        config.setPort(port);
        config.setQueueManager(queueManager);
        config.setChannel("DEV.APP.SVRCONN");
        config.setTransportType("CLIENT");
        config.setCredentialRef("env:MQ_USER,MQ_PASSWORD");
        config.setReconnectAttempts(attempts);
        config.setReconnectDelayMs(50);
        return config;
    }
}
