package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.ibm.mq.jms.MQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages IBM MQ connections with lifecycle and reconnection support.
 *
 * <p>Key responsibilities:
 * <ul>
 *   <li>Build MQConnectionFactory from configuration</li>
 *   <li>Create and start JMS connections</li>
 *   <li>Handle reconnection with bounded retries</li>
 *   <li>Clean shutdown</li>
 * </ul>
 *
 * <p>Connection is shared across threads (JMS Connection is thread-safe).
 * Sessions must be created per-thread.
 */
public class MqConnectionManager implements MqConnectionProvider {

    private static final Logger log = LoggerFactory.getLogger(MqConnectionManager.class);

    private final Map<String, MqConnectionConfig> connectionConfigs;
    private final CredentialProvider credentialProvider;
    private final Map<String, ManagedConnection> connections = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MqConnectionManager(Map<String, MqConnectionConfig> connectionConfigs,
                                CredentialProvider credentialProvider) {
        this.connectionConfigs = new ConcurrentHashMap<>(connectionConfigs);
        this.credentialProvider = Objects.requireNonNull(credentialProvider, "credentialProvider required");
    }

    /**
     * Gets or creates a connection for the given connection ID.
     *
     * @param connectionId the connection identifier
     * @return the JMS connection
     * @throws MqConnectionException if connection cannot be established
     */
    @Override
    public Connection getConnection(String connectionId) throws MqConnectionException {
        if (closed.get()) {
            throw new MqConnectionException("Connection manager is closed");
        }

        ManagedConnection managed = connections.computeIfAbsent(connectionId, this::createManagedConnection);
        return managed.getConnection();
    }

    /**
     * Gets the configuration for a connection.
     *
     * @param connectionId the connection identifier
     * @return configuration if found
     */
    @Override
    public Optional<MqConnectionConfig> getConfig(String connectionId) {
        return Optional.ofNullable(connectionConfigs.get(connectionId));
    }

    /**
     * Checks if a connection configuration exists.
     */
    @Override
    public boolean hasConnection(String connectionId) {
        return connectionConfigs.containsKey(connectionId);
    }

    /**
     * Closes all managed connections.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing MqConnectionManager with {} connections", connections.size());
            connections.values().forEach(ManagedConnection::close);
            connections.clear();
        }
    }

    private ManagedConnection createManagedConnection(String connectionId) {
        MqConnectionConfig config = connectionConfigs.get(connectionId);
        if (config == null) {
            throw new MqConnectionException("No configuration found for connection: " + connectionId);
        }
        return new ManagedConnection(config, credentialProvider);
    }

    /**
     * Wrapper around a JMS Connection with reconnection support.
     */
    private static class ManagedConnection {
        private final MqConnectionConfig config;
        private final CredentialProvider credentialProvider;
        private volatile Connection connection;
        private volatile MQConnectionFactory factory;

        ManagedConnection(MqConnectionConfig config, CredentialProvider credentialProvider) {
            this.config = config;
            this.credentialProvider = credentialProvider;
        }

        synchronized Connection getConnection() throws MqConnectionException {
            if (connection != null) {
                return connection;
            }

            return connect();
        }

        private Connection connect() throws MqConnectionException {
            int attempts = 0;
            int maxAttempts = config.getReconnectAttempts();
            JMSException lastException = null;

            while (attempts < maxAttempts) {
                attempts++;
                try {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new MqConnectionException("Connection attempt interrupted");
                    }

                    if (factory == null) {
                        factory = buildConnectionFactory();
                    }

                    connection = createConnection();
                    connection.start();

                    log.info("Connected to MQ: id={}, host={}, queueManager={}",
                            config.getId(), config.getHost(), config.getQueueManager());

                    return connection;

                } catch (JMSException e) {
                    lastException = e;
                    log.warn("Connection attempt {} of {} failed for {}: {}",
                            attempts, maxAttempts, config.getId(), e.getMessage());

                    if (isConfigurationError(e)) {
                        throw new MqConnectionException(
                                "Configuration error connecting to " + config.getId() + ": " + e.getMessage(), e);
                    }

                    if (attempts < maxAttempts) {
                        try {
                            Thread.sleep(config.getReconnectDelayMs());
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new MqConnectionException("Connection attempt interrupted", ie);
                        }
                    }
                }
            }

            throw new MqConnectionException(
                    "Failed to connect to " + config.getId() + " after " + maxAttempts + " attempts",
                    lastException);
        }

        private MQConnectionFactory buildConnectionFactory() throws JMSException {
            MQConnectionFactory mqFactory = new MQConnectionFactory();
            mqFactory.setHostName(config.getHost());
            mqFactory.setPort(config.getPort());
            mqFactory.setQueueManager(config.getQueueManager());
            mqFactory.setChannel(config.getChannel());

            mqFactory.setTransportType(
                    MqTransportType.fromConfig(config.getTransportType()).wmqConstant());

            log.debug("Built MQConnectionFactory for {}: host={}, port={}, queueManager={}, channel={}",
                    config.getId(), config.getHost(), config.getPort(),
                    config.getQueueManager(), config.getChannel());

            return mqFactory;
        }

        /**
         * Opens the JMS connection, failing closed on credentials.
         *
         * <p>A configured {@code credential-ref} is a statement that this
         * queue manager must be reached as a specific identity. If the lookup
         * then fails, the only safe outcome is no connection. Falling back to
         * an unauthenticated connect — the previous behaviour, behind a
         * warning — silently downgrades the security posture at the worst
         * possible moment: a credential store outage or a rotation that
         * removed the entry. Where the queue manager permits anonymous binds
         * it would connect with different authority than intended, and where
         * it does not, the real cause would be buried under an MQ auth error.
         */
        private Connection createConnection() throws JMSException {
            Optional<CredentialProvider.Credentials> creds =
                    resolveCredentials(config.getCredentialRef(), config.getId(), credentialProvider);

            if (creds.isEmpty()) {
                log.debug("No credential-ref configured for {} — connecting without credentials",
                        config.getId());
                return factory.createConnection();
            }

            CredentialProvider.Credentials c = creds.get();
            log.debug("Creating authenticated connection for {} as user {}",
                    config.getId(), c.getUsername());
            return factory.createConnection(c.getUsername(), c.getPassword());
        }

        private boolean isConfigurationError(JMSException e) {
            // A credential that will not resolve is a configuration problem,
            // not a transient one. Retrying cannot fix it and would only delay
            // the real error reaching the operator.
            if (e instanceof MqCredentialException) {
                return true;
            }

            String message = e.getMessage();
            if (message == null) return false;

            return message.contains("MQRC_UNKNOWN_OBJECT_NAME") ||
                    message.contains("MQRC_NOT_AUTHORIZED") ||
                    message.contains("MQRC_SECURITY_ERROR") ||
                    message.contains("MQRC_Q_MGR_NAME_ERROR") ||
                    message.contains("MQRC_CHANNEL_NOT_FOUND");
        }

        synchronized void close() {
            if (connection != null) {
                try {
                    connection.close();
                    log.info("Closed MQ connection: {}", config.getId());
                } catch (JMSException e) {
                    log.warn("Error closing MQ connection {}: {}", config.getId(), e.getMessage());
                }
                connection = null;
            }
        }
    }

    /**
     * Exception for MQ connection failures.
     */
    /**
     * Resolves the credentials a connection must use, failing closed.
     *
     * <p>A configured {@code credential-ref} is a statement that this queue
     * manager must be reached as a specific identity. If the lookup then
     * fails, the only safe outcome is no connection. Falling back to an
     * unauthenticated connect — the previous behaviour, behind a warning —
     * silently downgrades the security posture at the worst possible moment:
     * a credential-store outage, or a rotation that removed the entry. Where
     * the queue manager permits anonymous binds it would connect with
     * different authority than intended; where it does not, the real cause
     * would be buried under a generic MQ authorisation error.
     *
     * <p>Package-private so the decision can be tested without a live queue
     * manager — the alternative is that the one branch which must never
     * regress is the one branch no test can reach.
     *
     * @return the credentials to authenticate with, or empty when no
     *         {@code credential-ref} is configured and an unauthenticated
     *         connection is the intended behaviour
     * @throws MqCredentialException if a credential-ref is configured but does
     *         not resolve to a complete credential
     */
    static Optional<CredentialProvider.Credentials> resolveCredentials(
            String credentialRef, String connectionId, CredentialProvider provider)
            throws MqCredentialException {

        if (credentialRef == null || credentialRef.isBlank()) {
            // Deliberately absent: preserved behaviour for queue managers
            // configured to accept the process identity.
            return Optional.empty();
        }

        Optional<CredentialProvider.Credentials> creds;
        try {
            creds = provider.getCredentials(credentialRef);
        } catch (RuntimeException e) {
            // The message is the provider's, which may name the reference but
            // never the secret itself.
            throw new MqCredentialException(String.format(
                    "Credential lookup failed for connection '%s' (credential-ref '%s'): %s. "
                            + "Refusing to connect without credentials.",
                    connectionId, credentialRef, e.getMessage()), e);
        }

        if (creds == null || creds.isEmpty()) {
            throw new MqCredentialException(String.format(
                    "credential-ref '%s' is configured for connection '%s' but did not resolve to "
                            + "any credentials. Refusing to fall back to an unauthenticated "
                            + "connection.",
                    credentialRef, connectionId));
        }

        CredentialProvider.Credentials c = creds.get();
        boolean userMissing = c.getUsername() == null || c.getUsername().isEmpty();
        boolean passMissing = c.getPassword() == null || c.getPassword().isEmpty();
        if (userMissing || passMissing) {
            // A half-populated credential would be sent to the queue manager
            // as-is and come back as a confusing authorisation failure.
            throw new MqCredentialException(String.format(
                    "credential-ref '%s' for connection '%s' resolved to an incomplete credential "
                            + "(username %s, password %s). Refusing to connect.",
                    credentialRef, connectionId,
                    userMissing ? "missing" : "present",
                    passMissing ? "missing" : "present"));
        }

        return creds;
    }

    public static class MqConnectionException extends RuntimeException {
        public MqConnectionException(String message) {
            super(message);
        }

        public MqConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * A configured credential could not be resolved, so the connection was
     * refused rather than downgraded to an unauthenticated one.
     *
     * <p>Extends {@link JMSException} so it travels the same path as any other
     * connect failure, and is classified as a configuration error so it fails
     * fast instead of burning the reconnect budget: a missing or malformed
     * credential does not become present by trying again a moment later, and
     * the operator needs the real reason surfaced immediately.
     *
     * <p>Messages carry the credential <em>reference</em> and never the secret.
     */
    public static class MqCredentialException extends JMSException {
        public MqCredentialException(String message) {
            super(message);
        }

        public MqCredentialException(String message, Throwable cause) {
            super(message);
            initCause(cause);
        }
    }
}
