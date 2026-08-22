package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
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
public class MqConnectionManager {

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
    public Optional<MqConnectionConfig> getConfig(String connectionId) {
        return Optional.ofNullable(connectionConfigs.get(connectionId));
    }

    /**
     * Checks if a connection configuration exists.
     */
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

            int transportType = parseTransportType(config.getTransportType());
            mqFactory.setTransportType(transportType);

            log.debug("Built MQConnectionFactory for {}: host={}, port={}, queueManager={}, channel={}",
                    config.getId(), config.getHost(), config.getPort(),
                    config.getQueueManager(), config.getChannel());

            return mqFactory;
        }

        private Connection createConnection() throws JMSException {
            Optional<CredentialProvider.Credentials> creds = Optional.empty();

            if (config.getCredentialRef() != null && !config.getCredentialRef().isBlank()) {
                creds = credentialProvider.getCredentials(config.getCredentialRef());
                if (creds.isEmpty()) {
                    log.warn("Credential ref '{}' did not resolve to credentials for {}",
                            config.getCredentialRef(), config.getId());
                }
            }

            if (creds.isPresent()) {
                CredentialProvider.Credentials c = creds.get();
                log.debug("Creating authenticated connection for {} as user {}",
                        config.getId(), c.getUsername());
                return factory.createConnection(c.getUsername(), c.getPassword());
            } else {
                log.debug("Creating unauthenticated connection for {}", config.getId());
                return factory.createConnection();
            }
        }

        private int parseTransportType(String transportType) {
            if (transportType == null || transportType.isBlank() ||
                    transportType.equalsIgnoreCase("CLIENT")) {
                return WMQConstants.WMQ_CM_CLIENT;
            } else if (transportType.equalsIgnoreCase("BINDINGS")) {
                return WMQConstants.WMQ_CM_BINDINGS;
            } else {
                log.warn("Unknown transport type '{}', defaulting to CLIENT", transportType);
                return WMQConstants.WMQ_CM_CLIENT;
            }
        }

        private boolean isConfigurationError(JMSException e) {
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
    public static class MqConnectionException extends RuntimeException {
        public MqConnectionException(String message) {
            super(message);
        }

        public MqConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
