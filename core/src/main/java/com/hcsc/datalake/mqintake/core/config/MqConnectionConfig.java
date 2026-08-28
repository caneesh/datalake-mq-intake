package com.hcsc.datalake.mqintake.core.config;

import java.util.Objects;

/**
 * Configuration for a single IBM MQ connection.
 *
 * <p>Credentials are separated via credential-ref to avoid storing
 * plaintext passwords in configuration files.
 */
public class MqConnectionConfig {

    private String id;
    private String host;
    private int port = 1414;
    private String queueManager;
    private String channel;
    private String transportType = "CLIENT";
    private String credentialRef;
    private long receiveTimeoutMs = 1000;

    /**
     * Retry budget for the INITIAL connection attempt at startup
     * ({@code MqConnectionManager.ManagedConnection.connect()}) — these two
     * fields do NOT govern session recovery. Once a binding is running, a
     * broken session is rebuilt by {@code TransactedReceiveLoop}'s own
     * recovery, which is fixed at 10 attempts with exponential backoff
     * (1s→60s, jittered) and is not operator-tunable today. Making session
     * recovery configurable is a deliberate future change, not an oversight
     * of these fields.
     */
    private int reconnectAttempts = 3;
    private long reconnectDelayMs = 5000;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getQueueManager() {
        return queueManager;
    }

    public void setQueueManager(String queueManager) {
        this.queueManager = queueManager;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getTransportType() {
        return transportType;
    }

    public void setTransportType(String transportType) {
        this.transportType = transportType;
    }

    public String getCredentialRef() {
        return credentialRef;
    }

    public void setCredentialRef(String credentialRef) {
        this.credentialRef = credentialRef;
    }

    public long getReceiveTimeoutMs() {
        return receiveTimeoutMs;
    }

    public void setReceiveTimeoutMs(long receiveTimeoutMs) {
        this.receiveTimeoutMs = receiveTimeoutMs;
    }

    public int getReconnectAttempts() {
        return reconnectAttempts;
    }

    public void setReconnectAttempts(int reconnectAttempts) {
        this.reconnectAttempts = reconnectAttempts;
    }

    public long getReconnectDelayMs() {
        return reconnectDelayMs;
    }

    public void setReconnectDelayMs(long reconnectDelayMs) {
        this.reconnectDelayMs = reconnectDelayMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqConnectionConfig that = (MqConnectionConfig) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "MqConnectionConfig{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", queueManager='" + queueManager + '\'' +
                ", channel='" + channel + '\'' +
                '}';
    }
}
