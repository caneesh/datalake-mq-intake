package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;

import javax.jms.Connection;
import java.util.Optional;

/**
 * Supplies JMS connections by configured name.
 *
 * <p>What callers actually need from connection management: give me the
 * connection called "primary", tell me whether it exists, and tell me how it
 * is configured. Creation, caching, retry and credential resolution are the
 * implementation's business, not the caller's.
 *
 * <p>Connections handed out here are shared across listener threads — a JMS
 * {@link Connection} is thread-safe, sessions are not. Each thread must create
 * its own session from the connection it is given.
 */
public interface MqConnectionProvider {

    /**
     * @param connectionId the configured name, e.g. "primary"
     * @return a started connection, creating it if necessary
     * @throws MqConnectionManager.MqConnectionException if it cannot be established
     */
    Connection getConnection(String connectionId);

    boolean hasConnection(String connectionId);

    Optional<MqConnectionConfig> getConfig(String connectionId);
}
