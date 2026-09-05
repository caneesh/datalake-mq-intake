package com.hcsc.datalake.mqintake.core.mq;

import com.hcsc.datalake.mqintake.core.config.MqConnectionConfig;
import com.ibm.mq.jms.MQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.Optional;

/**
 * Opens a real IBM MQ connection: the production {@link ConnectionOpener}.
 *
 * <p>Everything here needs a queue manager to prove, which is exactly why it
 * is on the far side of the seam — {@link ManagedConnection}'s retry budget and
 * fault classification do not, and could not be tested while they sat behind
 * this.
 *
 * <p>{@link #buildConnectionFactory} is static and package-private rather than
 * injected. Configuring an {@code MQConnectionFactory} needs no broker to
 * check: build one and read its getters back. Testability here wanted
 * reachability, not another interface.
 *
 * <p>The factory is built once and reused across retry attempts, as it was
 * before: rebuilding it per attempt would be wasted work, and the object is
 * not what fails when a connect fails.
 */
class IbmMqConnectionOpener implements ConnectionOpener {

    private static final Logger log = LoggerFactory.getLogger(IbmMqConnectionOpener.class);

    private final MqConnectionConfig config;
    private final CredentialProvider credentialProvider;

    /** Built on first use, then reused; see the class javadoc. */
    private volatile MQConnectionFactory factory;

    IbmMqConnectionOpener(MqConnectionConfig config, CredentialProvider credentialProvider) {
        this.config = config;
        this.credentialProvider = credentialProvider;
    }

    static MQConnectionFactory buildConnectionFactory(MqConnectionConfig config)
            throws JMSException {
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
    @Override
    public Connection open() throws JMSException {
        if (factory == null) {
            factory = buildConnectionFactory(config);
        }
        Optional<CredentialProvider.Credentials> creds =
                MqConnectionManager.resolveCredentials(
                        config.getCredentialRef(), config.getId(), credentialProvider);

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
}
