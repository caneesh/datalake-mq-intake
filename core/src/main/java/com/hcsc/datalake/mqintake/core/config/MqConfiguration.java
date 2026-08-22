package com.hcsc.datalake.mqintake.core.config;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/**
 * Configures IBM MQ JMS ConnectionFactory.
 *
 * <p>The connection is shared across all bindings (thread-safe).
 * Each listener thread creates its own transacted Session from this connection.
 */
@Configuration
@ConditionalOnProperty(prefix = "intake.mq", name = "host")
public class MqConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MqConfiguration.class);

    @Bean
    public ConnectionFactory mqConnectionFactory(IntakeProperties properties) throws JMSException {
        IntakeProperties.MqProperties mq = properties.getMq();

        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setHostName(mq.getHost());
        factory.setPort(mq.getPort());
        factory.setQueueManager(mq.getQueueManager());
        factory.setChannel(mq.getChannel());
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);

        log.info("Configured MQ ConnectionFactory: host={}, port={}, queueManager={}, channel={}",
                mq.getHost(), mq.getPort(), mq.getQueueManager(), mq.getChannel());

        return factory;
    }

    @Bean(destroyMethod = "close")
    public Connection jmsConnection(ConnectionFactory connectionFactory, IntakeProperties properties)
            throws JMSException {

        IntakeProperties.MqProperties mq = properties.getMq();
        Connection connection;

        if (mq.getUser() != null && !mq.getUser().isBlank()) {
            connection = connectionFactory.createConnection(mq.getUser(), mq.getPassword());
            log.info("Created authenticated JMS connection for user: {}", mq.getUser());
        } else {
            connection = connectionFactory.createConnection();
            log.info("Created JMS connection (no authentication)");
        }

        connection.start();
        return connection;
    }
}
