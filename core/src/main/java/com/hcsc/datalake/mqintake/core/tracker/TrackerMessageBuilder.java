package com.hcsc.datalake.mqintake.core.tracker;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.Optional;

/**
 * Builds tracker messages from source messages.
 * Implementations live in binding modules (rms/, claims/), not in core.
 *
 * The tracker message carries specific content that is a downstream contract.
 * Core handles the transactional put; this interface handles message content.
 */
public interface TrackerMessageBuilder {

    /**
     * Builds a tracker message for the given source message.
     *
     * @param session       the JMS session to create the message on (same session as consumer)
     * @param sourceMessage the source message being acknowledged
     * @return the tracker message to send, or empty to suppress the send for this message
     * @throws JMSException if message creation fails
     */
    Optional<Message> build(Session session, Message sourceMessage) throws JMSException;
}
