package com.hcsc.datalake.mqintake.core.poison;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * Handles poison message detection and routing to backout queue.
 *
 * <p>From DESIGN.md §4.2 and §6.1: The application owns poison handling.
 * Read the backout count, compare against BOTHRESH, and on breach send
 * the message to the backout queue ON THE SAME TRANSACTED SESSION.
 *
 * <p>Do not rely on provider or queue-manager behavior — this loop
 * is not container-managed.
 */
public class PoisonMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(PoisonMessageHandler.class);

    /**
     * JMS property for delivery count (standard).
     */
    public static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    /**
     * IBM MQ-specific property for backout count.
     */
    public static final String JMS_IBM_MQMD_BACKOUT_COUNT = "JMS_IBM_MQMD_BackoutCount";

    private final int backoutThreshold;
    private final String backoutQueueName;

    /**
     * Creates a poison message handler.
     *
     * @param backoutThreshold  BOTHRESH: max delivery attempts before routing to backout
     * @param backoutQueueName  the backout queue name (BOQNAME)
     */
    public PoisonMessageHandler(int backoutThreshold, String backoutQueueName) {
        if (backoutThreshold <= 0) {
            throw new IllegalArgumentException("backoutThreshold must be positive");
        }
        if (backoutQueueName == null || backoutQueueName.isBlank()) {
            throw new IllegalArgumentException("backoutQueueName must not be blank");
        }
        this.backoutThreshold = backoutThreshold;
        this.backoutQueueName = backoutQueueName;
    }

    /**
     * Checks if a message has exceeded the backout threshold.
     *
     * @param message the message to check
     * @return true if the message should be routed to the backout queue
     */
    public boolean isPoisonMessage(Message message) {
        int deliveryCount = getDeliveryCount(message);
        return deliveryCount > backoutThreshold;
    }

    /**
     * Gets the delivery/backout count from a message.
     * Tries JMSXDeliveryCount first, then IBM MQ-specific property.
     *
     * @param message the message
     * @return the delivery count, or 1 if not available
     */
    public int getDeliveryCount(Message message) {
        try {
            // Try standard JMS property first
            if (message.propertyExists(JMSX_DELIVERY_COUNT)) {
                return message.getIntProperty(JMSX_DELIVERY_COUNT);
            }

            // Try IBM MQ-specific property
            if (message.propertyExists(JMS_IBM_MQMD_BACKOUT_COUNT)) {
                // BackoutCount is 0 on first delivery, so add 1
                return message.getIntProperty(JMS_IBM_MQMD_BACKOUT_COUNT) + 1;
            }

        } catch (JMSException e) {
            log.warn("Failed to read delivery count: {}", e.getMessage());
        }

        // Default: assume first delivery
        return 1;
    }

    /**
     * Routes a poison message to the backout queue.
     *
     * <p>CRITICAL: This must use the SAME transacted session as the consumer,
     * so the backout send is part of the same unit of work.
     *
     * @param session the transacted session (same as consumer)
     * @param message the poison message to route
     * @return result indicating success or failure
     */
    public BackoutResult routeToBackout(Session session, Message message) {
        String messageId = "unknown";
        try {
            messageId = message.getJMSMessageID();
            // Read delivery count BEFORE sending (sending may modify the message)
            int deliveryCount = getDeliveryCount(message);

            // Create producer for backout queue ON THE SAME SESSION
            Queue backoutQueue = session.createQueue(backoutQueueName);
            try (MessageProducer producer = session.createProducer(backoutQueue)) {
                producer.send(message);

                log.warn("Routed poison message to backout queue: id={}, queue={}, deliveryCount={}",
                        messageId, backoutQueueName, deliveryCount);

                return BackoutResult.success(messageId, backoutQueueName, deliveryCount);
            }

        } catch (JMSException e) {
            log.error("Failed to route poison message to backout queue: id={}, error={}",
                    messageId, e.getMessage(), e);
            return BackoutResult.failure(messageId, e);
        }
    }

    /**
     * Checks a batch of messages and routes any poison messages to backout.
     *
     * <p>CRITICAL: If routing to backout queue fails for ANY message, this method
     * throws {@link BackoutFailureException}. The caller MUST NOT commit when this
     * happens — the batch must be rolled back so no messages are lost.
     *
     * @param session  the transacted session
     * @param messages the messages to check
     * @return result containing any poison messages found
     * @throws BackoutFailureException if routing to backout queue fails
     */
    public BatchPoisonCheckResult checkAndRoutePoisonMessages(Session session, java.util.List<Message> messages)
            throws BackoutFailureException {
        java.util.List<BackoutResult> routed = new java.util.ArrayList<>();
        java.util.List<Message> clean = new java.util.ArrayList<>();

        for (Message message : messages) {
            if (isPoisonMessage(message)) {
                BackoutResult result = routeToBackout(session, message);
                if (!result.isSuccess()) {
                    // CRITICAL: BOQ failure must cause rollback - message cannot be lost
                    throw new BackoutFailureException(
                            "Failed to route poison message to backout queue: " + result.getMessageId(),
                            result.getError());
                }
                routed.add(result);
            } else {
                clean.add(message);
            }
        }

        return new BatchPoisonCheckResult(clean, routed);
    }

    /**
     * Thrown when routing a poison message to the backout queue fails.
     * The caller MUST roll back the transaction when this is thrown.
     */
    public static class BackoutFailureException extends Exception {
        public BackoutFailureException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public int getBackoutThreshold() {
        return backoutThreshold;
    }

    public String getBackoutQueueName() {
        return backoutQueueName;
    }

    /**
     * Result of routing a message to the backout queue.
     */
    public static class BackoutResult {
        private final boolean success;
        private final String messageId;
        private final String backoutQueue;
        private final int deliveryCount;
        private final Exception error;

        private BackoutResult(boolean success, String messageId, String backoutQueue,
                              int deliveryCount, Exception error) {
            this.success = success;
            this.messageId = messageId;
            this.backoutQueue = backoutQueue;
            this.deliveryCount = deliveryCount;
            this.error = error;
        }

        public static BackoutResult success(String messageId, String backoutQueue, int deliveryCount) {
            return new BackoutResult(true, messageId, backoutQueue, deliveryCount, null);
        }

        public static BackoutResult failure(String messageId, Exception error) {
            return new BackoutResult(false, messageId, null, 0, error);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessageId() {
            return messageId;
        }

        public String getBackoutQueue() {
            return backoutQueue;
        }

        public int getDeliveryCount() {
            return deliveryCount;
        }

        public Exception getError() {
            return error;
        }
    }

    /**
     * Result of checking a batch for poison messages.
     */
    public static class BatchPoisonCheckResult {
        private final java.util.List<Message> cleanMessages;
        private final java.util.List<BackoutResult> routedMessages;

        public BatchPoisonCheckResult(java.util.List<Message> cleanMessages,
                                       java.util.List<BackoutResult> routedMessages) {
            this.cleanMessages = cleanMessages;
            this.routedMessages = routedMessages;
        }

        public java.util.List<Message> getCleanMessages() {
            return cleanMessages;
        }

        public java.util.List<BackoutResult> getRoutedMessages() {
            return routedMessages;
        }

        public boolean hasPoisonMessages() {
            return !routedMessages.isEmpty();
        }

        public int getPoisonCount() {
            return routedMessages.size();
        }
    }
}
