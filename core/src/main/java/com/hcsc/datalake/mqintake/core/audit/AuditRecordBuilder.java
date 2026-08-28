package com.hcsc.datalake.mqintake.core.audit;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;

import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Builds audit records from batch write results.
 *
 * <p>From DESIGN.md §12: Per-batch, immediately after a successful commit,
 * the writer emits an audit record containing binding_id, partition path,
 * filename, record count, byte count, first/last identity value, instance ID,
 * commit timestamp.
 */
public class AuditRecordBuilder {

    private final String instanceId;
    private final Clock clock;
    private final Function<Message, String> identityExtractor;

    /**
     * Creates a builder with the default identity extractor.
     * The default extractor tries to get payload_guid property first,
     * then falls back to JMS message ID.
     */
    public AuditRecordBuilder(String instanceId, Clock clock) {
        this(instanceId, clock, AuditRecordBuilder::extractDefaultIdentity);
    }

    /**
     * Creates a builder with a custom identity extractor.
     *
     * @param instanceId        the instance identifier
     * @param clock             clock for timestamps
     * @param identityExtractor function to extract identity from a message
     */
    public AuditRecordBuilder(String instanceId, Clock clock,
                               Function<Message, String> identityExtractor) {
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId required");
        this.clock = Objects.requireNonNull(clock, "clock required");
        this.identityExtractor = Objects.requireNonNull(identityExtractor, "identityExtractor required");
    }

    /**
     * Builds an audit record for a completed batch.
     *
     * @param bindingId   the binding identifier
     * @param writeResult the result from BatchWriter.write()
     * @param messages    the messages that were written
     * @return the audit record
     */
    public AuditRecord build(String bindingId,
                              BatchWriter.BatchWriteResult writeResult,
                              List<Message> messages) {
        return build(bindingId, writeResult, messages, 0);
    }

    /**
     * Builds a record carrying the full balance for a unit of work.
     *
     * @param backoutCount messages consumed but routed to the backout queue,
     *                     without which {@code consumed == landed} would be a
     *                     false statement for any batch containing poison
     */
    public AuditRecord build(String bindingId,
                              BatchWriter.BatchWriteResult writeResult,
                              List<Message> messages,
                              int backoutCount) {
        return build(bindingId, writeResult, messages, backoutCount, -1);
    }

    /**
     * Builds a record carrying an independently observed consumed count.
     *
     * @param consumedCount the MQ batch size as seen by the receive loop, or
     *                      negative when no source-side observation exists
     *                      (the record then derives it)
     */
    public AuditRecord build(String bindingId,
                              BatchWriter.BatchWriteResult writeResult,
                              List<Message> messages,
                              int backoutCount,
                              int consumedCount) {
        Objects.requireNonNull(bindingId, "bindingId required");
        Objects.requireNonNull(writeResult, "writeResult required");
        Objects.requireNonNull(messages, "messages required");

        if (messages.isEmpty()) {
            throw new IllegalArgumentException("messages cannot be empty");
        }

        // Extract partition path and filename from full path
        String filePath = writeResult.getFilePath();
        int lastSlash = filePath.lastIndexOf('/');
        String partitionPath = lastSlash > 0 ? filePath.substring(0, lastSlash) : "";
        String filename = lastSlash >= 0 ? filePath.substring(lastSlash + 1) : filePath;

        // Extract first and last identity values
        String firstIdentity = identityExtractor.apply(messages.get(0));
        String lastIdentity = identityExtractor.apply(messages.get(messages.size() - 1));

        AuditRecord.Builder builder = AuditRecord.builder()
                .bindingId(bindingId)
                .partitionPath(partitionPath)
                .filename(filename)
                .recordCount(writeResult.getRecordCount())
                .byteCount(writeResult.getByteCount())
                .backoutCount(backoutCount)
                .firstIdentity(firstIdentity)
                .lastIdentity(lastIdentity)
                .instanceId(instanceId)
                .commitTimestamp(Instant.now(clock));
        if (consumedCount >= 0) {
            builder.consumedCount(consumedCount);
        }
        return builder.build();
    }

    /**
     * Default identity extractor: tries payload_guid property, falls back to JMS message ID.
     */
    private static String extractDefaultIdentity(Message message) {
        if (message == null) {
            return null;
        }
        try {
            // Try payload_guid first (application-level identity)
            if (message.propertyExists("payload_guid")) {
                return message.getStringProperty("payload_guid");
            }
            // Fall back to JMS message ID (transport-level identity)
            return message.getJMSMessageID();
        } catch (JMSException e) {
            return null;
        }
    }

    /**
     * Extracts identity from a message using this builder's configured extractor.
     */
    public String extractIdentity(Message message) {
        return identityExtractor.apply(message);
    }

    public String getInstanceId() {
        return instanceId;
    }
}
