package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PLACEHOLDER RecordSerializer for the RMS/HPS membership feed.
 *
 * <p><strong>WARNING: NON-CONTRACTUAL OUTPUT.</strong> This is a placeholder
 * implementation per DESIGN.md §9.1. Files produced by this serializer must
 * NOT be used as reference data for downstream consumers. The metadata
 * placement decision (open item #2) is not yet finalized.
 *
 * <p>Metadata captured per record (§9.2 key hierarchy):
 * <ul>
 *   <li>binding_id — provenance, which queue the record arrived on</li>
 *   <li>payload_guid — extracted from &lt;MessageID&gt; in the payload</li>
 *   <li>mq_message_id — transport-level identity from MQMD.MsgId</li>
 *   <li>mq_put_datetime — from MQMD.PutDate + PutTime</li>
 *   <li>consume_ts_utc — service clock at get</li>
 *   <li>source_file / record_offset — traceability back to landing file</li>
 * </ul>
 *
 * <p>This serializer does NOT parse business fields (grouping/sequence keys
 * like SubscriberIdNumber, SourceLastUpdateTS). Those already exist in the
 * payload and core must stay schema-agnostic.
 *
 * <p>Current placeholder approach: metadata in key (Text), payload in value
 * (BytesWritable). This matches Option A from §9.1 pending verification that
 * no consumer depends on current key semantics.
 */
public class RmsRecordSerializer implements RecordSerializer, PlaceholderSerializer {

    @Override
    public String getPlaceholderReason() {
        return "RMS serializer: metadata placement (Option A/B) per DESIGN.md §9.1 not finalized";
    }

    /**
     * Pattern to extract MessageID (payload GUID) from the XML payload.
     * Handles both raw and common XML structures.
     */
    private static final Pattern MESSAGE_ID_PATTERN = Pattern.compile(
            "<MessageID>([^<]+)</MessageID>",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Pattern for escaped variant: &lt;MessageID&gt;
     */
    private static final Pattern MESSAGE_ID_ESCAPED_PATTERN = Pattern.compile(
            "&lt;MessageID&gt;([^&]+)&lt;/MessageID&gt;",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public SerializedRecord serialize(Message message, RecordMetadata metadata)
            throws SerializationException {

        try {
            // Extract payload
            String payload = extractPayload(message);

            // Extract payload GUID from <MessageID> tag
            String payloadGuid = extractMessageId(payload);

            // Build metadata key (placeholder format — NOT contractual)
            String keyData = buildMetadataKey(metadata, payloadGuid);
            Text key = new Text(keyData);

            // Value is the raw payload bytes
            byte[] valueBytes = payload.getBytes(StandardCharsets.UTF_8);
            BytesWritable value = new BytesWritable(valueBytes);

            return new SerializedRecord(key, value);

        } catch (JMSException e) {
            throw new SerializationException("Failed to read message: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts the payload from a JMS message.
     */
    private String extractPayload(Message message) throws JMSException, SerializationException {
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            if (text == null) {
                throw new SerializationException("TextMessage body is null");
            }
            return text;
        }
        throw new SerializationException(
                "Unsupported message type: " + message.getClass().getName() +
                ". Expected TextMessage.");
    }

    /**
     * Extracts the MessageID (payload GUID) from the XML payload.
     * Tries raw tags first, then escaped tags.
     *
     * @param payload the XML payload
     * @return the MessageID value, or null if not found
     */
    private String extractMessageId(String payload) {
        if (payload == null) {
            return null;
        }

        // Try raw tag format first: <MessageID>...</MessageID>
        Matcher matcher = MESSAGE_ID_PATTERN.matcher(payload);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }

        // Try escaped tag format: &lt;MessageID&gt;...&lt;/MessageID&gt;
        Matcher escapedMatcher = MESSAGE_ID_ESCAPED_PATTERN.matcher(payload);
        if (escapedMatcher.find()) {
            return escapedMatcher.group(1).trim();
        }

        return null;
    }

    /**
     * Builds the metadata key string.
     *
     * <p><strong>PLACEHOLDER FORMAT — NOT CONTRACTUAL.</strong>
     * Format will change when metadata placement decision (item #2) is finalized.
     */
    private String buildMetadataKey(RecordMetadata metadata, String payloadGuid) {
        StringBuilder sb = new StringBuilder();
        sb.append("binding_id=").append(nullSafe(metadata.getBindingId()));
        sb.append("|payload_guid=").append(nullSafe(payloadGuid));
        sb.append("|mq_message_id=").append(nullSafe(metadata.getMqMessageId()));
        sb.append("|mq_put_datetime=").append(
                metadata.getMqPutTimestamp() != null ? metadata.getMqPutTimestamp().toString() : "");
        sb.append("|consume_ts_utc=").append(
                metadata.getConsumeTimestamp() != null ? metadata.getConsumeTimestamp().toString() : "");
        sb.append("|source_file=").append(nullSafe(metadata.getSourceFile()));
        sb.append("|record_offset=").append(metadata.getRecordOffset());
        return sb.toString();
    }

    private String nullSafe(String value) {
        return value != null ? value : "";
    }

    @Override
    public Class<? extends Writable> getKeyClass() {
        return Text.class;
    }

    @Override
    public Class<? extends Writable> getValueClass() {
        return BytesWritable.class;
    }
}
