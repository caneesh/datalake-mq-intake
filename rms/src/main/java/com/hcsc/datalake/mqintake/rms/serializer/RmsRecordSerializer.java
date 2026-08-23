package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
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
 * <p><strong>Layout:</strong> {@code LongWritable} key, {@code Text} value —
 * matching the production SequenceFile types established from the legacy MDB.
 * The key is a positional ordinal; the exact expression the live writer uses is
 * still unconfirmed (MDB_QUESTIONS A3), so the type matches production while the
 * value does not yet.
 *
 * <p><strong>The metadata above is therefore NOT written to the file.</strong>
 * It previously rode in a composite Text key (Option A, §9.1), which the
 * production types rule out — a {@code LongWritable} key has no room for it.
 * Until metadata placement (open item #2) is resolved, records carry no
 * payload_guid, so reconciliation and §10 orphan classification cannot identify
 * records from file contents. Option C (sidecar file) would restore it without
 * altering these data files. {@link #extractPayloadGuid} remains available for
 * whichever placement is chosen.
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


            // Production layout: LongWritable key, Text value (§9.1).
            // The key is a positional ordinal. The exact expression the live
            // MDB uses is still unconfirmed (MDB_QUESTIONS A3) — it seeds from
            // getLength() and increments — so the TYPE matches production while
            // the VALUE does not yet.
            LongWritable key = new LongWritable(metadata.getRecordOffset());

            // Value is the payload as Text, matching the production value type.
            // NOTE: not yet whitespace-normalised the way the MDB's
            // processMessage does (\n, \r, \t → single space). Tracked as
            // readiness blocker D2.
            Text value = new Text(payload);

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
    public String extractPayloadGuid(String payload) {
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


    @Override
    public Class<? extends Writable> getKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends Writable> getValueClass() {
        return Text.class;
    }
}
