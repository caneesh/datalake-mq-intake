package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.serializer.PayloadNormalizer;
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
 * Production RecordSerializer for the RMS/HPS membership feed.
 *
 * <p><strong>Output matches the legacy MDB's SequenceFile contract exactly:</strong>
 * <ul>
 *   <li>key — {@code LongWritable} holding the record's byte offset in the
 *       file, reproducing the legacy writer's
 *       {@code long offset = sequenceFileWriter.getLength()} read immediately
 *       before each append (MDB_QUESTIONS A3, since confirmed against the
 *       MDB source)</li>
 *   <li>value — {@code Text} holding the payload after the MDB's whitespace
 *       normalisation ({@code \n}, {@code \r}, {@code \t} each to one space,
 *       no collapsing, no trim)</li>
 * </ul>
 *
 * <p>Both halves are now confirmed rather than assumed, which is why this is no
 * longer marked as a placeholder: a file written here is byte-comparable with
 * one the legacy MDB would have written for the same input.
 *
 * <p>This serializer does NOT parse business fields (grouping/sequence keys
 * like SubscriberIdNumber, SourceLastUpdateTS). Those already exist in the
 * payload and core must stay schema-agnostic.
 *
 * <p><strong>Known limitation — record identity is not in the file.</strong>
 * A {@code LongWritable} key has no room for {@code payload_guid}, and adding
 * it to the value would break parity, so reconciliation and §10 orphan
 * classification cannot identify a record from the file's contents alone. That
 * is a gap in reconciliation, not a defect in the output: the legacy MDB never
 * carried identity either, and it is addressed by sidecar metadata alongside
 * the data files. {@link #extractPayloadGuid} is what supplies the identity to
 * that sidecar.
 */
public class RmsRecordSerializer implements RecordSerializer {

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
            // Extract the body, then apply the MDB's whitespace normalisation
            // (\n, \r, \t -> single space, no collapsing, no trim). Consumers
            // have only ever seen normalised payloads, so this is parity.
            String payload = PayloadNormalizer.normalize(extractPayload(message));


            // Production layout: LongWritable key, Text value (§9.1).
            // The key is the record's byte offset in the file, matching the
            // legacy writer's `long offset = sequenceFileWriter.getLength()`
            // read before each append (MDB_QUESTIONS A3).
            LongWritable key = new LongWritable(metadata.getFileByteOffset());
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
