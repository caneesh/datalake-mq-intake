package com.hcsc.datalake.mqintake.claims.serializer;

import com.hcsc.datalake.mqintake.core.serializer.PayloadNormalizer;
import com.hcsc.datalake.mqintake.core.serializer.PlaceholderSerializer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Objects;

/**
 * PLACEHOLDER RecordSerializer for the DMIH claims feed.
 *
 * <p><strong>WARNING: NON-CONTRACTUAL OUTPUT.</strong> This is a placeholder
 * implementation per DESIGN.md §9.1. Files produced by this serializer must
 * NOT be used as reference data for downstream consumers. The metadata
 * placement decision (open item #2) is not yet finalized.
 *
 * <p><strong>LAND_ONLY mode:</strong> This feed has no tracker queue — messages
 * are landed to HDFS with no corresponding tracker message. The absence of
 * MessageHeaderDetails in claims messages is what triggered the §20.3 null
 * guard in the current tracker implementation.
 *
 * <p><strong>OPEN ITEM #17 — Identity field not located:</strong> RMS has
 * {@code <MessageID>} (UUID) as its payload GUID. Claims has no confirmed
 * equivalent. Candidates:
 * <ul>
 *   <li>{@code CLM_XMITSN_ID}</li>
 *   <li>{@code REC_CTL_NBR}</li>
 * </ul>
 *
 * <p>Identity extraction is implemented via {@link ClaimsIdentityExtractor},
 * a pluggable strategy. When the claims identity field is confirmed, swap
 * the extractor — nothing else changes.
 *
 * <p>Schema divergence from RMS (§19.1):
 * <ul>
 *   <li>Different root element and namespace</li>
 *   <li>{@code EVENT_TYPE} instead of {@code ActionCode} (both I/U/D)</li>
 *   <li>Timestamp format: {@code 2025-05-21-12.45.00.835539} vs
 *       {@code 20251107123318474775}</li>
 *   <li>{@code CLM_ADJSTMT_NBR} is source-monotonic revision counter</li>
 * </ul>
 */
public class ClaimsRecordSerializer implements RecordSerializer, PlaceholderSerializer {

    @Override
    public String getPlaceholderReason() {
        return "Claims serializer: metadata placement per DESIGN.md §9.1 not finalized, " +
               "identity field (open item #17) not confirmed";
    }

    private final ClaimsIdentityExtractor identityExtractor;
    private final boolean failOnMissingIdentity;

    /**
     * Creates a serializer for TEST/DEV use with the non-production fixture
     * extractor. Missing identity is tolerated.
     *
     * <p><strong>Not for production.</strong> Production wiring must use
     * {@link #ClaimsRecordSerializer(ClaimsIdentityExtractor, boolean)} with an
     * explicitly configured extractor and {@code failOnMissingIdentity=true}.
     */
    public ClaimsRecordSerializer() {
        this(ClaimsIdentityExtractor.nonProductionFixture(), false);
    }

    /**
     * Creates a serializer with a custom identity extractor.
     *
     * @param identityExtractor the strategy for extracting identity
     */
    public ClaimsRecordSerializer(ClaimsIdentityExtractor identityExtractor) {
        this(identityExtractor, false);
    }

    /**
     * Creates a serializer with a custom identity extractor and identity policy.
     *
     * @param identityExtractor     the strategy for extracting identity
     * @param failOnMissingIdentity when true, a payload whose identity field is
     *                              missing or blank fails serialization (and the
     *                              batch rolls back) instead of landing with a
     *                              null identity. Required for reconciliation.
     */
    public ClaimsRecordSerializer(ClaimsIdentityExtractor identityExtractor,
                                  boolean failOnMissingIdentity) {
        this.identityExtractor = Objects.requireNonNull(identityExtractor,
                "identityExtractor required");
        this.failOnMissingIdentity = failOnMissingIdentity;
    }

    @Override
    public SerializedRecord serialize(Message message, RecordMetadata metadata)
            throws SerializationException {

        try {
            // Normalise first (MDB parity: \n, \r, \t -> single space), then
            // extract identity FROM THE NORMALISED payload, so the identity
            // corresponds to what is actually written to the file rather than
            // to a raw form no reader could recover.
            String payload = PayloadNormalizer.normalize(extractPayload(message));

            String payloadIdentity = identityExtractor.extractIdentity(payload);
            if (payloadIdentity != null && payloadIdentity.isBlank()) {
                payloadIdentity = null;
            }
            if (payloadIdentity == null && failOnMissingIdentity) {
                throw new SerializationException(
                        "Claims identity field missing or blank in payload — " +
                        "identity is required for reconciliation (open item #17)");
            }

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


    @Override
    public Class<? extends Writable> getKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends Writable> getValueClass() {
        return Text.class;
    }

    /**
     * Returns the identity extractor in use.
     */
    public ClaimsIdentityExtractor getIdentityExtractor() {
        return identityExtractor;
    }
}
