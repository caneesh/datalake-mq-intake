package com.hcsc.datalake.mqintake.core.serializer;

import org.apache.hadoop.io.Writable;

import javax.jms.Message;

/**
 * Serializes JMS messages to SequenceFile key/value pairs.
 *
 * <p>Implementations live in binding modules (rms/, claims/), not in core.
 * Core handles the file I/O; this interface handles message serialization.
 *
 * <p>The key/value types are per-binding configuration. Metadata placement
 * (§9.1) is resolved in the implementation — core does not know the schema.
 */
public interface RecordSerializer {

    /**
     * Serializes a message to a key/value pair.
     *
     * @param message  the JMS message to serialize
     * @param metadata transport-level metadata (binding_id, mq_message_id, timestamps, etc.)
     * @return the serialized key/value pair
     * @throws SerializationException if serialization fails
     */
    SerializedRecord serialize(Message message, RecordMetadata metadata) throws SerializationException;

    /**
     * Returns the key class for SequenceFile.Writer configuration.
     */
    Class<? extends Writable> getKeyClass();

    /**
     * Returns the value class for SequenceFile.Writer configuration.
     */
    Class<? extends Writable> getValueClass();

    /**
     * A serialized key/value pair ready for SequenceFile writing.
     */
    class SerializedRecord {
        private final Writable key;
        private final Writable value;
        private final String identity;

        public SerializedRecord(Writable key, Writable value) {
            this(key, value, null);
        }

        /**
         * @param identity a value that uniquely identifies this record's
         *        payload, or null when the binding has none.
         *        <p><strong>This is never written to the SequenceFile.</strong>
         *        The file contract is a LongWritable byte offset and a Text
         *        payload, byte-comparable with the legacy MDB's output, and
         *        adding identity to either half would break that. It is carried
         *        alongside so the writer can record it in a sidecar index,
         *        which is what lets reconciliation identify a record without
         *        altering the data files.
         */
        public SerializedRecord(Writable key, Writable value, String identity) {
            this.key = key;
            this.value = value;
            this.identity = identity;
        }

        /** The payload identity, or null if the binding does not supply one. */
        public String getIdentity() {
            return identity;
        }

        public boolean hasIdentity() {
            return identity != null && !identity.isEmpty();
        }

        public Writable getKey() {
            return key;
        }

        public Writable getValue() {
            return value;
        }
    }

    /**
     * Exception thrown when serialization fails.
     */
    class SerializationException extends Exception {
        public SerializationException(String message) {
            super(message);
        }

        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
