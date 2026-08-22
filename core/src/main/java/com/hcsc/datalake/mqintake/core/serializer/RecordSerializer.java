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

        public SerializedRecord(Writable key, Writable value) {
            this.key = key;
            this.value = value;
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
