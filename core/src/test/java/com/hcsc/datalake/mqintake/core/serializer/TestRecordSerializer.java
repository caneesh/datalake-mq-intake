package com.hcsc.datalake.mqintake.core.serializer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Simple RecordSerializer for testing.
 * Writes the message ID as key and message body as value.
 */
public class TestRecordSerializer implements RecordSerializer {

    @Override
    public SerializedRecord serialize(Message message, RecordMetadata metadata)
            throws SerializationException {
        try {
            String key = metadata.getMqMessageId() != null
                    ? metadata.getMqMessageId()
                    : String.valueOf(metadata.getRecordOffset());

            String value;
            if (message instanceof TextMessage) {
                value = ((TextMessage) message).getText();
            } else {
                value = "non-text-message";
            }

            return new SerializedRecord(new Text(key), new Text(value));
        } catch (JMSException e) {
            throw new SerializationException("Failed to serialize message", e);
        }
    }

    @Override
    public Class<? extends Writable> getKeyClass() {
        return Text.class;
    }

    @Override
    public Class<? extends Writable> getValueClass() {
        return Text.class;
    }
}
