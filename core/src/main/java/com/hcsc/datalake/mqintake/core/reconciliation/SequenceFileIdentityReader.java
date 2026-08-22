package com.hcsc.datalake.mqintake.core.reconciliation;

import com.hcsc.datalake.mqintake.core.audit.IdentityExtractor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Production identity reader for landed SequenceFiles.
 *
 * <p>Both binding serializers write a Text key of the form
 * {@code binding_id=...|payload_guid=...|mq_message_id=...|...}. This reader
 * iterates the file's keys and extracts {@code payload_guid}, falling back to
 * {@code mq_message_id} when the payload identity is absent (§10: identity is
 * payload_guid with mq_message_id as fallback).
 *
 * <p>Only keys are interpreted — record values are never deserialized beyond
 * what the SequenceFile format requires, keeping reconciliation cheap (§12).
 */
public class SequenceFileIdentityReader implements IdentityExtractor {

    private final Configuration conf;

    public SequenceFileIdentityReader(Configuration conf) {
        this.conf = Objects.requireNonNull(conf, "conf required");
    }

    @Override
    public Set<String> extractIdentities(String filePath) throws IOException {
        Set<String> identities = new HashSet<>();

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(new Path(filePath)))) {

            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                String identity = parseIdentity(key.toString());
                if (identity != null) {
                    identities.add(identity);
                }
            }
        }

        return identities;
    }

    @Override
    public int countRecords(String filePath) throws IOException {
        int count = 0;

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(new Path(filePath)))) {

            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                count++;
            }
        }

        return count;
    }

    /**
     * Parses the identity from a metadata key: payload_guid first,
     * mq_message_id fallback.
     */
    static String parseIdentity(String key) {
        String identity = parseField(key, "payload_guid");
        if (identity == null || identity.isEmpty()) {
            identity = parseField(key, "mq_message_id");
        }
        return (identity == null || identity.isEmpty()) ? null : identity;
    }

    private static String parseField(String key, String fieldName) {
        String marker = fieldName + "=";
        int start = key.indexOf(marker);
        if (start < 0) {
            return null;
        }
        start += marker.length();
        int end = key.indexOf('|', start);
        return end < 0 ? key.substring(start) : key.substring(start, end);
    }
}
