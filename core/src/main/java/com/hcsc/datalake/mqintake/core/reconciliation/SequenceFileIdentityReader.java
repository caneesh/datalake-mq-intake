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
 * Identity reader for landed SequenceFiles carrying a composite metadata key.
 *
 * <p>Reads a Text key of the form
 * {@code binding_id=...|payload_guid=...|mq_message_id=...|...}, extracting
 * {@code payload_guid} and falling back to {@code mq_message_id} when the
 * payload identity is absent (§10: identity is payload_guid with
 * mq_message_id as fallback).
 *
 * <p>Only keys are interpreted — record values are never deserialized beyond
 * what the SequenceFile format requires, keeping reconciliation cheap (§12).
 *
 * <p><strong>Not applicable to the production layout.</strong> Production
 * SequenceFiles use a {@code LongWritable} positional key, which carries no
 * identity, and the binding serializers now match that. Against such a file
 * this reader returns no identities, which propagates as
 * {@code INCONCLUSIVE} through {@link com.hcsc.datalake.mqintake.core.audit.OrphanFileClassifier}
 * — the safe direction, since INCONCLUSIVE means KEEP, never delete. It does
 * mean reconciliation cannot classify duplicates until metadata placement
 * (open item #2) gives identity a home; a sidecar file (Option C) would
 * restore it without altering the data files. The condition is logged once per
 * reader rather than failing silently.
 */
public class SequenceFileIdentityReader implements IdentityExtractor {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(SequenceFileIdentityReader.class);

    private final Configuration conf;
    private final java.util.concurrent.atomic.AtomicBoolean warnedNoIdentityKey =
            new java.util.concurrent.atomic.AtomicBoolean(false);

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

            if (identities.isEmpty() && warnedNoIdentityKey.compareAndSet(false, true)) {
                log.warn("No record identities found in {} (key class {}). Files using the " +
                        "production positional key carry no identity, so reconciliation " +
                        "cannot classify duplicates and will report INCONCLUSIVE (files are " +
                        "KEPT). Resolve metadata placement (open item #2) to restore this.",
                        filePath, reader.getKeyClass().getSimpleName());
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
