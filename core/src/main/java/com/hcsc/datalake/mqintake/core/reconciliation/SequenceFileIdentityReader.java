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
import java.util.function.Function;

/**
 * Reads record identities and counts straight from a landed SequenceFile.
 *
 * <p>Production files carry a {@code LongWritable} byte-offset key and a
 * {@code Text} payload value — the legacy MDB contract, which has no room for
 * identity. Identity is therefore recovered from the <em>value</em>, using
 * the binding's own extractor ({@code RecordSerializer.identityOf}), which is
 * the same function that supplied the identity at write time.
 *
 * <p>Without a value extractor this falls back to parsing a composite
 * metadata key ({@code payload_guid=...|mq_message_id=...}) — a layout only
 * test fixtures still write. Against a production file that fallback finds
 * nothing and says so once, and every orphan then classifies INCONCLUSIVE,
 * which is the safe direction (INCONCLUSIVE means KEEP).
 *
 * <p>Where a binding writes a sidecar index, {@link
 * com.hcsc.datalake.mqintake.core.index.RecordIndexIdentityExtractor} reads
 * that first and delegates here only for files that have none. The record
 * COUNT always comes from here.
 */
public class SequenceFileIdentityReader implements IdentityExtractor {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(SequenceFileIdentityReader.class);

    private final Configuration conf;
    private final Function<String, String> valueIdentity;   // null: key-based fallback
    private final java.util.concurrent.atomic.AtomicBoolean warnedNoIdentity =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    /** Key-based reader: fixtures and pre-production layouts only. */
    public SequenceFileIdentityReader(Configuration conf) {
        this(conf, null);
    }

    /**
     * @param valueIdentity recovers a record's identity from its Text value;
     *                      null selects the composite-key fallback
     */
    public SequenceFileIdentityReader(Configuration conf, Function<String, String> valueIdentity) {
        this.conf = Objects.requireNonNull(conf, "conf required");
        this.valueIdentity = valueIdentity;
    }

    @Override
    public Set<String> extractIdentities(String filePath) throws IOException {
        Set<String> identities = new HashSet<>();

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(new Path(filePath)))) {

            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                String identity = valueIdentity != null
                        ? valueIdentity.apply(value.toString())
                        : parseIdentity(key.toString());
                if (identity != null && !identity.isEmpty()) {
                    identities.add(identity);
                }
            }

            if (identities.isEmpty() && warnedNoIdentity.compareAndSet(false, true)) {
                if (valueIdentity != null) {
                    log.warn("No record identities recovered from {} — the binding's extractor "
                            + "found nothing in any value. Orphans in such files classify "
                            + "INCONCLUSIVE and are KEPT.", filePath);
                } else {
                    log.warn("No record identities found in {} (key class {}). This reader has "
                            + "no value extractor and the key carries no identity, so "
                            + "reconciliation cannot classify duplicates and will report "
                            + "INCONCLUSIVE (files are KEPT).",
                            filePath, reader.getKeyClass().getSimpleName());
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
