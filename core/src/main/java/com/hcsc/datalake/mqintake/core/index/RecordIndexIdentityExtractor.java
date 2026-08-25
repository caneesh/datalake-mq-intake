package com.hcsc.datalake.mqintake.core.index;

import com.hcsc.datalake.mqintake.core.audit.IdentityExtractor;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Supplies record identities from the sidecar index.
 *
 * <p>Reconciliation previously asked the SequenceFile itself, which under the
 * production contract holds a byte offset and a payload and no identity at
 * all — so it always found nothing and reconciliation could conclude nothing.
 * This reads the index written beside the file instead.
 *
 * <p>Falls back to the previous extractor for files with no index: everything
 * landed before indexing was enabled, everything from a binding that has it
 * off, and the occasional file whose index write was interrupted. The fallback
 * still supplies record counts, which is what count-based checks need.
 *
 * <p>A partially identified index is treated as no index. Identity-set
 * comparison against a set that is missing entries reports those records as
 * losses, which is a worse answer than admitting the file cannot be identified.
 */
public class RecordIndexIdentityExtractor implements IdentityExtractor {

    private static final Logger log = LoggerFactory.getLogger(RecordIndexIdentityExtractor.class);

    private final RecordIndexReader indexReader;
    private final IdentityExtractor fallback;

    public RecordIndexIdentityExtractor(RecordIndexReader indexReader, IdentityExtractor fallback) {
        this.indexReader = Objects.requireNonNull(indexReader, "indexReader required");
        this.fallback = Objects.requireNonNull(fallback, "fallback required");
    }

    @Override
    public Set<String> extractIdentities(String filePath) throws IOException {
        Optional<RecordIndex> index = indexReader.read(new Path(filePath));

        if (index.isEmpty()) {
            log.debug("No usable record index for {} — falling back to the file reader", filePath);
            return fallback.extractIdentities(filePath);
        }

        if (!index.get().isFullyIdentified()) {
            log.warn("Record index for {} identifies only some records — treating the file as "
                    + "unidentifiable rather than reporting the rest as missing", filePath);
            return Set.of();
        }

        return indexReader.readIdentities(new Path(filePath));
    }

    @Override
    public int countRecords(String filePath) throws IOException {
        Optional<RecordIndex> index = indexReader.read(new Path(filePath));
        if (index.isPresent()) {
            return index.get().getRecordCount();
        }
        return fallback.countRecords(filePath);
    }

    /** True when this file can be identified from its index. */
    public boolean hasUsableIndex(String filePath) throws IOException {
        return indexReader.read(new Path(filePath))
                .map(RecordIndex::isFullyIdentified)
                .orElse(false);
    }
}
