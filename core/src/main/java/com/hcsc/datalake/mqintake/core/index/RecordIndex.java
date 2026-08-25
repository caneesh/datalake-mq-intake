package com.hcsc.datalake.mqintake.core.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The sidecar index for one landed SequenceFile.
 *
 * <p>Reconciliation needs to know which records a file contains. It cannot
 * learn that from the file itself: the production contract is a
 * {@code LongWritable} byte offset and a {@code Text} payload, and neither
 * carries identity. Adding it would break byte-parity with the legacy MDB's
 * output, so the index lives beside the file instead of inside it.
 */
public final class RecordIndex {

    private final String bindingId;
    private final String filename;
    private final String partitionPath;
    private final String instanceId;
    private final List<RecordIndexEntry> entries;

    public RecordIndex(String bindingId, String filename, String partitionPath,
                       String instanceId, List<RecordIndexEntry> entries) {
        this.bindingId = Objects.requireNonNull(bindingId, "bindingId required");
        this.filename = Objects.requireNonNull(filename, "filename required");
        this.partitionPath = Objects.requireNonNull(partitionPath, "partitionPath required");
        this.instanceId = instanceId;
        this.entries = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(entries, "entries required")));
    }

    public String getBindingId() {
        return bindingId;
    }

    public String getFilename() {
        return filename;
    }

    public String getPartitionPath() {
        return partitionPath;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public List<RecordIndexEntry> getEntries() {
        return entries;
    }

    public int getRecordCount() {
        return entries.size();
    }

    /**
     * True when every record carries an identity.
     *
     * <p>A partially-identified index is worse than none for identity-set
     * comparison: the missing records look like losses. Reconciliation checks
     * this before trusting the index.
     */
    public boolean isFullyIdentified() {
        return entries.stream().allMatch(e -> e.getIdentity() != null && !e.getIdentity().isEmpty());
    }

    @Override
    public String toString() {
        return "RecordIndex{binding='" + bindingId + "', file='" + filename
                + "', records=" + entries.size() + ", fullyIdentified=" + isFullyIdentified() + "}";
    }
}
