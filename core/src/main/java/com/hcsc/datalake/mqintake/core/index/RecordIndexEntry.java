package com.hcsc.datalake.mqintake.core.index;

import java.util.Objects;

/**
 * One record's position in a landed file, and what it is.
 *
 * <p>Deliberately just these two fields. The payload is already in the data
 * file; copying it here would double the storage cost of every feed to serve a
 * reconciliation lookup.
 */
public final class RecordIndexEntry {

    private final long byteOffset;
    private final String identity;

    public RecordIndexEntry(long byteOffset, String identity) {
        this.byteOffset = byteOffset;
        this.identity = identity;
    }

    /** Byte position of the record in the SequenceFile — the file's own key. */
    public long getByteOffset() {
        return byteOffset;
    }

    /** The payload identity, or null when the binding supplies none. */
    public String getIdentity() {
        return identity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordIndexEntry)) {
            return false;
        }
        RecordIndexEntry that = (RecordIndexEntry) o;
        return byteOffset == that.byteOffset && Objects.equals(identity, that.identity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(byteOffset, identity);
    }

    @Override
    public String toString() {
        return "RecordIndexEntry{offset=" + byteOffset + ", identity='" + identity + "'}";
    }
}
