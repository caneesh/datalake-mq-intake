package com.hcsc.datalake.mqintake.core.index;

import java.io.IOException;

/**
 * Persists the sidecar index for a landed file.
 *
 * <p>An interface so a binding without a usable payload identity — Claims,
 * whose per-message key is still an open question — can be given
 * {@link #disabled()} rather than writing an index that could not be trusted.
 */
public interface RecordIndexWriter {

    /**
     * Writes the index for one file.
     *
     * <p>Called after the data file is renamed into its partition and before
     * the MQ commit. The index therefore describes a file that already exists;
     * see {@code HdfsRecordIndexWriter} for the consistency model.
     */
    void write(RecordIndex index) throws IOException;

    /** True when this writer actually persists anything. */
    default boolean isEnabled() {
        return true;
    }

    /** A writer that does nothing, for bindings with no trustworthy identity. */
    static RecordIndexWriter disabled() {
        return new RecordIndexWriter() {
            @Override
            public void write(RecordIndex index) {
                // deliberately nothing
            }

            @Override
            public boolean isEnabled() {
                return false;
            }
        };
    }
}
