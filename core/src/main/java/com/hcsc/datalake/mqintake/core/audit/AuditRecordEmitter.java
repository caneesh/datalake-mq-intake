package com.hcsc.datalake.mqintake.core.audit;

import java.io.IOException;

/**
 * Interface for emitting audit records after successful commits.
 *
 * <p>From DESIGN.md §12: immediately after a successful commit, the writer
 * emits an audit record to an audit store (HDFS audit path or a control table).
 *
 * <p>Implementations may write to HDFS, a database, or both. The audit record
 * is critical for reconciliation — commit state is ambiguous until reconciled.
 */
public interface AuditRecordEmitter {

    /**
     * Emits an audit record after a successful commit.
     *
     * <p>This should be called AFTER the MQ commit succeeds. A crash between
     * MQ commit and audit write yields a committed, valid data file with no
     * audit record — this is acceptable and handled by the reconciliation
     * process via message-ID set comparison (§10).
     *
     * @param record the audit record to emit
     * @throws IOException if the audit record could not be written
     */
    void emit(AuditRecord record) throws IOException;

    /**
     * Flushes any buffered audit records. Some implementations may buffer
     * records for batched writes.
     *
     * @throws IOException if the flush fails
     */
    default void flush() throws IOException {
        // Default no-op for non-buffering implementations
    }

    /**
     * Closes the emitter and releases any resources.
     *
     * @throws IOException if closing fails
     */
    default void close() throws IOException {
        flush();
    }
}
