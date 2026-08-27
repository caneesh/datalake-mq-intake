package com.hcsc.datalake.mqintake.core.audit;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;

import javax.jms.Message;
import java.io.IOException;
import java.util.List;

/**
 * Interface for emitting per-batch audit records.
 *
 * <p>Emitted BEFORE the MQ commit (see {@link #emit(AuditRecord)}), so every
 * committed batch has a record. This is the ABC posture: the audit is a
 * balancing control, and under {@code fail_batch_on_audit_error} an
 * unwritable audit rolls the batch back rather than committing unaudited data.
 *
 * <p>Implementations may write to HDFS, a database, or both. The audit record
 * is critical for reconciliation — commit state is ambiguous until reconciled.
 */
public interface AuditRecordEmitter {

    /**
     * An emitter that records nothing, for callers wired without audit
     * (tests, LAND-only harnesses). Production wiring always supplies a real
     * emitter; this exists so the receive loop can call the emitter
     * unconditionally.
     */
    static AuditRecordEmitter noop() {
        return new AuditRecordEmitter() {
            @Override
            public void emit(AuditRecord record) {
            }

            @Override
            public void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                             List<Message> messages) {
            }

            @Override
            public void emitBackoutOnly(String bindingId, List<Message> messages,
                                        int backoutCount) {
            }
        };
    }

    /**
     * Emits the audit record for a unit of work.
     *
     * <p>Called BEFORE the MQ commit, so that every committed batch has an
     * audit record. Writing after the commit leaves a window in which
     * committed data has no record, which a balancing control reads as loss —
     * the wrong direction to fail in. Written first, the crash window instead
     * yields an audited file whose messages are redelivered, which shows up as
     * a duplicate: detectable, and true.
     *
     * @param record the audit record to emit
     * @throws IOException if the audit record could not be written
     */
    void emit(AuditRecord record) throws IOException;

    /**
     * Convenience method to build and emit an audit record from batch results.
     *
     * @param bindingId   the binding identifier
     * @param writeResult the result from BatchWriter.write()
     * @param messages    the messages that were written
     * @throws IOException if the audit record could not be written
     */
    void emit(String bindingId, BatchWriter.BatchWriteResult writeResult, List<Message> messages) throws IOException;

    /**
     * Emits an audit record carrying the full balance for a unit of work.
     *
     * @param backoutCount messages consumed in this unit of work that were
     *                     routed to the backout queue rather than landed.
     *                     Without it the balance cannot close: a batch that
     *                     consumed 10 and landed 9 would look like a loss of 1.
     */
    default void emit(String bindingId, BatchWriter.BatchWriteResult writeResult,
                      List<Message> messages, int backoutCount) throws IOException {
        emit(bindingId, writeResult, messages);
    }

    /**
     * Emits an audit record for a unit of work that landed nothing.
     *
     * <p>A batch consisting entirely of poison messages commits without
     * writing a file. It still consumed messages, so without a record of its
     * own those messages appear in no audit anywhere and the balance shows
     * them as lost.
     */
    void emitBackoutOnly(String bindingId, List<Message> messages, int backoutCount)
            throws IOException;

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
