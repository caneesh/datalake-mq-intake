package com.hcsc.datalake.mqintake.core.failuremode;

/**
 * Fault injection interface for failure-mode testing.
 *
 * <p>Hooks are called at real transaction boundaries, not mocked layers.
 * This ensures tests verify actual delivery guarantees, not test doubles.
 *
 * <p>The crash windows from DESIGN.md §12.1:
 * <ul>
 *   <li>RECEIVING: messages in memory only</li>
 *   <li>WRITING: SequenceFile exists only in _tmp</li>
 *   <li>LANDED: file closed and renamed, MQ uncommitted</li>
 *   <li>MQ_COMMITTED: delivery commitment point</li>
 *   <li>AUDITED: post-commit audit written</li>
 * </ul>
 */
public interface FaultInjector {

    /**
     * Called before any batch processing starts.
     * Inject fault here to simulate crash with batch only in memory.
     * Test 1: All messages should redeliver, no file appears.
     */
    default void beforeBatchProcess() throws FaultException {}

    /**
     * Called during SequenceFile write (while file is in _tmp).
     * Inject fault here to simulate crash mid-write.
     * Test 2: Messages redeliver, debris confined to _tmp.
     */
    default void duringHdfsWrite() throws FaultException {}

    /**
     * Called after HDFS file close (durability barrier) but before rename.
     * Inject fault here to simulate crash after write, before visibility.
     */
    default void afterHdfsClose() throws FaultException {}

    /**
     * Called after HDFS rename (visibility barrier) but before MQ commit.
     * Inject fault here to simulate crash when file is visible but MQ uncommitted.
     * Test 3: Duplicate data on replay, but no loss.
     */
    default void afterHdfsRename() throws FaultException {}

    /**
     * Called after tracker puts (TRACKED mode) but before commit.
     * Inject fault here to verify no tracker message escapes.
     * Test 5: CRITICAL — false success signal downstream is worse than duplicate.
     */
    default void afterTrackerPuts() throws FaultException {}

    /**
     * Called after MQ commit but before audit write.
     * Inject fault here to test orphan file handling.
     * Test 4: File must not be auto-deleted.
     */
    default void afterMqCommit() throws FaultException {}

    /**
     * Called after audit record is written.
     */
    default void afterAuditWrite() throws FaultException {}

    /**
     * No-op implementation — normal operation with no faults.
     */
    FaultInjector NONE = new FaultInjector() {};

    /**
     * Exception thrown to simulate a crash/failure at an injection point.
     */
    class FaultException extends Exception {
        private final boolean simulateJvmCrash;

        public FaultException(String message) {
            this(message, false);
        }

        public FaultException(String message, boolean simulateJvmCrash) {
            super(message);
            this.simulateJvmCrash = simulateJvmCrash;
        }

        public boolean isSimulateJvmCrash() {
            return simulateJvmCrash;
        }
    }
}
