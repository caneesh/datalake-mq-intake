package com.hcsc.datalake.mqintake.core.failure;

/**
 * Strategy for degrading batch size when a poison message is detected.
 *
 * <p>From DESIGN.md §6.1:
 * <ul>
 *   <li>RMS: degrades to batch size 1 (lower volume, acceptable)</li>
 *   <li>Claims: bisects (halve repeatedly) because dropping to batch-of-1
 *       on a high-volume feed means it may never catch up</li>
 * </ul>
 */
public enum DegradationStrategy {

    /**
     * Degrade to batch size 1 immediately.
     * Suitable for lower-volume feeds like RMS.
     */
    BATCH_OF_ONE {
        @Override
        public int degrade(int currentBatchSize, int normalBatchSize) {
            return 1;
        }

        @Override
        public String toString() {
            return "BATCH_OF_ONE";
        }
    },

    /**
     * Bisect: halve the batch size repeatedly to isolate the offender.
     * Suitable for high-volume feeds like claims.
     */
    BISECT {
        @Override
        public int degrade(int currentBatchSize, int normalBatchSize) {
            int halved = currentBatchSize / 2;
            return Math.max(1, halved);
        }

        @Override
        public String toString() {
            return "BISECT";
        }
    };

    /**
     * Computes the degraded batch size.
     *
     * @param currentBatchSize the current batch size
     * @param normalBatchSize  the normal (non-degraded) batch size
     * @return the new batch size after degradation
     */
    public abstract int degrade(int currentBatchSize, int normalBatchSize);
}
