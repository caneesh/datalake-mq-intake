package com.hcsc.datalake.mqintake.core.config;

/**
 * Tracker notification mode for a binding.
 *
 * TRACKED: N gets + N tracker puts = 2N uncommitted messages per batch.
 * LAND_ONLY: N gets = N uncommitted messages per batch, no tracker notification.
 */
public enum BindingMode {

    /**
     * Publishes one tracker message per source message.
     * Unit of work is 2N, so batch_size must be ≤ MAXUMSGS / 2.
     */
    TRACKED,

    /**
     * Acknowledges MQ only, publishes nothing to a tracker queue.
     * Unit of work is N, so batch_size may be up to MAXUMSGS.
     */
    LAND_ONLY
}
