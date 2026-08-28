package com.hcsc.datalake.mqintake.core.serializer;

/**
 * A serializer that counts payloads whose per-record identity could not be
 * extracted.
 *
 * <p>Exists so the runtime can wire that count into the binding's metrics
 * without core knowing any binding's schema: the factory checks for this
 * interface and, when present, publishes the count as a per-binding counter.
 * Before this, the count lived only on the serializer and in log lines —
 * a sustained upstream identity regression (which silently degrades every
 * affected file's reconciliation coverage) had no alertable signal.
 */
public interface ReportsIdentityMisses {

    /** Payloads seen without an extractable identity since startup. */
    long getIdentityMissCount();
}
