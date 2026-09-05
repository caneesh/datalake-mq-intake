package com.hcsc.datalake.mqintake.core.loop.recovery;

import com.hcsc.datalake.mqintake.core.lifecycle.BindingHealthManager;
import com.hcsc.datalake.mqintake.core.loop.session.ListenerSession;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Rebuilds a listener's session after a fault, with a bounded budget.
 *
 * <p>Extracted from the receive loop unchanged. It was one of the two most
 * self-contained responsibilities there — it touches the session, a backoff
 * policy, a fault policy and the health and metrics sinks, and nothing in the
 * loop's transaction path — and it already had direct test coverage, which is
 * why it moved first.
 *
 * <p>Confined to its owning listener thread, like the session it rebuilds. The
 * attempt counter is atomic only because it is read from outside for
 * reporting, not because two threads recover at once.
 */
public class SessionRecoveryCoordinator {

    private static final Logger log = LoggerFactory.getLogger(SessionRecoveryCoordinator.class);

    /**
     * Attempts before the listener gives up and stops.
     *
     * <p>The budget resets on every successful recovery, so this bounds one
     * incident rather than the process lifetime.
     */
    public static final int MAX_RECONNECT_ATTEMPTS = 10;

    private final String bindingId;
    private final ListenerSession listenerSession;
    private final SessionFaultPolicy faultPolicy;
    private final BackoffPolicy backoffPolicy;
    private final BindingHealthManager healthManager;
    private final BindingMetrics metrics;

    /** True while the loop still wants to run; recovery aborts when it is not. */
    private final BooleanSupplier running;

    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicLong reconnectCount = new AtomicLong(0);

    public SessionRecoveryCoordinator(String bindingId,
                                      ListenerSession listenerSession,
                                      SessionFaultPolicy faultPolicy,
                                      BackoffPolicy backoffPolicy,
                                      BindingHealthManager healthManager,
                                      BindingMetrics metrics,
                                      BooleanSupplier running) {
        this.bindingId = bindingId;
        this.listenerSession = listenerSession;
        this.faultPolicy = faultPolicy;
        this.backoffPolicy = backoffPolicy;
        this.healthManager = healthManager;   // may be absent
        this.metrics = metrics;
        this.running = running;
    }

    /**
     * Rebuilds the session, retrying with backoff until it works or the budget
     * runs out.
     *
     * <p>Iterative on purpose. An earlier version retried by recursing, which
     * was safe only because the budget is a hardcoded ten — each level's stack
     * frame stays live for the whole remaining backoff. The moment the budget
     * becomes configurable, recursion depth scales with it.
     *
     * @return true if the session is open again; false after the budget is
     *         exhausted, a fatal fault, or an interrupt
     */
    public boolean recover() {
        Outcome outcome;
        do {
            outcome = recoverOnce();
        } while (outcome == Outcome.RETRY);
        return outcome == Outcome.RECOVERED;
    }

    /** Successful recoveries since startup. */
    public long getReconnectCount() {
        return reconnectCount.get();
    }

    /** Attempts spent on the CURRENT incident; zero when nothing is wrong. */
    public int getCurrentAttempts() {
        return reconnectAttempts.get();
    }

    private enum Outcome {
        /** The session is open again; the receive loop can resume. */
        RECOVERED,
        /** This attempt failed but the next one might not. */
        RETRY,
        /** Stop recovering: budget exhausted, fatal fault, or shutting down. */
        GIVE_UP
    }

    /** One attempt: close, back off, reopen. */
    private Outcome recoverOnce() {
        int attempts = reconnectAttempts.incrementAndGet();

        if (attempts > MAX_RECONNECT_ATTEMPTS) {
            log.error("Max reconnect attempts ({}) exceeded for binding '{}'",
                    MAX_RECONNECT_ATTEMPTS, bindingId);
            if (healthManager != null) {
                healthManager.recordUnhealthy(bindingId,
                        new RuntimeException("Max reconnect attempts exceeded"));
            }
            metrics.recordReconnectFailure();
            return Outcome.GIVE_UP;
        }

        log.warn("Attempting session recovery for binding '{}' (attempt {}/{})",
                bindingId, attempts, MAX_RECONNECT_ATTEMPTS);

        if (healthManager != null) {
            healthManager.recordRecovering(bindingId,
                    String.format("Session reconnect attempt %d/%d",
                            attempts, MAX_RECONNECT_ATTEMPTS));
        }

        listenerSession.close();

        long backoffMs = backoffPolicy.backoffFor(attempts).toMillis();
        log.debug("Waiting {}ms before reconnect attempt {} for binding '{}'",
                backoffMs, attempts, bindingId);

        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.info("Reconnect wait interrupted for binding '{}'", bindingId);
            return Outcome.GIVE_UP;
        }

        if (!running.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            log.info("Recovery aborted - loop stopping for binding '{}'", bindingId);
            return Outcome.GIVE_UP;
        }

        try {
            listenerSession.open();
            // Fresh budget for the next incident. Plain state — the accessor
            // and the next recovery read it — not a success signal.
            reconnectAttempts.set(0);
            reconnectCount.incrementAndGet();
            log.info("Session recovered successfully for binding '{}' after {} attempt(s)",
                    bindingId, attempts);

            metrics.recordReconnect();

            if (healthManager != null) {
                healthManager.recordHealthy(bindingId);
            }

            return Outcome.RECOVERED;
        } catch (JMSException e) {
            log.error("Session recovery attempt {} failed for binding '{}': {}",
                    attempts, bindingId, e.getMessage());

            if (faultPolicy.isFatal(e)) {
                log.error("Non-recoverable error detected for binding '{}', stopping recovery",
                        bindingId);
                return Outcome.GIVE_UP;
            }

            return Outcome.RETRY;
        }
    }
}
