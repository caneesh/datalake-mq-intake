package com.hcsc.datalake.mqintake.core.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Populates the backout-queue-depth gauge for one binding.
 *
 * <p>DESIGN §14 nominates backout depth as the pager condition — a message
 * reaching the backout queue means data was set aside and needs a human. The
 * gauge existed but nothing ever wrote to it, so the alert could not fire.
 *
 * <p>Depth is measured with a {@link QueueBrowser}, which reads without
 * consuming. There is no portable JMS API for queue depth, and the
 * alternatives are worse: PCF needs a command server and admin authority, and
 * the IBM-specific inquire APIs would tie this class to one provider. Browsing
 * is cheap here precisely because a healthy backout queue is empty — the cost
 * scales with the thing we are alerting on, and is capped (see
 * {@code maxBrowse}).
 *
 * <p>Threading: the poll runs on its own daemon thread and creates its own
 * short-lived {@link Session} for each sample. It never touches a listener
 * thread's session, consumer, or producer, per the one-session-per-thread
 * rule. The browse session is non-transacted and read-only, so it cannot
 * affect delivery.
 *
 * <p><strong>On failure the gauge is deliberately left alone</strong> rather
 * than zeroed. Zeroing on error would actively suppress a page — the one
 * outcome this metric exists to prevent. A stale non-zero reading keeps
 * alerting; {@link #isDepthAvailable()} and {@link #getLastSuccessfulPollMs()}
 * expose the staleness for diagnosis.
 */
public class BackoutQueueDepthMonitor {

    private static final Logger log = LoggerFactory.getLogger(BackoutQueueDepthMonitor.class);

    /**
     * Upper bound on messages counted per sample. A backout queue deep enough
     * to hit this is already far past the alert threshold, so the exact number
     * carries no extra operational meaning and is not worth an unbounded
     * enumeration.
     */
    static final int DEFAULT_MAX_BROWSE = 1000;

    /** Failures before the log escalates from WARN to ERROR. */
    private static final long FAILURES_BEFORE_ERROR = 3;

    private final String bindingId;
    private final String backoutQueueName;
    private final Connection connection;
    private final BindingMetrics metrics;
    private final long pollIntervalMs;
    private final int maxBrowse;

    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private final AtomicLong lastSuccessfulPollMs = new AtomicLong(0);
    private final AtomicBoolean depthAvailable = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile ScheduledExecutorService scheduler;

    public BackoutQueueDepthMonitor(String bindingId,
                                    String backoutQueueName,
                                    Connection connection,
                                    BindingMetrics metrics,
                                    long pollIntervalMs) {
        this(bindingId, backoutQueueName, connection, metrics, pollIntervalMs, DEFAULT_MAX_BROWSE);
    }

    public BackoutQueueDepthMonitor(String bindingId,
                                    String backoutQueueName,
                                    Connection connection,
                                    BindingMetrics metrics,
                                    long pollIntervalMs,
                                    int maxBrowse) {
        this.bindingId = Objects.requireNonNull(bindingId, "bindingId required");
        this.backoutQueueName = Objects.requireNonNull(backoutQueueName, "backoutQueueName required");
        this.connection = Objects.requireNonNull(connection, "connection required");
        this.metrics = Objects.requireNonNull(metrics, "metrics required");
        this.pollIntervalMs = pollIntervalMs;
        this.maxBrowse = maxBrowse;

        if (maxBrowse < 1) {
            throw new IllegalArgumentException("maxBrowse must be at least 1, got " + maxBrowse);
        }
    }

    /**
     * Starts periodic sampling. A non-positive poll interval disables the
     * monitor entirely, matching how {@code batch_interval_ms} disables the
     * flush timer.
     */
    public void start() {
        if (pollIntervalMs <= 0) {
            log.info("Backout depth monitoring disabled for binding '{}' (poll interval {}ms) — "
                    + "the backout-depth alert will not fire for this binding",
                    bindingId, pollIntervalMs);
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "boq-depth-" + bindingId);
            t.setDaemon(true);
            return t;
        });

        // Zero initial delay: without a sample at startup the gauge would read
        // its default of 0 for the first interval, which is indistinguishable
        // from a genuinely empty queue.
        scheduler.scheduleWithFixedDelay(
                this::pollQuietly, 0, pollIntervalMs, TimeUnit.MILLISECONDS);

        log.info("Backout depth monitoring started for binding '{}': queue='{}', every {}ms",
                bindingId, backoutQueueName, pollIntervalMs);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdownNow();
        }
        log.debug("Backout depth monitoring stopped for binding '{}'", bindingId);
    }

    /**
     * Wraps {@link #pollOnce()} so nothing escapes into the scheduler.
     * A task that throws out of scheduleWithFixedDelay is cancelled silently
     * and never runs again — the gauge would freeze at its last value with no
     * indication, which for a pager metric is worse than an obvious failure.
     */
    private void pollQuietly() {
        try {
            pollOnce();
        } catch (Throwable t) {
            recordFailure(t);
        }
    }

    /**
     * Takes one sample. Package-private so tests can drive it deterministically
     * rather than waiting on the scheduler.
     */
    void pollOnce() throws JMSException {
        Session session = null;
        QueueBrowser browser = null;
        try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(backoutQueueName);
            browser = session.createBrowser(queue);

            int depth = 0;
            Enumeration<?> messages = browser.getEnumeration();
            while (depth < maxBrowse && messages.hasMoreElements()) {
                messages.nextElement();
                depth++;
            }
            boolean capped = depth == maxBrowse && messages.hasMoreElements();

            metrics.setBackoutQueueDepth(depth);
            depthAvailable.set(true);
            lastSuccessfulPollMs.set(System.currentTimeMillis());

            long priorFailures = consecutiveFailures.getAndSet(0);
            if (priorFailures > 0) {
                log.info("Backout depth readable again for binding '{}' after {} failed attempts",
                        bindingId, priorFailures);
            }

            if (capped) {
                log.error("Binding '{}': backout queue '{}' holds at least {} messages "
                                + "(count capped) — PAGE",
                        bindingId, backoutQueueName, maxBrowse);
            } else if (depth > 0) {
                log.warn("Binding '{}': backout queue '{}' depth {} — messages are set aside "
                                + "and will not be processed without intervention",
                        bindingId, backoutQueueName, depth);
            } else {
                log.debug("Binding '{}': backout queue '{}' empty", bindingId, backoutQueueName);
            }

        } finally {
            closeQuietly(browser);
            closeQuietly(session);
        }
    }

    private void recordFailure(Throwable t) {
        depthAvailable.set(false);
        long failures = consecutiveFailures.incrementAndGet();

        // The gauge is intentionally NOT reset here — see the class comment.
        String message = "Binding '{}': could not read backout queue '{}' depth "
                + "(attempt {}). The backout-depth alert is running on a stale reading: {}";
        if (failures >= FAILURES_BEFORE_ERROR) {
            log.error(message, bindingId, backoutQueueName, failures, t.getMessage(), t);
        } else {
            log.warn(message, bindingId, backoutQueueName, failures, t.getMessage());
        }
    }

    private void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception e) {
            log.debug("Binding '{}': error closing browse resource: {}", bindingId, e.getMessage());
        }
    }

    /** True if the most recent sample succeeded. */
    public boolean isDepthAvailable() {
        return depthAvailable.get();
    }

    /** Epoch millis of the last successful sample, or 0 if none has succeeded. */
    public long getLastSuccessfulPollMs() {
        return lastSuccessfulPollMs.get();
    }

    public long getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    public boolean isRunning() {
        return running.get();
    }

    public String getBackoutQueueName() {
        return backoutQueueName;
    }
}
