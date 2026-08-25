package com.hcsc.datalake.mqintake.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.Locale;

/**
 * This process's identity, as it appears in HDFS paths and filenames.
 *
 * <p>The instance id is not decoration. It appears in two places where a
 * collision does damage:
 * <ul>
 *   <li>{@code {base}/_tmp/{instanceId}/} — the temp directory a process
 *       sweeps on startup. §8.1 is explicit that an instance sweeps only its
 *       <em>own</em> subtree, which holds exactly as long as the id is
 *       unique. Two processes sharing an id share a temp directory, and one
 *       starting up can delete a file the other is still writing.</li>
 *   <li>{@code {binding}_{instanceId}_{epochMs}_{batchSeq}.seq} — the landed
 *       filename. {@code batchSeq} is a per-process counter starting at zero,
 *       so two processes with the same id that start together produce
 *       identical filenames for different data.</li>
 * </ul>
 *
 * <p>The default was {@code ${HOSTNAME}}, which is unique per host, not per
 * process. Two intake JVMs on one host — a rolling deploy, two replicas
 * scheduled together, a container sharing the host network namespace — would
 * collide. The default is now {@code hostname-pid}.
 *
 * <p><strong>Known limitation:</strong> this makes collisions very unlikely,
 * not impossible. A PID is unique only within a namespace, so two containers
 * on the same host with the same hostname and the same PID would still
 * collide. Nothing here detects that; it cannot be detected from inside one
 * JVM. Deployments that cannot guarantee distinct hostnames should set
 * {@code intake.instance-id} explicitly to something they know is unique.
 */
@Component
public class InstanceId {

    private static final Logger log = LoggerFactory.getLogger(InstanceId.class);

    /** Characters safe in an HDFS path component and a filename. */
    private static final String UNSAFE_CHARACTERS = "[^A-Za-z0-9._-]";

    private final String value;
    private final boolean generated;

    @Autowired
    public InstanceId(IntakeProperties properties) {
        this(properties == null ? null : properties.getInstanceId(),
                resolveHostname(),
                ProcessHandle.current().pid());
    }

    /**
     * Visible for testing: hostname and pid cannot be varied from inside a
     * running JVM.
     */
    InstanceId(String configured, String hostname, long pid) {
        if (configured != null && !configured.isBlank()) {
            this.value = sanitize(configured.trim());
            this.generated = false;
            log.info("Instance id (configured): {}", value);
        } else {
            this.value = sanitize(hostname + "-" + pid);
            this.generated = true;
            log.info("Instance id (generated from hostname and pid): {}", value);
        }

        // Must contain something that actually identifies a process. Blank is
        // the obvious case, but sanitising replaces unsafe characters rather
        // than dropping them, so an id like "///" arrives here as "---" — not
        // blank, and equally useless as an identity.
        if (value.isBlank() || !value.matches(".*[A-Za-z0-9].*")) {
            throw new IllegalStateException(
                    "Resolved instance id '" + value + "' contains no alphanumeric characters. "
                            + "It names this process's _tmp directory and appears in every landed "
                            + "filename, so it must identify this process. Set intake.instance-id "
                            + "explicitly.");
        }
    }

    /**
     * An instance id with an explicit value — for tests and for callers that
     * have already decided the identity.
     */
    public static InstanceId of(String value) {
        return new InstanceId(value, "unused", 0L);
    }

    /**
     * Replaces anything that would be awkward in a path or filename. A
     * hostname is normally safe, but an explicitly configured id might not be,
     * and a bad character would only surface as an HDFS error at first write.
     */
    private static String sanitize(String raw) {
        String cleaned = raw.replaceAll(UNSAFE_CHARACTERS, "-");
        if (!cleaned.equals(raw)) {
            log.warn("Instance id '{}' contained characters unsafe for HDFS paths — using '{}'",
                    raw, cleaned);
        }
        return cleaned;
    }

    private static String resolveHostname() {
        String fromEnv = System.getenv("HOSTNAME");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim().toLowerCase(Locale.ROOT);
        }
        try {
            return InetAddress.getLocalHost().getHostName().toLowerCase(Locale.ROOT);
        } catch (Exception e) {
            // Better a stable placeholder plus the pid than a failed startup:
            // the pid still distinguishes processes on this host.
            log.warn("Could not resolve hostname ({}), using 'unknown' — the pid still "
                    + "distinguishes this process", e.getMessage());
            return "unknown";
        }
    }

    /** The resolved id. Constant for the life of the process. */
    public String value() {
        return value;
    }

    /** True when the id was derived rather than explicitly configured. */
    public boolean isGenerated() {
        return generated;
    }

    @Override
    public String toString() {
        return value;
    }
}
