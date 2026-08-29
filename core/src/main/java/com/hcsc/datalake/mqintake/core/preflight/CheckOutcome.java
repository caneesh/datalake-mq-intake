package com.hcsc.datalake.mqintake.core.preflight;

import java.time.Duration;
import java.util.Objects;

/** The result of one {@link PreflightCheck}. */
public final class CheckOutcome {

    public enum Status {
        /** The dependency behaved as the application will require it to. */
        PASS,
        /** Proven broken. The application would not work against it. */
        FAIL,
        /** Not applicable to this configuration (e.g. tracker on a LAND_ONLY binding). */
        SKIP
    }

    private final Status status;
    private final String detail;
    private final String remedy;
    private Duration took = Duration.ZERO;

    private CheckOutcome(Status status, String detail, String remedy) {
        this.status = Objects.requireNonNull(status);
        this.detail = detail == null ? "" : detail;
        this.remedy = remedy;
    }

    public static CheckOutcome pass(String detail) {
        return new CheckOutcome(Status.PASS, detail, null);
    }

    public static CheckOutcome skip(String detail) {
        return new CheckOutcome(Status.SKIP, detail, null);
    }

    public static CheckOutcome fail(String detail) {
        return new CheckOutcome(Status.FAIL, detail, null);
    }

    /**
     * @param remedy what to do about it — the difference between a diagnostic
     *               and a complaint. Shown under the failure in the report.
     */
    public static CheckOutcome fail(String detail, String remedy) {
        return new CheckOutcome(Status.FAIL, detail, remedy);
    }

    /** Renders a throwable's chain, which is usually where the real cause is. */
    public static CheckOutcome fail(String detail, Throwable cause, String remedy) {
        StringBuilder sb = new StringBuilder(detail);
        java.util.Set<Throwable> seen =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (Throwable t = cause; t != null && seen.add(t); t = t.getCause()) {
            sb.append(" | ").append(t.getClass().getSimpleName());
            if (t.getMessage() != null) {
                sb.append(": ").append(t.getMessage().replace('\n', ' ').trim());
            }
        }
        return new CheckOutcome(Status.FAIL, sb.toString(), remedy);
    }

    CheckOutcome withDuration(Duration took) {
        this.took = took;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public String getDetail() {
        return detail;
    }

    public String getRemedy() {
        return remedy;
    }

    public Duration getTook() {
        return took;
    }

    public boolean isFailure() {
        return status == Status.FAIL;
    }
}
