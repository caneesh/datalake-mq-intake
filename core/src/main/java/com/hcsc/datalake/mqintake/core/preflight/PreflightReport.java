package com.hcsc.datalake.mqintake.core.preflight;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The outcome of a preflight run, rendered for a terminal.
 *
 * <p>Written to be read under pressure: one line per check, failures repeated
 * at the bottom with their remedy, and a verdict an operator can act on
 * without reading the detail lines.
 */
public final class PreflightReport {

    /** One check and what it produced. */
    public static final class Entry {
        private final PreflightCheck check;
        private final CheckOutcome outcome;

        Entry(PreflightCheck check, CheckOutcome outcome) {
            this.check = check;
            this.outcome = outcome;
        }

        public PreflightCheck getCheck() {
            return check;
        }

        public CheckOutcome getOutcome() {
            return outcome;
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    void add(PreflightCheck check, CheckOutcome outcome) {
        entries.add(new Entry(check, outcome));
    }

    public List<Entry> getEntries() {
        return List.copyOf(entries);
    }

    public long count(CheckOutcome.Status status) {
        return entries.stream().filter(e -> e.outcome.getStatus() == status).count();
    }

    public boolean hasFailures() {
        return entries.stream().anyMatch(e -> e.outcome.isFailure());
    }

    public String render() {
        StringBuilder sb = new StringBuilder();
        sb.append('\n').append(rule()).append('\n')
                .append("  PREFLIGHT — component checks against the live environment\n")
                .append(rule()).append('\n');

        Map<String, List<Entry>> byGroup = new LinkedHashMap<>();
        for (Entry entry : entries) {
            byGroup.computeIfAbsent(entry.check.group(), g -> new ArrayList<>()).add(entry);
        }

        int width = entries.stream().mapToInt(e -> e.check.name().length()).max().orElse(20);
        for (Map.Entry<String, List<Entry>> group : byGroup.entrySet()) {
            sb.append('\n').append("  ").append(group.getKey().toUpperCase()).append('\n');
            for (Entry entry : group.getValue()) {
                CheckOutcome outcome = entry.getOutcome();
                sb.append(String.format("    %-6s %-" + width + "s  %s%n",
                        badge(outcome.getStatus()),
                        entry.check.name(),
                        outcome.getDetail()));
            }
        }

        if (hasFailures()) {
            sb.append('\n').append(rule()).append('\n')
                    .append("  FAILURES — what to do\n").append(rule()).append('\n');
            for (Entry entry : entries) {
                if (!entry.getOutcome().isFailure()) {
                    continue;
                }
                sb.append("\n  ").append(entry.check.name()).append('\n')
                        .append("    proves : ").append(entry.check.describes()).append('\n')
                        .append("    error  : ").append(entry.getOutcome().getDetail()).append('\n');
                if (entry.getOutcome().getRemedy() != null) {
                    sb.append("    fix    : ").append(entry.getOutcome().getRemedy()).append('\n');
                }
            }
        }

        Duration total = entries.stream()
                .map(e -> e.getOutcome().getTook())
                .reduce(Duration.ZERO, Duration::plus);

        sb.append('\n').append(rule()).append('\n')
                .append(String.format("  %s — %d passed, %d failed, %d skipped in %d ms%n",
                        hasFailures() ? "PREFLIGHT FAILED" : "PREFLIGHT PASSED",
                        count(CheckOutcome.Status.PASS),
                        count(CheckOutcome.Status.FAIL),
                        count(CheckOutcome.Status.SKIP),
                        total.toMillis()))
                .append(rule()).append('\n');
        return sb.toString();
    }

    private String badge(CheckOutcome.Status status) {
        switch (status) {
            case PASS: return "[ ok ]";
            case FAIL: return "[FAIL]";
            default:   return "[skip]";
        }
    }

    private String rule() {
        return "  " + "-".repeat(76);
    }
}
