package com.hcsc.datalake.mqintake.core.index;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Reads a sidecar index back.
 *
 * <p>Reconciliation compares the set of identities a file claims to contain
 * against the audit trail. Before the sidecar existed it tried to recover
 * identity from the SequenceFile key, which under the production contract is a
 * byte offset carrying no identity at all — so it found nothing and could
 * conclude nothing.
 *
 * <p>Parsing is deliberately hand-rolled and strict. The format is three
 * numeric-or-string fields per line; pulling in a JSON library to read it
 * would be a dependency for the sake of one file shape, and a lenient parser
 * would let a truncated index look like a short one — which reconciliation
 * would report as missing records.
 */
public class RecordIndexReader {

    private static final Logger log = LoggerFactory.getLogger(RecordIndexReader.class);

    private final FileSystem fileSystem;

    public RecordIndexReader(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
    }

    /** The index path for a data file, by convention. */
    public static Path indexPathFor(Path dataFile) {
        return new Path(dataFile.getParent(),
                dataFile.getName() + HdfsRecordIndexWriter.INDEX_SUFFIX);
    }

    public boolean hasIndex(Path dataFile) throws IOException {
        return fileSystem.exists(indexPathFor(dataFile));
    }

    /**
     * Reads the index for a data file.
     *
     * @return the index, or empty when the file has none — which is the
     *         expected state for anything landed before indexing was enabled,
     *         and for a crash between the data rename and the index rename
     */
    public Optional<RecordIndex> read(Path dataFile) throws IOException {
        Path indexPath = indexPathFor(dataFile);
        if (!fileSystem.exists(indexPath)) {
            return Optional.empty();
        }

        try (FSDataInputStream in = fileSystem.open(indexPath);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(in, StandardCharsets.UTF_8))) {

            String header = reader.readLine();
            if (header == null) {
                log.warn("Record index {} is empty", indexPath);
                return Optional.empty();
            }

            int schema = (int) longField(header, "schema", -1);
            if (schema != HdfsRecordIndexWriter.SCHEMA_VERSION) {
                log.warn("Record index {} has unsupported schema {} (expected {}) — ignoring",
                        indexPath, schema, HdfsRecordIndexWriter.SCHEMA_VERSION);
                return Optional.empty();
            }

            String binding = stringField(header, "binding");
            String file = stringField(header, "file");
            String partition = stringField(header, "partition");
            String instance = stringField(header, "instance");
            long declaredCount = longField(header, "records", -1);

            List<RecordIndexEntry> entries = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                // A truncated write leaves a partial final line. It would
                // otherwise parse into an entry with a plausible offset and an
                // empty identity, which reconciliation would read as a record
                // that lost its identity rather than as a damaged index.
                if (!isCompleteLine(line)) {
                    log.warn("Record index {} ends with an incomplete line — ignoring the "
                            + "whole index rather than trusting a partial record", indexPath);
                    return Optional.empty();
                }
                entries.add(new RecordIndexEntry(
                        longField(line, "offset", -1), stringField(line, "identity")));
            }

            // A short index means the writer was interrupted. Treating it as
            // complete would make reconciliation report the missing records as
            // losses, which is a worse answer than "no index".
            if (declaredCount >= 0 && entries.size() != declaredCount) {
                log.warn("Record index {} is truncated: header declares {} records, found {} "
                                + "— ignoring rather than reporting the difference as loss",
                        indexPath, declaredCount, entries.size());
                return Optional.empty();
            }

            return Optional.of(new RecordIndex(
                    binding == null ? "" : binding,
                    file == null ? dataFile.getName() : file,
                    partition == null ? dataFile.getParent().toString() : partition,
                    instance,
                    entries));
        }
    }

    /** The identities a file claims to contain, for identity-set comparison. */
    public Set<String> readIdentities(Path dataFile) throws IOException {
        Optional<RecordIndex> index = read(dataFile);
        if (index.isEmpty()) {
            return Set.of();
        }
        Set<String> identities = new LinkedHashSet<>();
        for (RecordIndexEntry entry : index.get().getEntries()) {
            if (entry.getIdentity() != null && !entry.getIdentity().isEmpty()) {
                identities.add(entry.getIdentity());
            }
        }
        return identities;
    }

    /** A whole JSON object on one line, as the writer emits. */
    private static boolean isCompleteLine(String line) {
        String trimmed = line.trim();
        return trimmed.startsWith("{") && trimmed.endsWith("}");
    }

    // --- minimal field extraction ---

    static String stringField(String line, String field) {
        String marker = "\"" + field + "\":";
        int start = line.indexOf(marker);
        if (start < 0) {
            return null;
        }
        int valueStart = start + marker.length();
        if (line.startsWith("null", valueStart)) {
            return null;
        }
        if (line.charAt(valueStart) != '"') {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = valueStart + 1; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\\' && i + 1 < line.length()) {
                char next = line.charAt(++i);
                switch (next) {
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    default:  sb.append(next);
                }
            } else if (c == '"') {
                return sb.toString();
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    static long longField(String line, String field, long fallback) {
        String marker = "\"" + field + "\":";
        int start = line.indexOf(marker);
        if (start < 0) {
            return fallback;
        }
        int i = start + marker.length();
        int end = i;
        while (end < line.length()
                && (Character.isDigit(line.charAt(end)) || line.charAt(end) == '-')) {
            end++;
        }
        if (end == i) {
            return fallback;
        }
        try {
            return Long.parseLong(line.substring(i, end));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
