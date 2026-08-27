package com.hcsc.datalake.mqintake.core.index;

import com.hcsc.datalake.mqintake.core.hdfs.PartitionPath;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Writes the sidecar index beside its data file, via temp-and-rename.
 *
 * <p><strong>Format.</strong> JSON Lines. The first line describes the file,
 * every following line describes one record:
 *
 * <pre>
 * {"schema":1,"binding":"rms","file":"rms_h1_169_1.seq","partition":"...","records":2}
 * {"offset":129,"identity":"3f2b...-a1"}
 * {"offset":412,"identity":"9c7d...-b4"}
 * </pre>
 *
 * <p>Line-oriented so a reader can stream it without loading the whole index,
 * and so a truncated file is detectably truncated rather than silently
 * half-parsed. The payload is not repeated — it is already in the data file,
 * and copying it would double the storage cost of every feed.
 *
 * <p><strong>Consistency model.</strong> The index is written and renamed
 * <em>after</em> the data file is renamed into its partition and
 * <em>before</em> the MQ commit:
 *
 * <pre>
 * data _tmp write -&gt; close -&gt; data rename -&gt; index write -&gt; index rename -&gt; commit
 * </pre>
 *
 * <p>Two consequences follow, both deliberate:
 * <ul>
 *   <li><strong>An index never describes a file that does not exist.</strong>
 *       The data file is already visible when the index is written, so the
 *       orphan a crash can produce is data-without-index — which degrades
 *       reconciliation to what it does today — rather than index-without-data,
 *       which would look like a missing file and raise a false discrepancy.</li>
 *   <li><strong>An index can survive a rollback.</strong> If the commit fails
 *       after both renames, the messages are redelivered and land again in a
 *       new file with its own index. The first file and index still describe
 *       real, correct records; they are the design-permitted duplicate of
 *       §12.1, and reconciliation classifies them by comparing identity sets —
 *       which is precisely what the index makes possible.</li>
 * </ul>
 *
 * <p>An index write failure never fails the batch. The data is already durable
 * and visible; refusing to commit would roll back a landed file and manufacture
 * a duplicate in order to protect a reconciliation aid. The failure is logged
 * and the file simply has no index.
 */
public class HdfsRecordIndexWriter implements RecordIndexWriter {

    private static final Logger log = LoggerFactory.getLogger(HdfsRecordIndexWriter.class);

    /** Bumped if the line format ever changes; readers check it. */
    static final int SCHEMA_VERSION = 1;

    /** Appended to the data filename, so index and file are trivially paired. */
    public static final String INDEX_SUFFIX = ".index.jsonl";

    private final FileSystem fileSystem;
    private final String instanceId;

    public HdfsRecordIndexWriter(FileSystem fileSystem, String instanceId) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem required");
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId required");
    }

    @Override
    public void write(RecordIndex index) throws IOException {
        String indexFilename = index.getFilename() + INDEX_SUFFIX;
        Path finalPath = new Path(index.getPartitionPath(), indexFilename);

        // Same _tmp staging as the data file: a reader must never encounter a
        // partially written index in the partition.
        String tempDir = PartitionPath.tempDir(basePathOf(index), instanceId);
        Path tempPath = new Path(tempDir, indexFilename);

        boolean landed = false;
        try {
            fileSystem.mkdirs(tempPath.getParent());

            try (FSDataOutputStream out = fileSystem.create(tempPath, true)) {
                out.write(headerLine(index).getBytes(StandardCharsets.UTF_8));
                for (RecordIndexEntry entry : index.getEntries()) {
                    out.write(entryLine(entry).getBytes(StandardCharsets.UTF_8));
                }
                out.hflush();
            }

            fileSystem.mkdirs(finalPath.getParent());
            if (!fileSystem.rename(tempPath, finalPath)) {
                throw new IOException("Failed to rename index into partition: "
                        + tempPath + " -> " + finalPath);
            }
            landed = true;

            log.debug("Wrote record index for {}: {} records", index.getFilename(),
                    index.getRecordCount());

        } finally {
            if (!landed) {
                deleteQuietly(tempPath);
            }
        }
    }

    /**
     * The partition path minus its date components — the binding's base path.
     * The index stages under the same {@code _tmp/{instanceId}} tree the data
     * file used, so the startup sweep cleans both.
     */
    private String basePathOf(RecordIndex index) {
        String partition = index.getPartitionPath();
        // LAST occurrence: the partition component the writer appended is the
        // final /year= in the string, so an admin-configured base path that
        // itself contains "/year=" no longer truncates at the wrong point.
        int yearMarker = partition.lastIndexOf("/year=");
        return yearMarker > 0 ? partition.substring(0, yearMarker) : partition;
    }

    private String headerLine(RecordIndex index) {
        return "{\"schema\":" + SCHEMA_VERSION
                + ",\"binding\":" + jsonString(index.getBindingId())
                + ",\"file\":" + jsonString(index.getFilename())
                + ",\"partition\":" + jsonString(index.getPartitionPath())
                + ",\"instance\":" + jsonString(index.getInstanceId())
                + ",\"records\":" + index.getRecordCount()
                + "}\n";
    }

    private String entryLine(RecordIndexEntry entry) {
        return "{\"offset\":" + entry.getByteOffset()
                + ",\"identity\":" + jsonString(entry.getIdentity())
                + "}\n";
    }

    private String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.append('"').toString();
    }

    private void deleteQuietly(Path path) {
        try {
            fileSystem.delete(path, false);
        } catch (IOException e) {
            log.debug("Could not delete temp index {}: {}", path, e.getMessage());
        }
    }
}
