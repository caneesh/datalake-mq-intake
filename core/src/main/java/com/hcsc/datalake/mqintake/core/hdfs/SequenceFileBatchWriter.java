package com.hcsc.datalake.mqintake.core.hdfs;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes batches of messages to HDFS SequenceFiles.
 *
 * <p>Key invariants from DESIGN.md §7 and §8.1:
 * <ul>
 *   <li>Partition path computed fresh at every flush — NEVER cached</li>
 *   <li>Files written to _tmp/{instance_id}/, then atomically renamed into partition</li>
 *   <li>SequenceFile with RECORD or NONE compression — NEVER BLOCK</li>
 *   <li>Close file (durability barrier), then rename (visibility barrier)</li>
 * </ul>
 */
public class SequenceFileBatchWriter implements BatchWriter {

    private static final Logger log = LoggerFactory.getLogger(SequenceFileBatchWriter.class);

    private final FileSystem fileSystem;
    private final Configuration conf;
    private final RecordSerializer serializer;
    private final String instanceId;
    private final Clock clock;
    private final CompressionType compressionType;

    private final AtomicLong batchSequence = new AtomicLong(0);

    /**
     * Creates a writer with system clock.
     */
    public SequenceFileBatchWriter(FileSystem fileSystem,
                                    Configuration conf,
                                    RecordSerializer serializer,
                                    String instanceId) {
        this(fileSystem, conf, serializer, instanceId, Clock.systemUTC(), CompressionType.RECORD);
    }

    /**
     * Creates a writer with injectable clock (for testing).
     */
    public SequenceFileBatchWriter(FileSystem fileSystem,
                                    Configuration conf,
                                    RecordSerializer serializer,
                                    String instanceId,
                                    Clock clock,
                                    CompressionType compressionType) {
        this.fileSystem = fileSystem;
        this.conf = conf;
        this.serializer = serializer;
        this.instanceId = instanceId;
        this.clock = clock;
        this.compressionType = compressionType;

        // Validate: BLOCK compression is forbidden per §8
        if (compressionType == CompressionType.BLOCK) {
            throw new IllegalArgumentException(
                    "BLOCK compression is forbidden — it triggers mid-stream sync behavior " +
                    "that interacts badly with erasure-coded files. Use RECORD or NONE.");
        }
    }

    @Override
    public BatchWriteResult write(String bindingId, List<Message> messages) throws BatchWriteException {
        if (messages.isEmpty()) {
            throw new BatchWriteException("Cannot write empty batch");
        }

        // CRITICAL: Compute partition path fresh at flush time — NEVER cache
        Instant now = Instant.now(clock);
        String basePath = getBasePath(bindingId);
        String partitionPath = PartitionPath.compute(basePath, now);

        // Generate unique filename
        long batchSeq = batchSequence.incrementAndGet();
        String filename = PartitionPath.filename(bindingId, instanceId, now.toEpochMilli(), batchSeq);

        // Temp path: {base}/_tmp/{instance_id}/{filename}
        String tempDir = PartitionPath.tempDir(basePath, instanceId);
        Path tempPath = new Path(tempDir, filename);

        // Final path: {partition}/{filename}
        Path finalPath = new Path(partitionPath, filename);

        log.debug("Writing batch: {} messages to temp={}, final={}",
                messages.size(), tempPath, finalPath);

        long byteCount = 0;
        try {
            // Ensure temp directory exists
            fileSystem.mkdirs(tempPath.getParent());

            // Write to temp file
            byteCount = writeSequenceFile(tempPath, bindingId, messages, filename);

            // Ensure partition directory exists
            fileSystem.mkdirs(finalPath.getParent());

            // ATOMIC RENAME: visibility barrier
            boolean renamed = fileSystem.rename(tempPath, finalPath);
            if (!renamed) {
                throw new BatchWriteException(
                        "Failed to rename temp file to partition: " + tempPath + " -> " + finalPath);
            }

            log.debug("Batch written successfully: {} records, {} bytes to {}",
                    messages.size(), byteCount, finalPath);

            return new BatchWriteResult(finalPath.toString(), messages.size(), byteCount);

        } catch (IOException e) {
            // Clean up temp file on failure
            deleteQuietly(tempPath);
            throw new BatchWriteException("Failed to write batch to HDFS: " + e.getMessage(), e);
        } catch (RecordSerializer.SerializationException e) {
            deleteQuietly(tempPath);
            throw new BatchWriteException("Failed to serialize message: " + e.getMessage(), e);
        }
    }

    /**
     * Writes messages to a SequenceFile at the given path.
     *
     * @return total bytes written
     */
    private long writeSequenceFile(Path path, String bindingId, List<Message> messages, String filename)
            throws IOException, RecordSerializer.SerializationException {

        long startPos = 0;
        long endPos = 0;

        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(serializer.getKeyClass()),
                SequenceFile.Writer.valueClass(serializer.getValueClass()),
                SequenceFile.Writer.compression(compressionType, new DefaultCodec()))) {

            startPos = writer.getLength();

            for (int i = 0; i < messages.size(); i++) {
                Message message = messages.get(i);
                RecordMetadata metadata = buildMetadata(bindingId, message, filename, i);
                RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);
                writer.append(record.getKey(), record.getValue());
            }

            // Sync ensures data is flushed to DataNodes
            writer.hflush();
            endPos = writer.getLength();
        }
        // Close happens via try-with-resources — this is the DURABILITY BARRIER

        return endPos - startPos;
    }

    /**
     * Builds metadata for a single message.
     */
    private RecordMetadata buildMetadata(String bindingId, Message message, String filename, int offset) {
        RecordMetadata.Builder builder = RecordMetadata.builder()
                .bindingId(bindingId)
                .sourceFile(filename)
                .recordOffset(offset)
                .consumeTimestamp(Instant.now(clock));

        try {
            builder.mqMessageId(message.getJMSMessageID());
            // JMS timestamp is in millis
            long jmsTimestamp = message.getJMSTimestamp();
            if (jmsTimestamp > 0) {
                builder.mqPutTimestamp(Instant.ofEpochMilli(jmsTimestamp));
            }
        } catch (JMSException e) {
            log.warn("Failed to extract JMS metadata: {}", e.getMessage());
        }

        return builder.build();
    }

    /**
     * Gets the base path for a binding. In real implementation, this would
     * come from binding config. For now, uses a simple convention.
     */
    protected String getBasePath(String bindingId) {
        // Override this in actual implementation to use BindingConfig
        return "/data/raw/" + bindingId;
    }

    /**
     * Deletes a file, ignoring errors.
     */
    private void deleteQuietly(Path path) {
        try {
            fileSystem.delete(path, false);
        } catch (IOException e) {
            log.debug("Failed to delete temp file {}: {}", path, e.getMessage());
        }
    }

    /**
     * Cleans up stale temp files for THIS instance only.
     * Called on startup to remove crash debris.
     *
     * @param basePath the HDFS base path
     * @param maxAgeMs maximum age in milliseconds — files older than this are deleted
     * @return number of files deleted
     */
    public int cleanupTempFiles(String basePath, long maxAgeMs) throws IOException {
        String tempDir = PartitionPath.tempDir(basePath, instanceId);
        Path tempPath = new Path(tempDir);

        if (!fileSystem.exists(tempPath)) {
            return 0;
        }

        int deleted = 0;
        long cutoff = clock.millis() - maxAgeMs;

        for (var status : fileSystem.listStatus(tempPath)) {
            if (status.getModificationTime() < cutoff) {
                if (fileSystem.delete(status.getPath(), false)) {
                    log.info("Deleted stale temp file: {}", status.getPath());
                    deleted++;
                }
            }
        }

        return deleted;
    }

    public String getInstanceId() {
        return instanceId;
    }
}
