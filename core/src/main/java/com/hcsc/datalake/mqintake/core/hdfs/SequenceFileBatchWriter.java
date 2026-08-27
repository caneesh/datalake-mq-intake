package com.hcsc.datalake.mqintake.core.hdfs;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import java.util.ArrayList;
import com.hcsc.datalake.mqintake.core.index.RecordIndex;
import com.hcsc.datalake.mqintake.core.index.RecordIndexEntry;
import com.hcsc.datalake.mqintake.core.index.RecordIndexWriter;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes batches of messages to HDFS SequenceFiles.
 *
 * <p>Key invariants from DESIGN.md §7 and §8.1:
 * <ul>
 *   <li>Partition path computed fresh at every flush — NEVER cached</li>
 *   <li>Files written to _tmp/{instance_id}/, then atomically renamed into partition</li>
 *   <li>SequenceFile with RECORD or NONE compression — NEVER BLOCK</li>
 *   <li>hsync (durability barrier — see below), close, then rename (visibility barrier)</li>
 * </ul>
 */
public class SequenceFileBatchWriter implements BatchWriter {

    private static final Logger log = LoggerFactory.getLogger(SequenceFileBatchWriter.class);

    /**
     * True: hsync before close, forcing every DataNode to fsync the block to
     * disk. False: hflush only, which pushes bytes to the replica pipeline but
     * can leave them in OS page cache — close() does not fsync either unless
     * the cluster sets dfs.datanode.synconclose. The distinction is the
     * difference between "durable against process crash" and "durable against
     * correlated power loss after the MQ commit has acknowledged the messages".
     */
    private final boolean hsyncOnFlush;

    private final FileSystem fileSystem;
    private final Configuration conf;
    private final RecordSerializer serializer;
    private final String instanceId;
    private final Clock clock;
    private final CompressionType compressionType;
    private final Map<String, String> bindingBasePaths;
    private final RecordIndexWriter recordIndexWriter;

    private final AtomicLong batchSequence = new AtomicLong(0);

    /**
     * Creates a writer with system clock and a single binding path.
     */
    public SequenceFileBatchWriter(FileSystem fileSystem,
                                    Configuration conf,
                                    RecordSerializer serializer,
                                    String instanceId,
                                    String bindingId,
                                    String basePath) {
        this(fileSystem, conf, serializer, instanceId, Clock.systemUTC(),
                CompressionType.RECORD, Map.of(bindingId, basePath));
    }

    /**
     * Creates a writer with full configuration.
     */
    public SequenceFileBatchWriter(FileSystem fileSystem,
                                    Configuration conf,
                                    RecordSerializer serializer,
                                    String instanceId,
                                    Clock clock,
                                    CompressionType compressionType,
                                    Map<String, String> bindingBasePaths) {
        this(fileSystem, conf, serializer, instanceId, clock, compressionType,
                bindingBasePaths, RecordIndexWriter.disabled());
    }

    /**
     * Creates a writer that also emits a sidecar index.
     *
     * <p>Index writing is off unless a writer is supplied: a binding with no
     * trustworthy per-message identity should not produce an index that
     * reconciliation might believe.
     */
    public SequenceFileBatchWriter(FileSystem fileSystem,
                                    Configuration conf,
                                    RecordSerializer serializer,
                                    String instanceId,
                                    Clock clock,
                                    CompressionType compressionType,
                                    Map<String, String> bindingBasePaths,
                                    RecordIndexWriter recordIndexWriter) {
        this(fileSystem, conf, serializer, instanceId, clock, compressionType,
                bindingBasePaths, recordIndexWriter, true);
    }

    /**
     * Full constructor including the durability mode; see {@link #hsyncOnFlush}.
     */
    public SequenceFileBatchWriter(FileSystem fileSystem,
                                    Configuration conf,
                                    RecordSerializer serializer,
                                    String instanceId,
                                    Clock clock,
                                    CompressionType compressionType,
                                    Map<String, String> bindingBasePaths,
                                    RecordIndexWriter recordIndexWriter,
                                    boolean hsyncOnFlush) {
        this.hsyncOnFlush = hsyncOnFlush;
        this.recordIndexWriter = recordIndexWriter == null
                ? RecordIndexWriter.disabled() : recordIndexWriter;
        this.fileSystem = fileSystem;
        this.conf = conf;
        this.serializer = serializer;
        this.instanceId = instanceId;
        this.clock = clock;
        this.compressionType = compressionType;
        this.bindingBasePaths = new ConcurrentHashMap<>(bindingBasePaths);

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

        String basePath = bindingBasePaths.get(bindingId);
        if (basePath == null) {
            throw new BatchWriteException("No base path configured for binding: " + bindingId);
        }

        Instant now = Instant.now(clock);
        String partitionPath = PartitionPath.compute(basePath, now);

        long batchSeq = batchSequence.incrementAndGet();
        String filename = PartitionPath.filename(bindingId, instanceId, now.toEpochMilli(), batchSeq);

        String tempDir = PartitionPath.tempDir(basePath, instanceId);
        Path tempPath = new Path(tempDir, filename);
        Path finalPath = new Path(partitionPath, filename);

        log.debug("Writing batch: {} messages to temp={}, final={}",
                messages.size(), tempPath, finalPath);

        long byteCount = 0;
        // Cleared only once the file is renamed into its partition. Every exit
        // that leaves the temp file behind — including rename() returning false
        // and any RuntimeException out of a binding's serializer, neither of
        // which the catch clauses below see — must delete it in the finally.
        boolean landed = false;
        try {
            fileSystem.mkdirs(tempPath.getParent());

            List<RecordIndexEntry> indexEntries = new ArrayList<>(messages.size());
            byteCount = writeSequenceFile(tempPath, bindingId, messages, filename, indexEntries);

            fileSystem.mkdirs(finalPath.getParent());

            boolean renamed = fileSystem.rename(tempPath, finalPath);
            if (!renamed) {
                throw new BatchWriteException(
                        "Failed to rename temp file to partition: " + tempPath + " -> " + finalPath);
            }

            landed = true;

            writeIndexQuietly(new RecordIndex(bindingId, filename, partitionPath,
                    instanceId, indexEntries));

            log.debug("Batch written successfully: {} records, {} bytes to {}",
                    messages.size(), byteCount, finalPath);

            return new BatchWriteResult(finalPath.toString(), messages.size(), byteCount);

        } catch (IOException e) {
            throw new BatchWriteException("Failed to write batch to HDFS: " + e.getMessage(), e);
        } catch (RecordSerializer.SerializationException e) {
            throw new BatchWriteException("Failed to serialize message: " + e.getMessage(), e);
        } finally {
            if (!landed) {
                deleteQuietly(tempPath);
            }
        }
    }

    private long writeSequenceFile(Path path, String bindingId, List<Message> messages,
                                   String filename, List<RecordIndexEntry> indexEntries)
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

                // Byte position where this record will begin. Read immediately
                // before the append, matching the legacy writer's
                // `long offset = sequenceFileWriter.getLength()`. Only this
                // class can supply it — the serializer has no view of the file.
                long fileByteOffset = writer.getLength();

                RecordMetadata metadata =
                        buildMetadata(bindingId, message, filename, i, fileByteOffset);
                RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);
                writer.append(record.getKey(), record.getValue());

                // Identity travels beside the record, never inside it — the
                // file contract stays byte-comparable with the legacy MDB.
                indexEntries.add(new RecordIndexEntry(fileByteOffset, record.getIdentity()));
            }

            // hsync forces each DataNode to fsync its block file; hflush only
            // reaches the pipeline. Once the caller commits to MQ the messages
            // are unrecoverable from the queue, so with hflush alone a
            // correlated power loss across the replica set could lose
            // acknowledged data. One sync per batch, not per record — the
            // legacy MDB paid this per message.
            if (hsyncOnFlush) {
                writer.hsync();
            } else {
                writer.hflush();
            }
            endPos = writer.getLength();
        }

        return endPos - startPos;
    }

    /**
     * Writes the sidecar index, never failing the batch.
     *
     * <p>The data file is already durable and visible at this point. Refusing
     * to commit because a reconciliation aid could not be written would roll
     * back a landed file and manufacture a duplicate — a worse outcome than a
     * file with no index, which simply leaves reconciliation where it is today.
     */
    private void writeIndexQuietly(RecordIndex index) {
        if (!recordIndexWriter.isEnabled()) {
            return;
        }
        try {
            recordIndexWriter.write(index);
        } catch (Exception e) {
            log.warn("Could not write record index for {} — the data is landed and committed, "
                            + "only reconciliation metadata is missing: {}",
                    index.getFilename(), e.getMessage());
        }
    }

    private RecordMetadata buildMetadata(String bindingId, Message message, String filename,
                                          int offset, long fileByteOffset) {
        RecordMetadata.Builder builder = RecordMetadata.builder()
                .bindingId(bindingId)
                .sourceFile(filename)
                .recordOffset(offset)
                .fileByteOffset(fileByteOffset)
                .consumeTimestamp(Instant.now(clock));

        try {
            builder.mqMessageId(message.getJMSMessageID());
            long jmsTimestamp = message.getJMSTimestamp();
            if (jmsTimestamp > 0) {
                builder.mqPutTimestamp(Instant.ofEpochMilli(jmsTimestamp));
            }
        } catch (JMSException e) {
            log.warn("Failed to extract JMS metadata: {}", e.getMessage());
        }

        return builder.build();
    }

    private void deleteQuietly(Path path) {
        try {
            fileSystem.delete(path, false);
        } catch (IOException e) {
            log.debug("Failed to delete temp file {}: {}", path, e.getMessage());
        }
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getBasePath(String bindingId) {
        return bindingBasePaths.get(bindingId);
    }
}
