package com.hcsc.datalake.mqintake.core.failuremode;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BatchWriter with fault injection hooks for failure-mode testing.
 *
 * <p>This is not a mock — it performs real HDFS operations and injects
 * faults at actual transaction boundaries to verify delivery guarantees.
 *
 * <p>Tracks written files and message IDs for post-crash verification.
 */
public class FaultableBatchWriter implements BatchWriter {

    private static final Logger log = LoggerFactory.getLogger(FaultableBatchWriter.class);

    private final FileSystem fileSystem;
    private final Configuration conf;
    private final RecordSerializer serializer;
    private final String instanceId;
    private final String basePath;
    private final Clock clock;
    private final FaultInjector faultInjector;

    private final AtomicLong batchSequence = new AtomicLong(0);

    private final Set<String> filesInTmp = ConcurrentHashMap.newKeySet();
    private final Set<String> filesRenamed = ConcurrentHashMap.newKeySet();
    private final List<String> messageIdsWritten = new ArrayList<>();

    public FaultableBatchWriter(FileSystem fileSystem,
                                 Configuration conf,
                                 RecordSerializer serializer,
                                 String instanceId,
                                 String basePath,
                                 Clock clock,
                                 FaultInjector faultInjector) {
        this.fileSystem = fileSystem;
        this.conf = conf;
        this.serializer = serializer;
        this.instanceId = instanceId;
        this.basePath = basePath;
        this.clock = clock;
        this.faultInjector = faultInjector;
    }

    @Override
    public BatchWriteResult write(String bindingId, List<Message> messages,
                                 Instant partitionInstant) throws BatchWriteException {
        if (messages.isEmpty()) {
            throw new BatchWriteException("Cannot write empty batch");
        }

        try {
            faultInjector.beforeBatchProcess();
        } catch (FaultInjector.FaultException e) {
            throw new BatchWriteException("Fault injected before batch process: " + e.getMessage(), e);
        }

        Instant now = Instant.now(clock);
        // Mirrors SequenceFileBatchWriter: partition from the caller's anchor,
        // filename from the write instant.
        String partitionPath = computePartitionPath(basePath,
                partitionInstant != null ? partitionInstant : now);

        long batchSeq = batchSequence.incrementAndGet();
        String filename = String.format("%s_%s_%d_%d.seq",
                bindingId, instanceId, now.toEpochMilli(), batchSeq);

        String tempDir = basePath + "/_tmp/" + instanceId;
        Path tempPath = new Path(tempDir, filename);
        Path finalPath = new Path(partitionPath, filename);

        log.debug("Writing batch: {} messages to temp={}, final={}",
                messages.size(), tempPath, finalPath);

        long byteCount;
        try {
            fileSystem.mkdirs(tempPath.getParent());

            byteCount = writeSequenceFileWithFaults(tempPath, bindingId, messages, filename);

            filesInTmp.add(tempPath.toString());

            try {
                faultInjector.afterHdfsClose();
            } catch (FaultInjector.FaultException e) {
                throw new BatchWriteException("Fault injected after HDFS close: " + e.getMessage(), e);
            }

            fileSystem.mkdirs(finalPath.getParent());

            boolean renamed = fileSystem.rename(tempPath, finalPath);
            if (!renamed) {
                throw new BatchWriteException(
                        "Failed to rename temp file to partition: " + tempPath + " -> " + finalPath);
            }

            filesInTmp.remove(tempPath.toString());
            filesRenamed.add(finalPath.toString());

            try {
                faultInjector.afterHdfsRename();
            } catch (FaultInjector.FaultException e) {
                throw new BatchWriteException("Fault injected after HDFS rename: " + e.getMessage(), e);
            }

            for (Message msg : messages) {
                try {
                    String msgId = msg.getJMSMessageID();
                    if (msgId != null) {
                        synchronized (messageIdsWritten) {
                            messageIdsWritten.add(msgId);
                        }
                    }
                } catch (JMSException ignored) {}
            }

            log.debug("Batch written successfully: {} records, {} bytes to {}",
                    messages.size(), byteCount, finalPath);

            return new BatchWriteResult(finalPath.toString(), messages.size(), byteCount);

        } catch (IOException e) {
            deleteQuietly(tempPath);
            throw new BatchWriteException("Failed to write batch to HDFS: " + e.getMessage(), e);
        } catch (RecordSerializer.SerializationException e) {
            deleteQuietly(tempPath);
            throw new BatchWriteException("Failed to serialize message: " + e.getMessage(), e);
        }
    }

    private long writeSequenceFileWithFaults(Path path, String bindingId,
                                              List<Message> messages, String filename)
            throws IOException, RecordSerializer.SerializationException, BatchWriteException {

        long startPos = 0;
        long endPos = 0;

        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(serializer.getKeyClass()),
                SequenceFile.Writer.valueClass(serializer.getValueClass()),
                SequenceFile.Writer.compression(CompressionType.RECORD, new DefaultCodec()))) {

            startPos = writer.getLength();

            for (int i = 0; i < messages.size(); i++) {
                if (i == messages.size() / 2) {
                    try {
                        faultInjector.duringHdfsWrite();
                    } catch (FaultInjector.FaultException e) {
                        throw new BatchWriteException(
                                "Fault injected during HDFS write: " + e.getMessage(), e);
                    }
                }

                Message message = messages.get(i);
                RecordMetadata metadata = buildMetadata(bindingId, message, filename, i);
                RecordSerializer.SerializedRecord record = serializer.serialize(message, metadata);
                writer.append(record.getKey(), record.getValue());
            }

            writer.hflush();
            endPos = writer.getLength();
        }

        return endPos - startPos;
    }

    private RecordMetadata buildMetadata(String bindingId, Message message, String filename, int offset) {
        RecordMetadata.Builder builder = RecordMetadata.builder()
                .bindingId(bindingId)
                .sourceFile(filename)
                .recordOffset(offset)
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

    private String computePartitionPath(String basePath, Instant time) {
        java.time.ZonedDateTime zdt = time.atZone(java.time.ZoneOffset.UTC);
        int year = zdt.getYear();
        int month = zdt.getMonthValue();
        int day = zdt.getDayOfMonth();
        int hour = zdt.getHour();
        int quarter = zdt.getMinute() / 15;
        return String.format("%s/%04d/%02d/%02d/%02d/%d", basePath, year, month, day, hour, quarter);
    }

    private void deleteQuietly(Path path) {
        try {
            fileSystem.delete(path, false);
        } catch (IOException e) {
            log.debug("Failed to delete temp file {}: {}", path, e.getMessage());
        }
    }

    public Set<String> getFilesInTmp() {
        return Set.copyOf(filesInTmp);
    }

    public Set<String> getFilesRenamed() {
        return Set.copyOf(filesRenamed);
    }

    public List<String> getMessageIdsWritten() {
        synchronized (messageIdsWritten) {
            return List.copyOf(messageIdsWritten);
        }
    }

    public void clearTracking() {
        filesInTmp.clear();
        filesRenamed.clear();
        synchronized (messageIdsWritten) {
            messageIdsWritten.clear();
        }
    }
}
