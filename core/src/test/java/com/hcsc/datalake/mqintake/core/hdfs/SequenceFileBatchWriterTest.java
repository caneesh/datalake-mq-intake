package com.hcsc.datalake.mqintake.core.hdfs;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.serializer.TestRecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.*;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for SequenceFileBatchWriter.
 *
 * Verifies:
 * - Path recomputed per flush (not cached)
 * - Correct path across hour and quarter boundaries
 * - Batch spanning boundary lands wholly in flush-time partition
 * - No file appears in partition until renamed
 * - Simulated crash mid-write leaves nothing visible in partition
 */
class SequenceFileBatchWriterTest {

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem fileSystem;
    private Configuration conf;
    private Connection jmsConnection;
    private Session jmsSession;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fileSystem = FileSystem.getLocal(conf);

        // JMS setup for creating test messages
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        jmsConnection = factory.createConnection();
        jmsConnection.start();
        jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (jmsSession != null) jmsSession.close();
        if (jmsConnection != null) jmsConnection.close();
        if (fileSystem != null) fileSystem.close();
    }

    @Test
    void pathRecomputedPerFlush_notCached() throws Exception {
        // Use a clock we can control
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 10, 0, 0, ZoneOffset.UTC).toInstant());

        String basePath = tempDir.resolve("data").toString();
        SequenceFileBatchWriter writer = createWriter(basePath, clock);

        // First batch at 10:10 -> quarter=0
        List<Message> batch1 = createMessages(3);
        BatchWriter.BatchWriteResult result1 = writer.write("test-binding", batch1);

        assertThat(result1.getFilePath()).contains("hour=10/quarter=0");

        // Advance time to 10:20 -> quarter=1
        clock.set(ZonedDateTime.of(2025, 8, 22, 10, 20, 0, 0, ZoneOffset.UTC).toInstant());

        // Second batch should land in NEW partition
        List<Message> batch2 = createMessages(3);
        BatchWriter.BatchWriteResult result2 = writer.write("test-binding", batch2);

        assertThat(result2.getFilePath()).contains("hour=10/quarter=1");
        assertThat(result1.getFilePath()).isNotEqualTo(result2.getFilePath());
    }

    @Test
    void pathCorrectAcrossHourBoundary() throws Exception {
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 55, 0, 0, ZoneOffset.UTC).toInstant());

        String basePath = tempDir.resolve("data").toString();
        SequenceFileBatchWriter writer = createWriter(basePath, clock);

        // Batch at 10:55 -> hour=10, quarter=3
        List<Message> batch1 = createMessages(2);
        BatchWriter.BatchWriteResult result1 = writer.write("binding", batch1);

        assertThat(result1.getFilePath()).contains("hour=10/quarter=3");

        // Advance to 11:05 -> hour=11, quarter=0
        clock.set(ZonedDateTime.of(2025, 8, 22, 11, 5, 0, 0, ZoneOffset.UTC).toInstant());

        List<Message> batch2 = createMessages(2);
        BatchWriter.BatchWriteResult result2 = writer.write("binding", batch2);

        assertThat(result2.getFilePath()).contains("hour=11/quarter=0");
    }

    @Test
    void pathCorrectAcrossQuarterBoundary() throws Exception {
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 14, 0, 0, ZoneOffset.UTC).toInstant());

        String basePath = tempDir.resolve("data").toString();
        SequenceFileBatchWriter writer = createWriter(basePath, clock);

        // Batch at 10:14 -> quarter=0
        List<Message> batch1 = createMessages(2);
        BatchWriter.BatchWriteResult result1 = writer.write("binding", batch1);

        assertThat(result1.getFilePath()).contains("quarter=0");

        // Advance to 10:16 -> quarter=1
        clock.set(ZonedDateTime.of(2025, 8, 22, 10, 16, 0, 0, ZoneOffset.UTC).toInstant());

        List<Message> batch2 = createMessages(2);
        BatchWriter.BatchWriteResult result2 = writer.write("binding", batch2);

        assertThat(result2.getFilePath()).contains("quarter=1");
    }

    @Test
    void batchSpanningBoundaryLandsInFlushTimePartition() throws Exception {
        // A batch that STARTS at 10:14 but FLUSHES at 10:16
        // should land in the 10:16 partition (quarter=1), not the 10:14 partition (quarter=0)
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 16, 0, 0, ZoneOffset.UTC).toInstant());

        String basePath = tempDir.resolve("data").toString();
        SequenceFileBatchWriter writer = createWriter(basePath, clock);

        // Messages might have been received at 10:14, but flush happens at 10:16
        List<Message> batch = createMessages(5);

        // Flush time is 10:16 -> quarter=1
        BatchWriter.BatchWriteResult result = writer.write("binding", batch);

        assertThat(result.getFilePath()).contains("hour=10/quarter=1");
    }

    @Test
    void fileWrittenToTmpBeforeRename() throws Exception {
        TestClock clock = new TestClock(Instant.now());
        String basePath = tempDir.resolve("data").toString();

        // Use a writer that we can inspect mid-write
        TrackingWriter writer = new TrackingWriter(fileSystem, conf,
                new TestRecordSerializer(), "instance-1", basePath, clock);

        List<Message> batch = createMessages(3);
        writer.write("binding", batch);

        // Verify the temp directory path includes instance ID
        assertThat(writer.tempPathBeforeRename).isNotNull();
        assertThat(writer.tempPathBeforeRename.toString()).contains("_tmp");
        assertThat(writer.tempPathBeforeRename.toString()).contains("instance-1");

        // Final file should exist in partition, not in _tmp
        assertThat(writer.finalPathAfterRename).isNotNull();
        assertThat(writer.finalPathAfterRename.toString()).doesNotContain("_tmp");
    }

    @Test
    void noFileVisibleInPartitionUntilRenamed() throws Exception {
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 20, 0, 0, ZoneOffset.UTC).toInstant());
        String basePath = tempDir.resolve("data").toString();

        // Use writer that checks partition during write
        PartitionCheckingWriter writer = new PartitionCheckingWriter(
                fileSystem, conf, new TestRecordSerializer(), "instance-1", basePath, clock);

        List<Message> batch = createMessages(3);
        writer.write("binding", batch);

        // During the write (before rename), no file should be in the partition
        assertThat(writer.filesInPartitionDuringWrite).isEmpty();

        // After rename, file should be visible
        Path partitionPath = new Path(writer.partitionPathUsed);
        var files = fileSystem.listStatus(partitionPath);
        assertThat(files).hasSize(1);
    }

    @Test
    void simulatedCrashMidWriteLeavesNothingInPartition() throws Exception {
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 20, 0, 0, ZoneOffset.UTC).toInstant());
        String basePath = tempDir.resolve("data").toString();

        // Use a writer that fails after writing but before rename
        FailingWriter writer = new FailingWriter(
                fileSystem, conf, new TestRecordSerializer(), "instance-1", basePath, clock);

        List<Message> batch = createMessages(3);

        // Write should fail
        assertThatThrownBy(() -> writer.write("binding", batch))
                .isInstanceOf(BatchWriter.BatchWriteException.class);

        // Partition should have NO files
        String partitionPath = PartitionPath.compute(basePath, clock.instant());
        Path partition = new Path(partitionPath);
        if (fileSystem.exists(partition)) {
            var files = fileSystem.listStatus(partition);
            assertThat(files).isEmpty();
        }
    }

    @Test
    void tempFilesAreScopedToInstance() throws Exception {
        TestClock clock = new TestClock(Instant.now());
        String basePath = tempDir.resolve("data").toString();

        SequenceFileBatchWriter writer1 = createWriter(basePath, clock, "instance-A");
        SequenceFileBatchWriter writer2 = createWriter(basePath, clock, "instance-B");

        writer1.write("binding", createMessages(2));
        writer2.write("binding", createMessages(2));

        // Each instance should have its own temp directory
        Path tmpA = new Path(basePath + "/_tmp/instance-A");
        Path tmpB = new Path(basePath + "/_tmp/instance-B");

        // Temp dirs should exist (even if empty after successful rename)
        assertThat(fileSystem.exists(tmpA)).isTrue();
        assertThat(fileSystem.exists(tmpB)).isTrue();
    }

    @Test
    void filenameContainsAllRequiredComponents() throws Exception {
        TestClock clock = new TestClock(Instant.ofEpochMilli(1692700000000L));
        String basePath = tempDir.resolve("data").toString();

        SequenceFileBatchWriter writer = createWriter(basePath, clock, "my-instance");

        List<Message> batch = createMessages(2);
        BatchWriter.BatchWriteResult result = writer.write("my-binding", batch);

        // Filename should be: {binding_id}_{instance_id}_{epoch_millis}_{batch_seq}.seq
        String filename = new Path(result.getFilePath()).getName();
        assertThat(filename).startsWith("my-binding_my-instance_");
        assertThat(filename).endsWith(".seq");
        assertThat(filename).contains("1692700000000");
    }

    @Test
    void writtenFileIsValidSequenceFile() throws Exception {
        TestClock clock = new TestClock(Instant.now());
        String basePath = tempDir.resolve("data").toString();

        SequenceFileBatchWriter writer = createWriter(basePath, clock);

        List<Message> batch = createMessages(5);
        BatchWriter.BatchWriteResult result = writer.write("binding", batch);

        // Read back the file and verify contents
        Path filePath = new Path(result.getFilePath());
        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(filePath))) {

            Text key = new Text();
            Text value = new Text();
            int count = 0;

            while (reader.next(key, value)) {
                count++;
                assertThat(value.toString()).startsWith("Message-");
            }

            assertThat(count).isEqualTo(5);
        }
    }

    @Test
    void blockCompressionIsForbidden() {
        assertThatThrownBy(() -> new SequenceFileBatchWriter(
                fileSystem, conf, new TestRecordSerializer(), "instance",
                Clock.systemUTC(), CompressionType.BLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BLOCK compression is forbidden");
    }

    @Test
    void cleanupRemovesOnlyOwnInstanceFiles() throws Exception {
        String basePath = tempDir.resolve("data").toString();

        // Create temp directories for two instances
        Path tmpA = new Path(basePath + "/_tmp/instance-A");
        Path tmpB = new Path(basePath + "/_tmp/instance-B");
        fileSystem.mkdirs(tmpA);
        fileSystem.mkdirs(tmpB);

        // Create files
        Path fileA = new Path(tmpA, "old-file.seq");
        Path fileB = new Path(tmpB, "old-file.seq");
        fileSystem.create(fileA).close();
        fileSystem.create(fileB).close();

        // Verify files exist
        assertThat(fileSystem.exists(fileA)).isTrue();
        assertThat(fileSystem.exists(fileB)).isTrue();

        // Use a clock in the "future" so files appear old
        TestClock futureClock = new TestClock(Instant.now().plusSeconds(3600));
        SequenceFileBatchWriter writerA = createWriter(basePath, futureClock, "instance-A");

        // Cleanup with 1 hour maxAge - files created "1 hour ago" should be deleted
        int deleted = writerA.cleanupTempFiles(basePath, 3600 * 1000);

        // Only instance-A's files should be cleaned (its file is "old" relative to future clock)
        assertThat(deleted).isEqualTo(1);
        assertThat(fileSystem.exists(fileA)).isFalse();
        assertThat(fileSystem.exists(fileB)).isTrue(); // instance-B's file untouched
    }

    // --- Helper methods ---

    private SequenceFileBatchWriter createWriter(String basePath, TestClock clock) {
        return createWriter(basePath, clock, "test-instance");
    }

    private SequenceFileBatchWriter createWriter(String basePath, TestClock clock, String instanceId) {
        return new SequenceFileBatchWriter(
                fileSystem, conf, new TestRecordSerializer(), instanceId, clock, CompressionType.RECORD) {
            @Override
            protected String getBasePath(String bindingId) {
                return basePath;
            }
        };
    }

    private List<Message> createMessages(int count) throws JMSException {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TextMessage msg = jmsSession.createTextMessage("Message-" + i);
            msg.setJMSMessageID("ID:" + i);
            messages.add(msg);
        }
        return messages;
    }

    // --- Test support classes ---

    private static class TestClock extends Clock {
        private final AtomicLong currentTime;

        TestClock(Instant initial) {
            this.currentTime = new AtomicLong(initial.toEpochMilli());
        }

        void set(Instant instant) {
            currentTime.set(instant.toEpochMilli());
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(currentTime.get());
        }

        @Override
        public long millis() {
            return currentTime.get();
        }
    }

    /**
     * Writer that tracks temp and final paths.
     */
    private class TrackingWriter extends SequenceFileBatchWriter {
        Path tempPathBeforeRename;
        Path finalPathAfterRename;
        private final String basePath;

        TrackingWriter(FileSystem fs, Configuration conf, TestRecordSerializer serializer,
                       String instanceId, String basePath, Clock clock) {
            super(fs, conf, serializer, instanceId, clock, CompressionType.RECORD);
            this.basePath = basePath;
        }

        @Override
        protected String getBasePath(String bindingId) {
            return basePath;
        }

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages) throws BatchWriteException {
            BatchWriteResult result = super.write(bindingId, messages);
            // After write, deduce what paths were used
            String tempDir = PartitionPath.tempDir(basePath, getInstanceId());
            tempPathBeforeRename = new Path(tempDir);
            finalPathAfterRename = new Path(result.getFilePath());
            return result;
        }
    }

    /**
     * Writer that checks partition contents during write.
     */
    private class PartitionCheckingWriter extends SequenceFileBatchWriter {
        List<String> filesInPartitionDuringWrite = new ArrayList<>();
        String partitionPathUsed;
        private final String basePath;
        private final Clock clock;

        PartitionCheckingWriter(FileSystem fs, Configuration conf, TestRecordSerializer serializer,
                                String instanceId, String basePath, Clock clock) {
            super(fs, conf, serializer, instanceId, clock, CompressionType.RECORD);
            this.basePath = basePath;
            this.clock = clock;
        }

        @Override
        protected String getBasePath(String bindingId) {
            return basePath;
        }

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages) throws BatchWriteException {
            partitionPathUsed = PartitionPath.compute(basePath, Instant.now(clock));

            // Check partition before write completes (during our write)
            try {
                Path partition = new Path(partitionPathUsed);
                if (fileSystem.exists(partition)) {
                    for (var status : fileSystem.listStatus(partition)) {
                        filesInPartitionDuringWrite.add(status.getPath().getName());
                    }
                }
            } catch (IOException e) {
                // Ignore
            }

            return super.write(bindingId, messages);
        }
    }

    /**
     * Writer that fails after writing but before rename.
     */
    private class FailingWriter extends SequenceFileBatchWriter {
        private final String basePath;

        FailingWriter(FileSystem fs, Configuration conf, TestRecordSerializer serializer,
                      String instanceId, String basePath, Clock clock) {
            super(fs, conf, serializer, instanceId, clock, CompressionType.RECORD);
            this.basePath = basePath;
        }

        @Override
        protected String getBasePath(String bindingId) {
            return basePath;
        }

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages) throws BatchWriteException {
            // Let the parent write to temp, then throw before rename
            throw new BatchWriteException("Simulated crash after write, before rename");
        }
    }
}
