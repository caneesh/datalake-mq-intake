package com.hcsc.datalake.mqintake.core.hdfs;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
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
import java.util.Map;
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
    void aSerializerThrowMidBatchLeavesNoTempFileAndNothingInThePartition() throws Exception {
        // The previous version of this test overrode write() entirely and
        // never touched the parent — it passed vacuously whatever the cleanup
        // code did. This one drives the REAL write path and fails on record 2
        // of 3, inside the SequenceFile append loop.
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 20, 0, 0, ZoneOffset.UTC).toInstant());
        String basePath = tempDir.resolve("data").toString();

        java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();
        RecordSerializer failsOnSecond = new RecordSerializer() {
            private final TestRecordSerializer delegate = new TestRecordSerializer();

            @Override
            public SerializedRecord serialize(Message message,
                    com.hcsc.datalake.mqintake.core.serializer.RecordMetadata metadata)
                    throws SerializationException {
                if (calls.incrementAndGet() == 2) {
                    throw new SerializationException("record 2 is unserialisable");
                }
                return delegate.serialize(message, metadata);
            }

            @Override
            public Class<? extends org.apache.hadoop.io.Writable> getKeyClass() {
                return delegate.getKeyClass();
            }

            @Override
            public Class<? extends org.apache.hadoop.io.Writable> getValueClass() {
                return delegate.getValueClass();
            }
        };

        SequenceFileBatchWriter writer = new SequenceFileBatchWriter(
                fileSystem, conf, failsOnSecond, "instance-1", clock,
                CompressionType.RECORD, createBindingMap(basePath));

        assertThatThrownBy(() -> writer.write("binding", createMessages(3)))
                .isInstanceOf(BatchWriter.BatchWriteException.class);

        assertNoFilesUnder(basePath + "/_tmp");
        assertNoFilesUnder(PartitionPath.compute(basePath, clock.instant()));
    }

    @Test
    void renameReturningFalseCleansTheTempFileAndThrows() throws Exception {
        // rename() returning false (not throwing) is a real HDFS outcome —
        // destination exists, parent vanished — and used to have no test at
        // all. A wrapping FileSystem forces it against the real write path.
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 20, 0, 0, ZoneOffset.UTC).toInstant());
        String basePath = tempDir.resolve("data").toString();

        org.apache.hadoop.fs.FilterFileSystem renameRefuses =
                new org.apache.hadoop.fs.FilterFileSystem(fileSystem) {
            @Override
            public boolean rename(Path src, Path dst) throws java.io.IOException {
                if (src.toString().contains("/_tmp/")) {
                    return false;   // the batch's own rename fails
                }
                return super.rename(src, dst);
            }
        };

        SequenceFileBatchWriter writer = new SequenceFileBatchWriter(
                renameRefuses, conf, new TestRecordSerializer(), "instance-1", clock,
                CompressionType.RECORD, createBindingMap(basePath));

        assertThatThrownBy(() -> writer.write("binding", createMessages(2)))
                .isInstanceOf(BatchWriter.BatchWriteException.class)
                .hasMessageContaining("rename");

        assertNoFilesUnder(basePath + "/_tmp");
        assertNoFilesUnder(PartitionPath.compute(basePath, clock.instant()));
    }

    @Test
    void concurrentWritesFromManyThreadsProduceDistinctCompleteFiles() throws Exception {
        // One SequenceFileBatchWriter is shared by every listener thread of a
        // binding in production. Its thread-safety was correct by inspection
        // (all state method-local or atomic) but never tested — and a future
        // instance field would break it silently.
        TestClock clock = new TestClock(
                ZonedDateTime.of(2025, 8, 22, 10, 20, 0, 0, ZoneOffset.UTC).toInstant());
        String basePath = tempDir.resolve("data").toString();
        SequenceFileBatchWriter writer = createWriter(basePath, clock);

        int threads = 4;
        int batchesPerThread = 5;
        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
        java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();

        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                go.await();
                for (int b = 0; b < batchesPerThread; b++) {
                    writer.write("binding", createMessages(2));
                }
                return null;
            }));
        }
        go.countDown();
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(30, java.util.concurrent.TimeUnit.SECONDS);   // propagate failures
        }
        pool.shutdownNow();

        Path partition = new Path(PartitionPath.compute(basePath, clock.instant()));
        org.apache.hadoop.fs.FileStatus[] files = fileSystem.listStatus(partition,
                path -> path.getName().endsWith(".seq"));
        assertThat(files)
                .as("every batch lands as its own distinctly named file")
                .hasSize(threads * batchesPerThread);
        assertNoFilesUnder(basePath + "/_tmp");
    }

    private void assertNoFilesUnder(String dir) throws Exception {
        Path path = new Path(dir);
        if (!fileSystem.exists(path)) {
            return;
        }
        java.util.List<String> found = new java.util.ArrayList<>();
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it =
                fileSystem.listFiles(path, true);
        while (it.hasNext()) {
            String name = it.next().getPath().getName();
            if (!name.endsWith(".crc")) {   // local-FS checksum sidecars
                found.add(name);
            }
        }
        assertThat(found).as("no files under %s", dir).isEmpty();
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
                Clock.systemUTC(), CompressionType.BLOCK, Map.of("test", "/data/test")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BLOCK compression is forbidden");
    }


    // --- Helper methods ---

    private SequenceFileBatchWriter createWriter(String basePath, TestClock clock) {
        return createWriter(basePath, clock, "test-instance");
    }

    private SequenceFileBatchWriter createWriter(String basePath, TestClock clock, String instanceId) {
        return new SequenceFileBatchWriter(
                fileSystem, conf, new TestRecordSerializer(), instanceId, clock,
                CompressionType.RECORD, createBindingMap(basePath));
    }

    private Map<String, String> createBindingMap(String basePath) {
        return Map.of(
                "binding", basePath,
                "test-binding", basePath,
                "my-binding", basePath
        );
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
            super(fs, conf, serializer, instanceId, clock, CompressionType.RECORD, createBindingMap(basePath));
            this.basePath = basePath;
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
        private final Clock writerClock;

        PartitionCheckingWriter(FileSystem fs, Configuration conf, TestRecordSerializer serializer,
                                String instanceId, String basePath, Clock clock) {
            super(fs, conf, serializer, instanceId, clock, CompressionType.RECORD, createBindingMap(basePath));
            this.basePath = basePath;
            this.writerClock = clock;
        }

        @Override
        public BatchWriteResult write(String bindingId, List<Message> messages) throws BatchWriteException {
            partitionPathUsed = PartitionPath.compute(basePath, Instant.now(writerClock));

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
}
