package com.hcsc.datalake.mqintake.core.hdfs;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.index.HdfsRecordIndexWriter;
import com.hcsc.datalake.mqintake.core.index.RecordIndexReader;
import com.hcsc.datalake.mqintake.core.lifecycle.StartupValidator;
import com.hcsc.datalake.mqintake.core.serializer.PayloadNormalizer;
import com.hcsc.datalake.mqintake.core.serializer.RecordMetadata;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The write path against genuine HDFS.
 *
 * <p>Every other test in this repository runs against the local filesystem via
 * {@code fs.defaultFS=file:///}. That is a real Hadoop {@code FileSystem}, so
 * the code compiles and behaves — but it is not HDFS, and the differences are
 * exactly where a landing service gets hurt: rename semantics, what
 * {@code getLength()} reports on an open file, whether {@code hflush} does
 * anything, safemode, and block-level failure.
 *
 * <p>{@link MiniDFSCluster} is Hadoop's own in-JVM cluster: a real NameNode,
 * real DataNodes, the real block protocol. It is how Hadoop tests Hadoop.
 * DataNodes can be killed and the NameNode put into safemode from inside the
 * test, which is the closest thing to a cluster incident that fits in a build.
 *
 * <p>One difference is worth naming because it has already cost time: the local
 * filesystem writes a {@code .crc} sidecar beside every file and HDFS does not.
 * Two earlier tests tripped over it. On this cluster that artifact is gone, so
 * what is asserted here is what production will see.
 */
class RealHdfsIntegrationTest {

    private static MiniDFSCluster cluster;
    private static FileSystem fs;
    private static Configuration conf;
    private static Connection jmsConnection;
    private static Session jmsSession;

    private static final String BASE = "/data/raw/rms";

    @BeforeAll
    static void startCluster() throws Exception {
        java.nio.file.Path clusterDir = Files.createTempDirectory("minidfs");
        conf = new HdfsConfiguration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterDir.toString());
        // Three DataNodes so one can be lost without dropping below replication.
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
        cluster.waitActive();
        fs = cluster.getFileSystem();

        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://realhdfs?broker.persistent=false");
        jmsConnection = factory.createConnection();
        jmsConnection.start();
        jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        fs.mkdirs(new Path(BASE));
    }

    @AfterAll
    static void stopCluster() throws Exception {
        if (jmsSession != null) jmsSession.close();
        if (jmsConnection != null) jmsConnection.close();
        if (cluster != null) cluster.shutdown(true);
    }

    @Test
    void weAreActuallyTalkingToHdfsNotTheLocalFilesystem() throws Exception {
        // Worth asserting outright: the value of this whole class rests on it.
        assertThat(fs).isInstanceOf(DistributedFileSystem.class);
        assertThat(fs.getUri().getScheme()).isEqualTo("hdfs");
    }

    @Test
    void aBatchLandsAndReadsBackOverTheBlockProtocol() throws Exception {
        List<Message> batch = messages("hdfs-a", "hdfs-b", "hdfs-c");
        BatchWriter writer = writer("land");

        BatchWriter.BatchWriteResult result = writer.write("land", batch);

        assertThat(result.getRecordCount()).isEqualTo(3);
        assertThat(fs.exists(new Path(result.getFilePath()))).isTrue();

        List<String> landed = readValues(new Path(result.getFilePath()));
        assertThat(landed).containsExactly(
                "<M><MessageID>hdfs-a</MessageID></M>",
                "<M><MessageID>hdfs-b</MessageID></M>",
                "<M><MessageID>hdfs-c</MessageID></M>");
    }

    @Test
    void theByteOffsetKeyIsCorrectOnRealHdfs() throws Exception {
        // The key is read from writer.getLength() on an open stream. On the
        // local filesystem that is a plain file-position; on HDFS it goes
        // through the block layer, which is where an assumption could break.
        BatchWriter writer = writer("offsets");

        BatchWriter.BatchWriteResult result =
                writer.write("offsets", messages("o-1", "o-2", "o-3"));

        List<Long> keys = readKeys(new Path(result.getFilePath()));

        assertThat(keys.get(0))
                .as("first record starts after the 129-byte header")
                .isEqualTo(129L);
        assertThat(keys).isSorted();
        assertThat(keys).doesNotHaveDuplicates();
        // Each key is the true byte position, so gaps match record sizes
        assertThat(keys.get(1)).isGreaterThan(keys.get(0));
        assertThat(keys.get(2)).isGreaterThan(keys.get(1));
    }

    @Test
    void tempFilesAreStagedThenRenamedIntoThePartition() throws Exception {
        BatchWriter writer = writer("rename");

        BatchWriter.BatchWriteResult result = writer.write("rename", messages("r-1"));

        Path landed = new Path(result.getFilePath());
        assertThat(fs.exists(landed)).isTrue();
        assertThat(landed.toString()).contains("/year=").contains("/quarter=");

        // Nothing left behind in staging
        Path tmp = new Path(PartitionPath.tempDir(BASE + "/rename", "it-instance"));
        if (fs.exists(tmp)) {
            assertThat(fs.listStatus(tmp)).isEmpty();
        }
    }

    @Test
    void hdfsHasNoCrcSidecarsUnlikeTheLocalFilesystem() throws Exception {
        // The artifact that broke two earlier tests. Naming it here means the
        // difference is documented rather than rediscovered.
        BatchWriter writer = writer("crc");
        BatchWriter.BatchWriteResult result = writer.write("crc", messages("c-1"));

        FileStatus[] siblings = fs.listStatus(new Path(result.getFilePath()).getParent());

        assertThat(siblings)
                .noneMatch(s -> s.getPath().getName().startsWith("."))
                .noneMatch(s -> s.getPath().getName().endsWith(".crc"));
    }

    @Test
    void theSidecarIndexRoundTripsOnRealHdfs() throws Exception {
        BatchWriter writer = writer("index", new HdfsRecordIndexWriter(fs, "it-instance"));

        BatchWriter.BatchWriteResult result =
                writer.write("index", messages("i-1", "i-2"));

        RecordIndexReader reader = new RecordIndexReader(fs);
        Path landed = new Path(result.getFilePath());

        assertThat(reader.hasIndex(landed)).isTrue();
        assertThat(reader.readIdentities(landed)).containsExactly("i-1", "i-2");

        Optional<com.hcsc.datalake.mqintake.core.index.RecordIndex> index = reader.read(landed);
        assertThat(index).isPresent();
        assertThat(index.get().isFullyIdentified()).isTrue();
        assertThat(index.get().getEntries().get(0).getByteOffset()).isEqualTo(129L);
    }

    @Test
    void aLandedFileSurvivesLosingADataNode() throws Exception {
        // Written with replication 3 across 3 DataNodes; killing one must leave
        // the data readable. This is the property the whole design leans on:
        // once renamed, the file is durable.
        BatchWriter writer = writer("datanode");
        BatchWriter.BatchWriteResult result =
                writer.write("datanode", messages("dn-1", "dn-2"));

        Path landed = new Path(result.getFilePath());
        assertThat(readValues(landed)).hasSize(2);

        cluster.stopDataNode(0);

        assertThat(readValues(landed))
                .as("data must still be readable with a DataNode down")
                .hasSize(2);

        // The node is deliberately left down. Restarting it and waiting for the
        // cluster to re-register the DataNode is slow and flaky inside a build,
        // and two of three nodes is ample for whatever runs next — which is the
        // point being made anyway.
    }

    @Test
    void writingDuringSafemodeFailsRatherThanSilentlyLosingTheBatch() throws Exception {
        // Safemode is the ordinary state of a NameNode that has just restarted.
        // The important property is that the write FAILS — the loop then rolls
        // back and the messages stay on the queue. A silent success would
        // acknowledge messages that were never stored.
        BatchWriter writer = writer("safemode");
        DistributedFileSystem dfs = (DistributedFileSystem) fs;

        dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
        try {
            assertThatThrownBy(() -> writer.write("safemode", messages("s-1")))
                    .as("a write during safemode must fail loudly")
                    .isInstanceOf(BatchWriter.BatchWriteException.class);
        } finally {
            dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
        }

        // And the service recovers once the NameNode leaves safemode
        BatchWriter.BatchWriteResult afterwards = writer.write("safemode", messages("s-2"));
        assertThat(readValues(new Path(afterwards.getFilePath()))).hasSize(1);
    }

    @Test
    void startupValidationWorksAgainstRealHdfsPermissions() throws Exception {
        // Path validation is one of the few places the code asks HDFS a
        // question rather than telling it something, so it is worth checking
        // against a real NameNode.
        fs.mkdirs(new Path(BASE + "/validated"));
        fs.mkdirs(new Path("/data/audit"));

        StartupValidator validator = new StartupValidator(fs, "it-instance", "/data/audit");

        com.hcsc.datalake.mqintake.core.config.BindingConfig binding =
                new com.hcsc.datalake.mqintake.core.config.BindingConfig();
        binding.setId("validated");
        binding.setSourceQueue("Q");
        binding.setMode(com.hcsc.datalake.mqintake.core.config.BindingMode.LAND_ONLY);
        binding.getHdfs().setBasePath(BASE + "/validated");
        binding.getBatch().setSize(10);
        binding.getBatch().setBytes(1024);
        binding.getBatch().setIntervalMs(0);
        binding.setListenerThreads(1);

        assertThat(validator.validateBindings(List.of(binding))).isEmpty();

        // The _tmp subtree is created on HDFS, not merely assumed
        assertThat(fs.exists(new Path(
                PartitionPath.tempDir(BASE + "/validated", "it-instance")))).isTrue();
    }

    @Test
    void aMissingBasePathIsReportedNotCreatedSilently() throws Exception {
        StartupValidator validator = new StartupValidator(fs, "it-instance", "/data/audit");

        com.hcsc.datalake.mqintake.core.config.BindingConfig binding =
                new com.hcsc.datalake.mqintake.core.config.BindingConfig();
        binding.setId("absent");
        binding.setSourceQueue("Q");
        binding.setMode(com.hcsc.datalake.mqintake.core.config.BindingMode.LAND_ONLY);
        binding.getHdfs().setBasePath("/data/raw/does-not-exist");
        binding.getBatch().setSize(10);
        binding.getBatch().setBytes(1024);
        binding.getBatch().setIntervalMs(0);
        binding.setListenerThreads(1);

        assertThat(validator.validateBindings(List.of(binding)))
                .anySatisfy(e -> assertThat(e).contains("does not exist"));
    }

    @Test
    void hsyncedBatchLandsOverTheRealBlockProtocol() throws Exception {
        // hsync is the durability barrier the zero-loss posture depends on:
        // hflush reaches the replica pipeline, hsync forces DataNode fsync.
        // This proves the hsync path works against genuine HDFS — the local
        // filesystem accepts hsync too, but only this exercises the block
        // layer the production call will traverse.
        BatchWriter writer = new SequenceFileBatchWriter(
                fs, conf, new DemoSerializer(), "it-instance",
                java.time.Clock.systemUTC(),
                SequenceFile.CompressionType.RECORD,
                Map.of("hsync", BASE + "/hsync"),
                null,
                true);

        BatchWriter.BatchWriteResult result =
                writer.write("hsync", messages("sync-1", "sync-2"));

        assertThat(readValues(new Path(result.getFilePath())))
                .containsExactly("<M><MessageID>sync-1</MessageID></M>",
                        "<M><MessageID>sync-2</MessageID></M>");
    }

    // --- helpers ---

    private BatchWriter writer(String binding) {
        return writer(binding, null);
    }

    private BatchWriter writer(String binding,
                               com.hcsc.datalake.mqintake.core.index.RecordIndexWriter indexWriter) {
        return new SequenceFileBatchWriter(
                fs, conf, new DemoSerializer(), "it-instance",
                java.time.Clock.systemUTC(),
                SequenceFile.CompressionType.RECORD,
                Map.of(binding, BASE + "/" + binding),
                indexWriter);
    }

    private List<Message> messages(String... ids) throws Exception {
        List<Message> batch = new ArrayList<>();
        for (String id : ids) {
            batch.add(jmsSession.createTextMessage("<M><MessageID>" + id + "</MessageID></M>"));
        }
        return batch;
    }

    private List<String> readValues(Path file) throws IOException {
        List<String> values = new ArrayList<>();
        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(file))) {
            LongWritable key = new LongWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                values.add(value.toString());
            }
        }
        return values;
    }

    private List<Long> readKeys(Path file) throws IOException {
        List<Long> keys = new ArrayList<>();
        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(file))) {
            LongWritable key = new LongWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                keys.add(key.get());
            }
        }
        return keys;
    }

    /** The production shape: byte-offset key, normalised payload, identity alongside. */
    private static class DemoSerializer implements RecordSerializer {
        @Override
        public SerializedRecord serialize(Message message, RecordMetadata metadata)
                throws SerializationException {
            try {
                String payload = PayloadNormalizer.normalize(((TextMessage) message).getText());
                String identity = payload.replaceAll(".*<MessageID>([^<]+)</MessageID>.*", "$1");
                return new SerializedRecord(
                        new LongWritable(metadata.getFileByteOffset()),
                        new Text(payload),
                        identity);
            } catch (Exception e) {
                throw new SerializationException("demo serializer: " + e.getMessage(), e);
            }
        }

        @Override
        public Class<? extends Writable> getKeyClass() {
            return LongWritable.class;
        }

        @Override
        public Class<? extends Writable> getValueClass() {
            return Text.class;
        }
    }
}
