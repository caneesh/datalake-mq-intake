package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Proves the SequenceFile key is the record's byte offset, end to end.
 *
 * <p>The legacy writer does {@code long offset = sequenceFileWriter.getLength()}
 * immediately before each append and uses it as the key, so keys grow with
 * record size and the first record of a fresh file lands on the header length.
 *
 * <p>This drives the real {@link SequenceFileBatchWriter} and reads the file
 * back, rather than asserting the serializer copies a field. A serializer-level
 * test would pass even if the writer never supplied a real offset.
 */
class ByteOffsetKeyIntegrationTest {

    /** Header length of a LongWritable/Text SequenceFile under RECORD compression. */
    private static final long HEADER_LENGTH = 129L;

    @TempDir
    java.nio.file.Path tempDir;

    private Configuration conf;
    private FileSystem fs;
    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fs = FileSystem.getLocal(conf);

        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (Exception ignored) {
        }
    }

    @Test
    void keysAreByteOffsetsAndFirstRecordLandsOnTheHeaderLength() throws Exception {
        String basePath = tempDir.resolve("data").toString();
        BatchWriter writer = new SequenceFileBatchWriter(
                fs, conf, new RmsRecordSerializer(), "it-instance", "rms", basePath);

        // Deliberately different sizes: an ordinal would be 0,1,2 regardless,
        // whereas byte offsets must grow by each record's encoded length.
        List<Message> batch = new ArrayList<>();
        batch.add(session.createTextMessage("<M><MessageID>a</MessageID></M>"));
        batch.add(session.createTextMessage("<M><MessageID>bbbbbbbbbbbbbbbbbbbb</MessageID></M>"));
        batch.add(session.createTextMessage("<M><MessageID>c</MessageID></M>"));

        BatchWriter.BatchWriteResult result = writer.write("rms", batch);

        List<Long> keys = readKeys(result.getFilePath());

        assertThat(keys).hasSize(3);

        // First record begins at the header — the 129 seen in production samples
        assertThat(keys.get(0))
                .as("first record of a fresh file starts at the header length")
                .isEqualTo(HEADER_LENGTH);

        // Strictly increasing, and by more than 1 — i.e. real byte positions,
        // not a 0,1,2 ordinal
        assertThat(keys.get(1)).isGreaterThan(keys.get(0));
        assertThat(keys.get(2)).isGreaterThan(keys.get(1));
        assertThat(keys.get(1) - keys.get(0))
                .as("gap must be the encoded size of record 0, not 1")
                .isGreaterThan(1L);

        // The larger middle record must produce a larger gap after it
        assertThat(keys.get(2) - keys.get(1))
                .as("a longer payload advances the offset further")
                .isGreaterThan(keys.get(1) - keys.get(0));
    }

    @Test
    void keysRestartAtTheHeaderInEachNewFile() throws Exception {
        String basePath = tempDir.resolve("data2").toString();
        BatchWriter writer = new SequenceFileBatchWriter(
                fs, conf, new RmsRecordSerializer(), "it-instance", "rms", basePath);

        // Each batch produces its own file; the legacy writer likewise restarts
        // from the new file's length when it rolls.
        for (int batchNo = 0; batchNo < 2; batchNo++) {
            List<Message> batch = new ArrayList<>();
            batch.add(session.createTextMessage("<M><MessageID>x" + batchNo + "</MessageID></M>"));
            BatchWriter.BatchWriteResult result = writer.write("rms", batch);

            assertThat(readKeys(result.getFilePath()))
                    .as("batch %d", batchNo)
                    .containsExactly(HEADER_LENGTH);
        }
    }

    @Test
    void keyEqualsTheWritersLengthBeforeThatRecord() throws Exception {
        // Independent derivation: rebuild the file record by record and check
        // each key against getLength() taken before the append.
        Path path = new Path(tempDir.resolve("probe.seq").toString());
        List<Long> expected = new ArrayList<>();

        try (SequenceFile.Writer w = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(Text.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD,
                        new org.apache.hadoop.io.compress.DefaultCodec()))) {
            for (String body : new String[]{"one", "two-longer", "three"}) {
                long before = w.getLength();
                expected.add(before);
                w.append(new LongWritable(before), new Text(body));
            }
        }

        assertThat(readKeys(path.toString())).isEqualTo(expected);
        assertThat(expected.get(0)).isEqualTo(HEADER_LENGTH);
    }

    private List<Long> readKeys(String filePath) throws Exception {
        // Resolve the actual landed file: the writer renames into a partition
        Path path = new Path(filePath);
        if (!fs.exists(path)) {
            FileStatus[] found = fs.globStatus(new Path(filePath));
            assertThat(found).as("landed file %s", filePath).isNotEmpty();
            path = found[0].getPath();
        }

        List<Long> keys = new ArrayList<>();
        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path))) {
            LongWritable key = new LongWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                keys.add(key.get());
            }
        }
        return keys;
    }
}
