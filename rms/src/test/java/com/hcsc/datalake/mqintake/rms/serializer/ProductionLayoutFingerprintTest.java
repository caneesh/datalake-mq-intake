package com.hcsc.datalake.mqintake.rms.serializer;

import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.*;

/**
 * Guards the on-disk layout against the production fingerprint.
 *
 * <p>A SequenceFile header embeds the key and value <em>class names</em>, so
 * its length is a fingerprint of the chosen types. Production files from both
 * feeds show a first-record key of {@code 129}, which is exactly the header
 * length of an empty {@code SequenceFile(LongWritable, Text)} under
 * {@code RECORD} compression — the writer seeds its key from
 * {@code getLength()}.
 *
 * <p>That coincidence is what let the types be established without access to
 * the live writer, and it makes a cheap, precise regression guard: if anyone
 * changes a serializer's declared types, the header length moves off 129 and
 * these fail. The previous declaration of {@code Text}/{@code BytesWritable}
 * produced 130 — a one-byte difference that would have made every file
 * unreadable to a consumer opening it with the production classes.
 */
class ProductionLayoutFingerprintTest {

    /** Header length of an empty production-layout SequenceFile. */
    private static final int PRODUCTION_HEADER_LENGTH = 129;

    @TempDir
    java.nio.file.Path tempDir;

    private Configuration conf;
    private FileSystem fs;

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        fs = FileSystem.getLocal(conf);
    }

    @Test
    void rmsSerializerProducesProductionHeaderFingerprint() throws Exception {
        assertHeaderLength(new RmsRecordSerializer(), "rms.seq");
    }

    @Test
    void rmsDeclaresTheProductionWritableTypes() {
        // Stated explicitly as well as by fingerprint: the modules cannot see
        // each other (rms and claims both depend only on core), so each asserts
        // the shared layout independently.
        RecordSerializer serializer = new RmsRecordSerializer();
        assertThat(serializer.getKeyClass()).isEqualTo(org.apache.hadoop.io.LongWritable.class);
        assertThat(serializer.getValueClass()).isEqualTo(org.apache.hadoop.io.Text.class);
    }

    private void assertHeaderLength(RecordSerializer serializer, String filename) throws Exception {
        Path path = new Path(tempDir.resolve(filename).toString());

        long headerLength;
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(serializer.getKeyClass()),
                SequenceFile.Writer.valueClass(serializer.getValueClass()),
                SequenceFile.Writer.compression(CompressionType.RECORD, new DefaultCodec()))) {
            headerLength = writer.getLength();
        }

        assertThat(headerLength)
                .as("%s declares %s/%s — header %d must match the production " +
                    "fingerprint of %d, or consumers cannot read the file",
                    serializer.getClass().getSimpleName(),
                    serializer.getKeyClass().getSimpleName(),
                    serializer.getValueClass().getSimpleName(),
                    headerLength, PRODUCTION_HEADER_LENGTH)
                .isEqualTo(PRODUCTION_HEADER_LENGTH);
    }
}
