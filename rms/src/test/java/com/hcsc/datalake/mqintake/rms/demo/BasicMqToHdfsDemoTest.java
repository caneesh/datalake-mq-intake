package com.hcsc.datalake.mqintake.rms.demo;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.rms.serializer.RmsRecordSerializer;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The plainest possible statement of what this service is for: a message put on
 * an IBM MQ queue ends up in a file on HDFS, byte for byte.
 *
 * <p>Nothing is mocked. A real queue manager, the production serializer, the
 * production writer, the production receive loop. The only substitution is the
 * local filesystem standing in for HDFS, which is a Hadoop FileSystem either
 * way.
 *
 * <p>It exists because it is worth being able to answer "does the basic thing
 * work" by running one test, without reading past the poison handling, the
 * degraded mode and the controls.
 */
@EnabledIfEnvironmentVariable(named = "MQ_USER", matches = ".+")
class BasicMqToHdfsDemoTest {

    private static final String QUEUE = "DEV.QUEUE.1";

    @Test
    void aMessagePutOnTheQueueEndsUpInAFileOnHdfs() throws Exception {
        List<String> sent = List.of(
                "<Member><MessageID>demo-1</MessageID><Name>first</Name></Member>",
                "<Member><MessageID>demo-2</MessageID><Name>second</Name></Member>",
                "<Member><MessageID>demo-3</MessageID><Name>third</Name></Member>");

        java.nio.file.Path landing = Files.createTempDirectory("basic-demo");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);

        Connection connection = connectionFactory()
                .createConnection(System.getenv("MQ_USER"), System.getenv("MQ_PASSWORD"));
        connection.start();

        drain(connection);

        // --- put three messages on the queue ---
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             MessageProducer producer = session.createProducer(session.createQueue(QUEUE))) {
            for (String body : sent) {
                producer.send(session.createTextMessage(body));
            }
        }

        // --- run the production loop against it ---
        BindingConfig config = new BindingConfig();
        config.setId("demo");
        config.setSourceQueue(QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.setHdfsBasePath(landing.toString());
        config.setBatchSize(3);
        config.setBatchBytes(64L * 1024 * 1024);
        config.setBatchIntervalMs(500);
        config.setListenerThreads(1);

        BatchWriter writer = new SequenceFileBatchWriter(
                fs, conf, new RmsRecordSerializer(), "demo-instance",
                config.getId(), config.getHdfsBasePath());

        TransactedReceiveLoop loop = new TransactedReceiveLoop(
                config, connection, writer, null, null, null, null, null, null,
                "demo-instance", 200);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(loop);

        long deadline = System.currentTimeMillis() + 20_000;
        while (seqFiles(landing).isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        loop.stop();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // --- read the file back and compare ---
        List<java.nio.file.Path> files = seqFiles(landing);
        assertThat(files).as("a file must have landed").isNotEmpty();

        List<String> landed = new ArrayList<>();
        List<Long> offsets = new ArrayList<>();
        for (java.nio.file.Path file : files) {
            try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(new Path(file.toString())))) {
                LongWritable key = new LongWritable();
                Text value = new Text();
                while (reader.next(key, value)) {
                    offsets.add(key.get());
                    landed.add(value.toString());
                }
            }
        }

        System.out.println("\n================ MQ -> HDFS ================");
        System.out.println("queue      : " + QUEUE);
        System.out.println("sent       : " + sent.size() + " messages");
        System.out.println("file       : " + files.get(0).getFileName());
        System.out.println("records    : " + landed.size());
        for (int i = 0; i < landed.size(); i++) {
            System.out.println("  offset " + offsets.get(i) + "  " + landed.get(i));
        }
        System.out.println("queue depth after: " + depth(connection));
        System.out.println("============================================\n");

        assertThat(landed).containsExactlyInAnyOrderElementsOf(sent);
        assertThat(depth(connection)).as("queue drained").isZero();

        connection.close();
        fs.delete(new Path(landing.toString()), true);
    }

    private List<java.nio.file.Path> seqFiles(java.nio.file.Path root) throws Exception {
        try (var stream = Files.walk(root)) {
            return stream.filter(p -> p.toString().endsWith(".seq"))
                    .filter(p -> !p.toString().contains("/_tmp/"))
                    .collect(java.util.stream.Collectors.toList());
        } catch (Exception e) {
            return List.of();
        }
    }

    private int depth(Connection connection) throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            javax.jms.QueueBrowser browser = s.createBrowser(s.createQueue(QUEUE));
            int n = 0;
            var e = browser.getEnumeration();
            while (e.hasMoreElements()) { e.nextElement(); n++; }
            return n;
        }
    }

    private void drain(Connection connection) throws Exception {
        try (Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             javax.jms.MessageConsumer c = s.createConsumer(s.createQueue(QUEUE))) {
            while (c.receive(300) != null) { /* drain */ }
        }
    }

    private MQConnectionFactory connectionFactory() throws Exception {
        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setHostName("localhost");
        factory.setPort(1414);
        factory.setQueueManager("QM1");
        factory.setChannel("DEV.APP.SVRCONN");
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        return factory;
    }
}
