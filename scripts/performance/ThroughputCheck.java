import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcsc.datalake.mqintake.core.audit.HdfsAuditRecordEmitter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.failure.DegradedModeManager;
import com.hcsc.datalake.mqintake.core.failure.DegradationStrategy;
import com.hcsc.datalake.mqintake.core.hdfs.SequenceFileBatchWriter;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.metrics.BindingMetrics;
import com.hcsc.datalake.mqintake.core.poison.PoisonMessageHandler;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import com.hcsc.datalake.mqintake.rms.serializer.RmsRecordSerializer;
import com.hcsc.datalake.mqintake.rms.tracker.RmsTrackerMessageBuilder;
import com.hcsc.datalake.mqintake.claims.serializer.ClaimsRecordSerializer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.LoggerFactory;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.nio.file.Files;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Standalone, opt-in local baseline; never connects to external MQ or HDFS. */
public class ThroughputCheck {
    private static final int PAYLOAD_BYTES = 600 * 1024;
    private static final long TIMEOUT = TimeUnit.MINUTES.toNanos(5);
    private static String prefix;
    private static String suffix;

    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !Set.of("rms", "claims").contains(args[0])
                || !Set.of("local", "hdfs").contains(args[1])) {
            throw new IllegalArgumentException("Usage: rms|claims local|hdfs count offeredMessagesPerSecond (0=burst)");
        }
        String binding = args[0];
        int count = Integer.parseInt(args[2]);
        double rate = Double.parseDouble(args[3]);
        if (count < 1 || count > 999999 || !Double.isFinite(rate) || rate < 0) {
            throw new IllegalArgumentException("count must be 1..999999; rate must be finite and nonnegative");
        }
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
        String tag = binding.equals("rms") ? "MessageID" : "CLM_XMITSN_ID";
        prefix = "<Record><" + tag + ">";
        String closing = "</" + tag + "><Data>";
        String end = "</Data></Record>";
        Random random = new Random(20260913);
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder padding = new StringBuilder();
        int paddingLength = PAYLOAD_BYTES - prefix.length() - 6 - closing.length() - end.length();
        for (int i = 0; i < paddingLength; i++) padding.append(alphabet.charAt(random.nextInt(alphabet.length())));
        suffix = closing + padding + end;

        java.nio.file.Path work = Files.createTempDirectory("intake-throughput-");
        MiniDFSCluster cluster = null;
        FileSystem fs = null;
        BrokerService broker = new BrokerService();
        Connection connection = null;
        List<TransactedReceiveLoop> loops = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        List<Map<String, Object>> samples = new ArrayList<>();
        try {
            Configuration conf = new HdfsConfiguration();
            if (args[1].equals("hdfs")) {
                conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, work.resolve("hdfs").toString());
                conf.setInt("dfs.replication", 3);
                conf.setFloat("dfs.namenode.safemode.threshold-pct", 0.0f);
                cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
                cluster.waitActive();
                fs = cluster.getFileSystem();
            } else {
                conf.set("fs.defaultFS", "file:///");
                fs = FileSystem.getLocal(conf);
            }
            broker.setBrokerName("throughput");
            broker.setPersistent(false);
            broker.setUseJmx(false);
            broker.setUseShutdownHook(false);
            broker.getSystemUsage().getMemoryUsage().setLimit(4L * 1024 * 1024 * 1024);
            broker.setDataDirectoryFile(work.resolve("broker").toFile());
            broker.start();
            broker.waitUntilStarted();
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://throughput?create=false");
            // Pull delivery makes dispatched count an observation of receive(), not prefetch.
            factory.getPrefetchPolicy().setQueuePrefetch(0);
            factory.setAlwaysSyncSend(true);
            connection = factory.createConnection();
            connection.start();
            BindingConfig config = new BindingConfig();
            config.setId(binding);
            config.setSourceQueue("SOURCE");
            config.setMode(binding.equals("rms") ? BindingMode.TRACKED : BindingMode.LAND_ONLY);
            config.setListenerThreads(4);
            config.getBatch().setSize(binding.equals("rms") ? 1000 : 8000);
            config.getBatch().setBytes(128L * 1024 * 1024);
            config.getBatch().setIntervalMs(0);
            config.getTracker().setQueue("TRACKER");
            config.getBackout().setQueue("BACKOUT");
            config.getAudit().setBalanceCheckEnabled(true);
            String base = args[1].equals("hdfs") ? "/benchmark" : work.resolve("landing").toString();
            config.getHdfs().setBasePath(base);
            RecordSerializer serializer = binding.equals("rms") ? new RmsRecordSerializer() : new ClaimsRecordSerializer();
            BindingMetrics metrics = new BindingMetrics(binding);
            SequenceFileBatchWriter writer = new SequenceFileBatchWriter(fs, conf, serializer, "benchmark", binding, base);
            HdfsAuditRecordEmitter audit = new HdfsAuditRecordEmitter(fs, base + "-audit", "benchmark", Clock.systemUTC());
            RmsTrackerMessageBuilder tracker = binding.equals("rms")
                    ? new RmsTrackerMessageBuilder(RmsTrackerMessageBuilder.TrackerFields.defaultRms()) : null;
            DegradedModeManager degradation = new DegradedModeManager(binding, config.getBatch().getSize(),
                    tracker == null ? DegradationStrategy.BISECT : DegradationStrategy.BATCH_OF_ONE, 10);
            for (int i = 0; i < 4; i++) {
                TransactedReceiveLoop loop = new TransactedReceiveLoop(config, connection, writer, tracker,
                        new PoisonMessageHandler(14, "BACKOUT"), degradation, null, audit, metrics, "benchmark", 100);
                loops.add(loop);
                threads.add(new Thread(loop));
            }
            long start = System.nanoTime();
            // Burst is preloaded: its timed interval excludes producer preparation.
            if (rate > 0) {
                threads.forEach(Thread::start);
                awaitRunning(loops);
                start = System.nanoTime();
            }
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageProducer producer = session.createProducer(session.createQueue("SOURCE"))) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT); // broker itself remains nonpersistent
                for (int i = 0; i < count; i++) {
                    if (rate > 0) {
                        long delay = start + (long) (i * 1_000_000_000.0 / rate) - System.nanoTime();
                        if (delay > 0) TimeUnit.NANOSECONDS.sleep(delay);
                    }
                    TextMessage message = session.createTextMessage(payload(i));
                    if (tracker != null) message.setStringProperty("MessageHeaderDetails",
                            "<MessageHeaderDetailsType><MesgStatus>SENT</MesgStatus></MessageHeaderDetailsType>");
                    producer.send(message);
                    if (rate > 0 && ((i + 1) % 250 == 0 || i + 1 == count)) {
                        Map<String, Object> sample = new LinkedHashMap<>();
                        sample.put("seconds", (System.nanoTime() - start) / 1e9);
                        sample.put("sent", i + 1);
                        sample.put("committed", metrics.getMessagesConsumed());
                        sample.put("sourceOutstandingIncludingUncommitted", broker.getDestination(new ActiveMQQueue("SOURCE"))
                                .getDestinationStatistics().getMessages().getCount());
                        sample.put("heapUsedMiBIncludingBrokerAndHdfs", (Runtime.getRuntime().totalMemory()
                                - Runtime.getRuntime().freeMemory()) / 1048576);
                        samples.add(sample);
                        System.out.println("THROUGHPUT_SAMPLE " + new ObjectMapper().writeValueAsString(sample));
                    }
                }
            }
            double offerSeconds = (System.nanoTime() - start) / 1e9;
            if (rate == 0) {
                start = System.nanoTime();
                threads.forEach(Thread::start);
                awaitRunning(loops);
            }
            long deadline = System.nanoTime() + TIMEOUT;
            while (broker.getDestination(new ActiveMQQueue("SOURCE")).getDestinationStatistics().getDispatched().getCount() < count
                    || !allReceiving(threads)) {
                check(System.nanoTime() < deadline, "Timed out draining source");
                check(metrics.getRollbackCount() == 0, "Unexpected rollback");
                Thread.sleep(50);
            }
            // With interval=0, residual batches wait for a partition boundary.
            // Stop only while all listeners are in receive(), avoiding interruption of HDFS I/O.
            long beforeDrain = metrics.getMessagesConsumed();
            loops.forEach(TransactedReceiveLoop::stop);
            for (Thread thread : threads) thread.join(120000);
            double seconds = (System.nanoTime() - start) / 1e9;
            for (Thread thread : threads) check(!thread.isAlive(), "Listener did not stop");
            check(metrics.getMessagesConsumed() == count, "Committed count mismatch");
            check(metrics.getMessagesWritten() == count, "Written count mismatch");
            check(metrics.getRollbackCount() == 0, "Rollback occurred");
            check(metrics.getBalanceCheckFailures() == 0, "Balance failure");
            check(metrics.getAuditFailureCount() == 0, "Audit failure");
            check(metrics.getPoisonMessagesRouted() == 0, "Backout routing occurred");
            check(metrics.getTrackerFailureCount() == 0 && metrics.getTrackerSuppressedCount() == 0, "Tracker failure/suppression");
            check(metrics.getTrackerSentCount() == (tracker != null ? count : 0), "Tracker count mismatch");
            BitSet seen = new BitSet(count);
            long fileBytes = 0;
            int fileCount = 0;
            var files = fs.listFiles(new Path(base), true);
            while (files.hasNext()) {
                var file = files.next();
                if (!file.getPath().getName().startsWith(binding + "_benchmark_")) continue;
                check(!file.getPath().toString().contains("/_tmp/"), "Unpublished sequence file");
                fileCount++;
                fileBytes += file.getLen();
                try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file.getPath()))) {
                    LongWritable key = new LongWritable();
                    Text value = new Text();
                    while (reader.next(key, value)) {
                        String actual = value.toString();
                        int id = Integer.parseInt(actual.substring(prefix.length(), prefix.length() + 6));
                        check(id >= 0 && id < count && !seen.get(id), "Unexpected/duplicate record");
                        check(actual.equals(payload(id)), "Payload mismatch");
                        seen.set(id);
                    }
                }
            }
            check(seen.cardinality() == count, "Missing landed records");
            int tracked = 0;
            BitSet trackerSeen = new BitSet(count);
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageConsumer consumer = session.createConsumer(session.createQueue("TRACKER"))) {
                Message message;
                while ((message = consumer.receive(100)) != null) {
                    String body = ((TextMessage) message).getText();
                    int id = Integer.parseInt(body.substring(prefix.length(), prefix.length() + 6));
                    check(id >= 0 && id < count && !trackerSeen.get(id) && body.equals(payload(id)), "Tracker payload mismatch/duplicate");
                    trackerSeen.set(id);
                    tracked++;
                }
            }
            check(tracked == (tracker != null ? count : 0), "Tracker queue count mismatch");
            check(broker.getDestination(new ActiveMQQueue("SOURCE")).getDestinationStatistics().getMessages().getCount() == 0, "Source not empty");
            ObjectMapper mapper = new ObjectMapper();
            long auditRecords = 0;
            var audits = fs.listFiles(new Path(base + "-audit"), true);
            while (audits.hasNext()) {
                var file = audits.next();
                if (!file.getPath().getName().endsWith(".json")) continue;
                try (var in = fs.open(file.getPath())) {
                    auditRecords += mapper.readTree(in).path("record_count").asLong();
                }
            }
            check(auditRecords == count, "Audit record count mismatch: " + auditRecords);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("binding", binding);
            result.put("filesystem", fs.getUri().getScheme());
            result.put("hdfsReplication", cluster == null ? 0 : 3);
            result.put("broker", "embedded ActiveMQ, nonpersistent, VM transport, prefetch=0");
            result.put("payloadBytes", PAYLOAD_BYTES);
            result.put("listeners", 4);
            result.put("offeredMessagesPerSecond", rate);
            result.put("producerSeconds", offerSeconds);
            result.put("messages", count);
            result.put("elapsedSecondsIncludingShutdownDrain", seconds);
            result.put("messagesPerSecond", count / seconds);
            result.put("rawMiBPerSecond", count * (double) PAYLOAD_BYTES / seconds / 1048576);
            result.put("committedBeforeShutdownDrain", beforeDrain);
            result.put("files", fileCount);
            result.put("fileBytes", fileBytes);
            result.put("meanFlushMs", metrics.getAverageFlushLatency().toNanos() / 1e6);
            result.put("rollbacks", metrics.getRollbackCount());
            result.put("verifiedRecords", seen.cardinality());
            result.put("verifiedTrackerRecords", tracked);
            result.put("verifiedAuditRecords", auditRecords);
            result.put("samples", samples);
            System.out.println("THROUGHPUT_RESULT " + mapper.writeValueAsString(result));
        } finally {
            loops.forEach(TransactedReceiveLoop::stop);
            for (Thread thread : threads) if (thread.isAlive()) thread.join(10000);
            if (connection != null) connection.close();
            broker.stop();
            if (cluster != null) cluster.shutdown(true);
            else if (fs != null) fs.close();
            // Only delete this run's generated temporary directory.
            try (var paths = Files.walk(work)) {
                for (java.nio.file.Path path : (Iterable<java.nio.file.Path>) paths.sorted(Comparator.reverseOrder())::iterator) Files.deleteIfExists(path);
            }
        }
    }

    private static String payload(int id) { return prefix + String.format(Locale.ROOT, "%06d", id) + suffix; }
    private static void check(boolean condition, String message) { if (!condition) throw new IllegalStateException(message); }
    private static void awaitRunning(List<TransactedReceiveLoop> loops) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT;
        while (loops.stream().anyMatch(loop -> !loop.isRunning())) {
            check(System.nanoTime() < deadline, "Listener startup timeout");
            Thread.sleep(10);
        }
    }
    private static boolean allReceiving(List<Thread> threads) {
        return threads.stream().allMatch(thread -> Arrays.stream(thread.getStackTrace()).anyMatch(frame ->
                frame.getClassName().equals("org.apache.activemq.ActiveMQMessageConsumer") && frame.getMethodName().equals("receive")));
    }
}
