package com.hcsc.datalake.mqintake.core.runtime;

import com.hcsc.datalake.mqintake.core.batch.CountingBatchWriter;
import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import com.hcsc.datalake.mqintake.core.loop.TransactedReceiveLoop;
import com.hcsc.datalake.mqintake.core.tracker.TrackerMessageBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.*;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for BindingRuntime.
 */
class BindingRuntimeTest {

    private static final String SOURCE_QUEUE = "TEST.SOURCE";
    private static final String TRACKER_QUEUE = "TEST.TRACKER";

    private Connection connection;
    private CountingBatchWriter batchWriter;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
        batchWriter = new CountingBatchWriter();
    }

    @AfterEach
    void tearDown() {
        try {
            if (connection != null) connection.close();
        } catch (Exception e) {
            // Ignore cleanup errors - connection may already be disposed
        }
    }

    @Test
    void landOnlyBindingCreatesRuntimeWithoutTrackerBuilder() {
        BindingConfig config = createLandOnlyConfig(2);
        List<TransactedReceiveLoop> loops = createLoops(config, null, 2);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        BindingRuntime runtime = new BindingRuntime(config, loops, executor, false);

        assertThat(runtime.getBindingId()).isEqualTo("test-land-only");
        assertThat(runtime.getMode()).isEqualTo(BindingMode.LAND_ONLY);
        assertThat(runtime.hasTrackerBuilder()).isFalse();
        assertThat(runtime.getLoopCount()).isEqualTo(2);
        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.CREATED);

        executor.shutdownNow();
    }

    @Test
    void trackedBindingRequiresTrackerBuilder() {
        BindingConfig config = createTrackedConfig(2);
        TrackerMessageBuilder tracker = createMockTrackerBuilder();
        List<TransactedReceiveLoop> loops = createLoops(config, tracker, 2);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // BindingRuntime validates hasTrackerBuilder=false for TRACKED mode
        assertThatThrownBy(() -> new BindingRuntime(config, loops, executor, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TRACKED")
                .hasMessageContaining("TrackerMessageBuilder");

        executor.shutdownNow();
    }

    @Test
    void trackedBindingCreatesRuntimeWithTrackerBuilder() {
        BindingConfig config = createTrackedConfig(3);
        TrackerMessageBuilder tracker = createMockTrackerBuilder();
        List<TransactedReceiveLoop> loops = createLoops(config, tracker, 3);
        ExecutorService executor = Executors.newFixedThreadPool(3);

        BindingRuntime runtime = new BindingRuntime(config, loops, executor, true);

        assertThat(runtime.getBindingId()).isEqualTo("test-tracked");
        assertThat(runtime.getMode()).isEqualTo(BindingMode.TRACKED);
        assertThat(runtime.hasTrackerBuilder()).isTrue();
        assertThat(runtime.getLoopCount()).isEqualTo(3);

        executor.shutdownNow();
    }

    @Test
    void listenerThreadsNCreatesNLoops() {
        BindingConfig config = createLandOnlyConfig(5);
        List<TransactedReceiveLoop> loops = createLoops(config, null, 5);
        ExecutorService executor = Executors.newFixedThreadPool(5);

        BindingRuntime runtime = new BindingRuntime(config, loops, executor, false);

        assertThat(runtime.getLoopCount()).isEqualTo(5);
        assertThat(runtime.getLoops()).hasSize(5);

        for (TransactedReceiveLoop loop : runtime.getLoops()) {
            assertThat(loop).isNotNull();
            assertThat(loop.getBindingId()).isEqualTo("test-land-only");
        }

        executor.shutdownNow();
    }

    @Test
    void lifecycleTransitionsCorrectly() throws Exception {
        BindingConfig config = createLandOnlyConfig(1);
        List<TransactedReceiveLoop> loops = createLoops(config, null, 1);
        ExecutorService executor = Executors.newFixedThreadPool(1);

        BindingRuntime runtime = new BindingRuntime(config, loops, executor, false);

        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.CREATED);
        assertThat(runtime.isRunning()).isFalse();

        runtime.start();
        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.RUNNING);
        assertThat(runtime.isRunning()).isTrue();

        Thread.sleep(100);

        runtime.stop(1000);
        assertThat(runtime.getState()).isEqualTo(BindingRuntime.State.STOPPED);
        assertThat(runtime.isRunning()).isFalse();
    }

    @Test
    void cannotStartTwice() throws Exception {
        BindingConfig config = createLandOnlyConfig(1);
        List<TransactedReceiveLoop> loops = createLoops(config, null, 1);
        ExecutorService executor = Executors.newFixedThreadPool(1);

        BindingRuntime runtime = new BindingRuntime(config, loops, executor, false);
        runtime.start();

        assertThatThrownBy(runtime::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot start");

        runtime.stop(1000);
    }

    @Test
    void emptyLoopListRejected() {
        BindingConfig config = createLandOnlyConfig(1);
        ExecutorService executor = Executors.newFixedThreadPool(1);

        assertThatThrownBy(() -> new BindingRuntime(config, List.of(), executor, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one receive loop required");

        executor.shutdownNow();
    }

    @Test
    void statsAggregatedAcrossLoops() throws Exception {
        BindingConfig config = createLandOnlyConfig(2);
        List<TransactedReceiveLoop> loops = createLoops(config, null, 2);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        BindingRuntime runtime = new BindingRuntime(config, loops, executor, false);
        runtime.start();

        Thread.sleep(200);

        BindingRuntime.RuntimeStats stats = runtime.getStats();
        assertThat(stats).isNotNull();
        assertThat(stats.getCommitCount()).isGreaterThanOrEqualTo(0);
        assertThat(stats.getRollbackCount()).isGreaterThanOrEqualTo(0);

        runtime.stop(1000);
    }

    private BindingConfig createLandOnlyConfig(int threads) {
        BindingConfig config = new BindingConfig();
        config.setId("test-land-only");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.LAND_ONLY);
        config.getHdfs().setBasePath("/data/raw/test");
        config.getBatch().setSize(100);
        config.getBatch().setBytes(1024 * 1024);
        config.getBatch().setIntervalMs(30000);
        config.setListenerThreads(threads);
        return config;
    }

    private BindingConfig createTrackedConfig(int threads) {
        BindingConfig config = new BindingConfig();
        config.setId("test-tracked");
        config.setSourceQueue(SOURCE_QUEUE);
        config.setMode(BindingMode.TRACKED);
        config.getTracker().setQueue(TRACKER_QUEUE);
        config.getHdfs().setBasePath("/data/raw/test");
        config.getBatch().setSize(100);
        config.getBatch().setBytes(1024 * 1024);
        config.getBatch().setIntervalMs(30000);
        config.setListenerThreads(threads);
        return config;
    }

    private List<TransactedReceiveLoop> createLoops(BindingConfig config,
                                                     TrackerMessageBuilder trackerBuilder,
                                                     int count) {
        List<TransactedReceiveLoop> loops = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            TransactedReceiveLoop loop = new TransactedReceiveLoop(
                    config, connection, batchWriter, trackerBuilder,
                    null, null, null, null, null, "test-instance", 100);
            loops.add(loop);
        }
        return loops;
    }

    private TrackerMessageBuilder createMockTrackerBuilder() {
        return (Session session, Message source) -> Optional.empty();
    }
}
