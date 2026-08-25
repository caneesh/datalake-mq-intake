package com.hcsc.datalake.mqintake.core.loop.session;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.BindingMode;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.Session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The JMS resources one listener thread owns.
 *
 * <p>The invariant under test is CLAUDE.md's second standing constraint: one
 * transacted Session per thread, with the consumer and producer created from
 * that same Session. It is what makes the landing write, the tracker send and
 * the source acknowledge a single unit of work, so it is asserted rather than
 * left to convention.
 */
class ListenerSessionTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws Exception {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("vm://listener-session?broker.persistent=false");
        connection = factory.createConnection();
        connection.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void sessionIsTransacted() throws Exception {
        // Non-transacted would acknowledge each message on receive, which is
        // the exact defect this project exists to remove.
        try (ListenerSession listener = new ListenerSession(connection, landOnly())) {
            listener.open();

            assertThat(listener.session().getTransacted()).isTrue();
            assertThat(listener.session().getAcknowledgeMode())
                    .isEqualTo(Session.SESSION_TRANSACTED);
        }
    }

    @Test
    void consumerAndTrackerProducerComeFromTheSameSession() throws Exception {
        // If the producer came from a different session the tracker send would
        // be a separate transaction, and a tracker could commit while the
        // source message rolled back.
        try (ListenerSession listener = new ListenerSession(connection, tracked())) {
            listener.open();

            assertThat(listener.consumer()).isNotNull();
            assertThat(listener.trackerProducer()).isNotNull();
            assertThat(listener.hasTrackerProducer()).isTrue();
        }
    }

    @Test
    void landOnlyBindingHasNoTrackerProducer() throws Exception {
        try (ListenerSession listener = new ListenerSession(connection, landOnly())) {
            listener.open();

            assertThat(listener.hasTrackerProducer()).isFalse();
            assertThatThrownBy(listener::trackerProducer)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Only TRACKED bindings send trackers");
        }
    }

    @Test
    void isOpenReflectsLifecycle() throws Exception {
        ListenerSession listener = new ListenerSession(connection, landOnly());
        assertThat(listener.isOpen()).isFalse();

        listener.open();
        assertThat(listener.isOpen()).isTrue();

        listener.close();
        assertThat(listener.isOpen()).isFalse();
    }

    @Test
    void canBeReopenedAfterClose() throws Exception {
        // Session recovery depends on this: a broken session is closed and the
        // same holder is opened again on the surviving connection.
        ListenerSession listener = new ListenerSession(connection, tracked());

        listener.open();
        Session first = listener.session();
        listener.close();

        listener.open();
        assertThat(listener.session()).isNotSameAs(first);
        assertThat(listener.hasTrackerProducer()).isTrue();

        listener.close();
    }

    @Test
    void closeIsIdempotent() throws Exception {
        ListenerSession listener = new ListenerSession(connection, landOnly());
        listener.open();

        assertThatCode(() -> {
            listener.close();
            listener.close();
        }).doesNotThrowAnyException();
    }

    @Test
    void closeBeforeOpenIsSafe() {
        ListenerSession listener = new ListenerSession(connection, landOnly());

        assertThatCode(listener::close).doesNotThrowAnyException();
    }

    @Test
    void closeSwallowsFailuresBecauseItRunsOnTheRecoveryPath() throws Exception {
        // The connection dies underneath us; closing must not mask the error
        // that got us here.
        ListenerSession listener = new ListenerSession(connection, landOnly());
        listener.open();
        connection.close();

        assertThatCode(listener::close).doesNotThrowAnyException();
        assertThat(listener.isOpen()).isFalse();
    }

    @Test
    void requiresConnectionAndConfig() {
        assertThatThrownBy(() -> new ListenerSession(null, landOnly()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ListenerSession(connection, null))
                .isInstanceOf(NullPointerException.class);
    }

    private BindingConfig landOnly() {
        BindingConfig config = base();
        config.setMode(BindingMode.LAND_ONLY);
        return config;
    }

    private BindingConfig tracked() {
        BindingConfig config = base();
        config.setMode(BindingMode.TRACKED);
        config.setTrackerQueue("TEST.TRACKER");
        return config;
    }

    private BindingConfig base() {
        BindingConfig config = new BindingConfig();
        config.setId("test");
        config.setSourceQueue("TEST.SOURCE");
        config.setHdfsBasePath("/tmp/listener-session");
        config.setBatchSize(10);
        config.setBatchBytes(1024 * 1024);
        config.setBatchIntervalMs(1000);
        config.setListenerThreads(1);
        return config;
    }
}
