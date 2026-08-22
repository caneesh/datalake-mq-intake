package com.hcsc.datalake.mqintake.core.failure;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.security.AccessControlException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for FailureClassifier.
 *
 * From DESIGN.md §6.2: Each class is fault-injected in test to verify
 * the classification and degraded-mode decision.
 */
class FailureClassifierTest {

    private FailureClassifier classifier;

    @BeforeEach
    void setUp() {
        classifier = new FailureClassifier();
    }

    // --- MESSAGE_DATA tests ---

    @Test
    void classifiesSerializationExceptionAsMessageData() {
        var exception = new RecordSerializer.SerializationException("Parse error");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(result.triggersDegradedMode()).isTrue();
    }

    @Test
    void classifiesMessageDataExceptionAsMessageData() {
        var exception = new FailureClassifier.MessageDataException("Malformed payload");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(result.triggersDegradedMode()).isTrue();
    }

    @Test
    void classifiesNumberFormatExceptionAsMessageData() {
        var exception = new NumberFormatException("Invalid number");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(result.triggersDegradedMode()).isTrue();
    }

    @Test
    void classifiesBatchWriteExceptionWithParseMessageAsMessageData() {
        var exception = new BatchWriter.BatchWriteException("Failed to parse message");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(result.triggersDegradedMode()).isTrue();
    }

    @Test
    void classifiesWrappedSerializationExceptionAsMessageData() {
        var cause = new RecordSerializer.SerializationException("Parse error");
        var exception = new RuntimeException("Wrapper", cause);

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MESSAGE_DATA);
    }

    // --- MQ_INFRASTRUCTURE tests ---

    @Test
    void classifiesJmsExceptionAsMqInfrastructure() {
        var exception = new JMSException("Connection lost");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesConnectExceptionAsMqInfrastructure() {
        var exception = new ConnectException("Connection refused");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesSocketExceptionAsMqInfrastructure() {
        var exception = new SocketException("Socket closed");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesMaxumsgsExceptionAsMqInfrastructure() {
        var exception = new RuntimeException("MAXUMSGS exceeded");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    // --- HDFS_INFRASTRUCTURE tests ---

    @Test
    void classifiesHadoopExceptionAsHdfsInfrastructure() throws Exception {
        // Simulate Hadoop exception
        var exception = Class.forName("java.io.IOException")
                .getConstructor(String.class)
                .newInstance("NameNode not available");

        FailureClass result = classifier.classify((Throwable) exception);

        // IOException with NameNode message -> HDFS
        assertThat(result).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesDataNodeExceptionAsHdfsInfrastructure() {
        var exception = new IOException("DataNode unavailable");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesQuotaExceptionAsHdfsInfrastructure() {
        var exception = new IOException("quota exceeded");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void hdfsFailureDoesNotTriggerDegradedMode() {
        var exception = new IOException("HDFS write failure: block replication failed");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    // --- SECURITY_CONFIG tests ---

    @Test
    void classifiesJmsSecurityExceptionAsSecurityConfig() {
        var exception = new JMSSecurityException("Not authorized");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.SECURITY_CONFIG);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesLoginExceptionAsSecurityConfig() {
        var exception = new LoginException("Kerberos ticket expired");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.SECURITY_CONFIG);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesAccessControlExceptionAsSecurityConfig() {
        var exception = new AccessControlException("Permission denied");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.SECURITY_CONFIG);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesPermissionDeniedMessageAsSecurityConfig() {
        var exception = new IOException("Permission denied: /data/raw/test");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.SECURITY_CONFIG);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    // --- SHUTDOWN tests ---

    @Test
    void classifiesInterruptedExceptionAsShutdown() {
        var exception = new InterruptedException("Thread interrupted");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.SHUTDOWN);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesClosedByInterruptExceptionAsShutdown() {
        var exception = new ClosedByInterruptException();

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.SHUTDOWN);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesShutdownMessageAsShutdown() {
        var exception = new RuntimeException("Service shutdown in progress");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.SHUTDOWN);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    // --- UNKNOWN tests ---

    @Test
    void classifiesUnknownExceptionAsUnknown() {
        var exception = new RuntimeException("Something unexpected");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.UNKNOWN);
    }

    @Test
    void unknownNeverTriggersDegradedMode() {
        var exception = new RuntimeException("Totally unknown error");

        FailureClass result = classifier.classify(exception);

        assertThat(result).isEqualTo(FailureClass.UNKNOWN);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    @Test
    void classifiesNullAsUnknown() {
        FailureClass result = classifier.classify(null);

        assertThat(result).isEqualTo(FailureClass.UNKNOWN);
        assertThat(result.triggersDegradedMode()).isFalse();
    }

    // --- Alert level tests ---

    @Test
    void messageDataAlertsOnDegradedEntry() {
        assertThat(FailureClass.MESSAGE_DATA.getAlertLevel())
                .isEqualTo(FailureClass.AlertLevel.ON_DEGRADED_ENTRY);
    }

    @Test
    void mqInfrastructureAlertsImmediately() {
        assertThat(FailureClass.MQ_INFRASTRUCTURE.getAlertLevel())
                .isEqualTo(FailureClass.AlertLevel.IMMEDIATE);
    }

    @Test
    void securityConfigPages() {
        assertThat(FailureClass.SECURITY_CONFIG.getAlertLevel())
                .isEqualTo(FailureClass.AlertLevel.PAGE);
    }

    @Test
    void unknownPages() {
        assertThat(FailureClass.UNKNOWN.getAlertLevel())
                .isEqualTo(FailureClass.AlertLevel.PAGE);
    }

    @Test
    void shutdownIsInfoOnly() {
        assertThat(FailureClass.SHUTDOWN.getAlertLevel())
                .isEqualTo(FailureClass.AlertLevel.INFO);
    }
}
