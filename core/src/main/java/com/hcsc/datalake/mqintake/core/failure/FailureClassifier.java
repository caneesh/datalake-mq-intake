package com.hcsc.datalake.mqintake.core.failure;

import com.hcsc.datalake.mqintake.core.batch.BatchWriter;
import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.security.AccessControlException;

/**
 * Classifies exceptions into failure classes for degraded-mode decisions.
 *
 * <p>From DESIGN.md §6.2: Misclassify an infrastructure failure as poison
 * and the binding crawls at batch-of-1 through an outage; misclassify a
 * poison message as infrastructure and it rolls back forever.
 *
 * <p>CRITICAL: Unknown failures NEVER trigger degraded mode.
 */
public class FailureClassifier {

    private static final Logger log = LoggerFactory.getLogger(FailureClassifier.class);

    /**
     * Classifies an exception into a failure class.
     *
     * @param throwable the exception to classify
     * @return the failure class
     */
    public FailureClass classify(Throwable throwable) {
        if (throwable == null) {
            return FailureClass.UNKNOWN;
        }

        // Check the exception and its cause chain
        FailureClass result = classifyDirect(throwable);
        if (result != FailureClass.UNKNOWN) {
            return result;
        }

        // Check cause chain
        Throwable cause = throwable.getCause();
        while (cause != null && cause != throwable) {
            result = classifyDirect(cause);
            if (result != FailureClass.UNKNOWN) {
                return result;
            }
            cause = cause.getCause();
        }

        log.warn("Unclassified exception: {} - {}", throwable.getClass().getName(), throwable.getMessage());
        return FailureClass.UNKNOWN;
    }

    /**
     * Classifies a single exception without checking cause chain.
     */
    private FailureClass classifyDirect(Throwable t) {
        // Message / data failures are classified by TYPE, so they must be
        // checked before the shutdown heuristic below, which falls back to a
        // free-text search for "interrupted"/"shutdown" in the message. A
        // SerializationException naming a payload field like shutdownReason
        // would otherwise classify as SHUTDOWN, which never enters degraded
        // mode — the poison message would never be isolated and would keep
        // rolling back every clean message batched with it.
        if (isMessageDataException(t)) {
            return FailureClass.MESSAGE_DATA;
        }

        // Shutdown / cancellation
        if (isShutdownException(t)) {
            return FailureClass.SHUTDOWN;
        }

        // Security / configuration
        if (isSecurityConfigException(t)) {
            return FailureClass.SECURITY_CONFIG;
        }

        // HDFS infrastructure
        if (isHdfsInfrastructureException(t)) {
            return FailureClass.HDFS_INFRASTRUCTURE;
        }

        // MQ infrastructure
        if (isMqInfrastructureException(t)) {
            return FailureClass.MQ_INFRASTRUCTURE;
        }

        return FailureClass.UNKNOWN;
    }

    private boolean isShutdownException(Throwable t) {
        if (t instanceof InterruptedException) {
            return true;
        }
        if (t instanceof InterruptedIOException) {
            return true;
        }
        if (t instanceof ClosedByInterruptException) {
            return true;
        }
        // Thread interruption message patterns
        String msg = t.getMessage();
        if (msg != null && (msg.contains("interrupted") || msg.contains("shutdown"))) {
            return true;
        }
        return false;
    }

    private boolean isMessageDataException(Throwable t) {
        // Serialization failures
        if (t instanceof RecordSerializer.SerializationException) {
            return true;
        }

        // Batch write failures due to message content
        if (t instanceof BatchWriter.BatchWriteException) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("serialize") || msg.contains("parse") ||
                    msg.contains("malformed") || msg.contains("invalid"))) {
                return true;
            }
        }

        // Message-level parse errors
        if (t instanceof MessageDataException) {
            return true;
        }

        // XML/JSON parsing failures
        String className = t.getClass().getName();
        if (className.contains("ParseException") || className.contains("ParserException") ||
                className.contains("MalformedInputException") || className.contains("XMLStreamException") ||
                className.contains("JsonParseException") || className.contains("SAXException")) {
            return true;
        }

        // NumberFormatException, IllegalArgumentException from parsing
        if (t instanceof NumberFormatException) {
            return true;
        }

        return false;
    }

    private boolean isSecurityConfigException(Throwable t) {
        if (t instanceof JMSSecurityException) {
            return true;
        }
        if (t instanceof SecurityException) {
            return true;
        }
        if (t instanceof AccessControlException) {
            return true;
        }
        if (t instanceof LoginException) {
            return true;
        }

        // Kerberos-related
        String className = t.getClass().getName();
        if (className.contains("KerberosException") || className.contains("GSSException") ||
                className.contains("KrbException")) {
            return true;
        }

        // Permission denied in messages
        String msg = t.getMessage();
        if (msg != null && (msg.contains("Permission denied") || msg.contains("Access denied") ||
                msg.contains("not authorized") || msg.contains("credential"))) {
            return true;
        }

        return false;
    }

    private boolean isHdfsInfrastructureException(Throwable t) {
        String className = t.getClass().getName();

        // Hadoop/HDFS specific exceptions
        if (className.contains("hadoop") || className.contains("hdfs")) {
            // Except security exceptions which are classified separately
            if (!className.contains("Security") && !className.contains("Access")) {
                return true;
            }
        }

        // IO exceptions with HDFS-like messages
        if (t instanceof IOException) {
            String msg = t.getMessage();
            if (msg != null) {
                if (msg.contains("NameNode") || msg.contains("DataNode") ||
                        msg.contains("HDFS") || msg.contains("quota") ||
                        msg.contains("block") || msg.contains("replication") ||
                        msg.contains("lease") || msg.contains("safemode")) {
                    return true;
                }
            }
        }

        // File system errors (could be HDFS)
        if (className.contains("FileSystem") || className.contains("RemoteException")) {
            return true;
        }

        return false;
    }

    private boolean isMqInfrastructureException(Throwable t) {
        // JMS exceptions (except security)
        if (t instanceof JMSException && !(t instanceof JMSSecurityException)) {
            return true;
        }

        // MQ-specific exceptions
        String className = t.getClass().getName();
        if (className.contains("MQ") || className.contains("mq.jms")) {
            return true;
        }

        // Connection/socket issues (network to queue manager)
        if (t instanceof ConnectException || t instanceof SocketException ||
                t instanceof SocketTimeoutException) {
            return true;
        }

        // MAXUMSGS and syncpoint failures in messages
        String msg = t.getMessage();
        if (msg != null && (msg.contains("MAXUMSGS") || msg.contains("syncpoint") ||
                msg.contains("queue manager") || msg.contains("channel"))) {
            return true;
        }

        return false;
    }

    /**
     * Marker interface for message-level data exceptions.
     * Implementations should throw this for payload-specific failures.
     */
    public static class MessageDataException extends RuntimeException {
        public MessageDataException(String message) {
            super(message);
        }

        public MessageDataException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
