package com.hcsc.datalake.mqintake.core.failure;

import com.hcsc.datalake.mqintake.core.serializer.RecordSerializer;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DegradedModeManager.
 *
 * Verifies:
 * - Only MESSAGE_DATA triggers degraded mode
 * - RMS strategy: batch-of-1
 * - Claims strategy: bisect
 * - Restore after M consecutive successes
 * - HDFS failure does NOT trigger degraded mode
 */
class DegradedModeManagerTest {

    // --- RMS: Batch-of-1 strategy tests ---

    @Test
    void rmsStrategy_degradesToBatchOfOne() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 4000, DegradationStrategy.BATCH_OF_ONE, 10);

        assertThat(manager.getCurrentBatchSize()).isEqualTo(4000);
        assertThat(manager.isInDegradedMode()).isFalse();

        // Message data failure triggers degraded mode
        manager.recordFailure(new RecordSerializer.SerializationException("Parse error"));

        assertThat(manager.isInDegradedMode()).isTrue();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(1);
    }

    @Test
    void rmsStrategy_multipleFailuresStayAtOne() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 4000, DegradationStrategy.BATCH_OF_ONE, 10);

        manager.recordFailure(new RecordSerializer.SerializationException("Error 1"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(1);

        manager.recordFailure(new RecordSerializer.SerializationException("Error 2"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(1);
    }

    // --- Claims: Bisect strategy tests ---

    @Test
    void claimsStrategy_bisectsOnFirstFailure() {
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 8000, DegradationStrategy.BISECT, 10);

        assertThat(manager.getCurrentBatchSize()).isEqualTo(8000);

        manager.recordFailure(new RecordSerializer.SerializationException("Parse error"));

        assertThat(manager.isInDegradedMode()).isTrue();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(4000); // 8000 / 2
    }

    @Test
    void claimsStrategy_bisectsRepeatedlyToIsolate() {
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 8000, DegradationStrategy.BISECT, 10);

        manager.recordFailure(new RecordSerializer.SerializationException("Error 1"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(4000);

        manager.recordFailure(new RecordSerializer.SerializationException("Error 2"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(2000);

        manager.recordFailure(new RecordSerializer.SerializationException("Error 3"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(1000);

        manager.recordFailure(new RecordSerializer.SerializationException("Error 4"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(500);
    }

    @Test
    void claimsStrategy_bisectsToOneEventually() {
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 8, DegradationStrategy.BISECT, 10);

        manager.recordFailure(new RecordSerializer.SerializationException("Error"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(4);

        manager.recordFailure(new RecordSerializer.SerializationException("Error"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(2);

        manager.recordFailure(new RecordSerializer.SerializationException("Error"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(1);

        // At 1, stays at 1
        manager.recordFailure(new RecordSerializer.SerializationException("Error"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(1);
    }

    // --- Restore after M successes ---

    @Test
    void restoresNormalBatchSizeAfterMConsecutiveSuccesses() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 4000, DegradationStrategy.BATCH_OF_ONE, 5);

        // Enter degraded mode
        manager.recordFailure(new RecordSerializer.SerializationException("Error"));
        assertThat(manager.getCurrentBatchSize()).isEqualTo(1);

        // 4 successes - not enough
        for (int i = 0; i < 4; i++) {
            manager.recordSuccess();
        }
        assertThat(manager.isInDegradedMode()).isTrue();

        // 5th success - restores
        manager.recordSuccess();
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(4000);
    }

    @Test
    void failureResetsConsecutiveSuccessCount() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 4000, DegradationStrategy.BATCH_OF_ONE, 5);

        manager.recordFailure(new RecordSerializer.SerializationException("Error"));

        // 3 successes
        for (int i = 0; i < 3; i++) {
            manager.recordSuccess();
        }
        assertThat(manager.getConsecutiveSuccesses()).isEqualTo(3);

        // Another failure resets the count
        manager.recordFailure(new RecordSerializer.SerializationException("Another error"));
        assertThat(manager.getConsecutiveSuccesses()).isEqualTo(0);
    }

    // --- Failure classification integration ---

    @Test
    void messageDataFailureTriggersDegradedMode() {
        DegradedModeManager manager = new DegradedModeManager(
                "test", 100, DegradationStrategy.BATCH_OF_ONE, 5);

        FailureClass result = manager.recordFailure(
                new RecordSerializer.SerializationException("Bad data")).getFailureClass();

        assertThat(result).isEqualTo(FailureClass.MESSAGE_DATA);
        assertThat(manager.isInDegradedMode()).isTrue();
    }

    @Test
    void mqInfrastructureFailureDoesNotTriggerDegradedMode() {
        DegradedModeManager manager = new DegradedModeManager(
                "test", 100, DegradationStrategy.BATCH_OF_ONE, 5);

        FailureClass result = manager.recordFailure(new JMSException("Connection lost")).getFailureClass();

        assertThat(result).isEqualTo(FailureClass.MQ_INFRASTRUCTURE);
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(100);
    }

    @Test
    void hdfsInfrastructureFailureDoesNotTriggerDegradedMode() {
        DegradedModeManager manager = new DegradedModeManager(
                "test", 100, DegradationStrategy.BATCH_OF_ONE, 5);

        FailureClass result = manager.recordFailure(
                new IOException("NameNode failover in progress")).getFailureClass();

        assertThat(result).isEqualTo(FailureClass.HDFS_INFRASTRUCTURE);
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(100);
    }

    @Test
    void unknownFailureNeverTriggersDegradedMode() {
        DegradedModeManager manager = new DegradedModeManager(
                "test", 100, DegradationStrategy.BATCH_OF_ONE, 5);

        FailureClass result = manager.recordFailure(
                new RuntimeException("Totally unexpected error")).getFailureClass();

        assertThat(result).isEqualTo(FailureClass.UNKNOWN);
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(100);
    }

    @Test
    void shutdownFailureDoesNotTriggerDegradedMode() {
        DegradedModeManager manager = new DegradedModeManager(
                "test", 100, DegradationStrategy.BATCH_OF_ONE, 5);

        FailureClass result = manager.recordFailure(new InterruptedException("Shutdown")).getFailureClass();

        assertThat(result).isEqualTo(FailureClass.SHUTDOWN);
        assertThat(manager.isInDegradedMode()).isFalse();
    }

    @Test
    void securityFailureDoesNotTriggerDegradedMode() {
        DegradedModeManager manager = new DegradedModeManager(
                "test", 100, DegradationStrategy.BATCH_OF_ONE, 5);

        FailureClass result = manager.recordFailure(
                new SecurityException("Permission denied")).getFailureClass();

        assertThat(result).isEqualTo(FailureClass.SECURITY_CONFIG);
        assertThat(manager.isInDegradedMode()).isFalse();
    }

    // --- Scenario tests ---

    @Test
    void oneBadPayloadInLargeBatchIsolatesWithoutLosingSurroundingMessages() {
        // Scenario: 1000-message batch, one poison message
        // With bisect: 1000 -> 500 -> 250 -> 125 -> 62 -> 31 -> 15 -> 7 -> 3 -> 1
        // The poison message is isolated, surrounding 999 messages commit normally

        DegradedModeManager manager = new DegradedModeManager(
                "claims", 1000, DegradationStrategy.BISECT, 10);

        // Simulate isolation process
        int failures = 0;
        while (manager.getCurrentBatchSize() > 1) {
            manager.recordFailure(new RecordSerializer.SerializationException("Bad message"));
            failures++;
        }

        assertThat(manager.getCurrentBatchSize()).isEqualTo(1);
        // log2(1000) ≈ 10 bisections to reach 1
        assertThat(failures).isLessThanOrEqualTo(10);
    }

    @Test
    void degradationLevelTrackedForBisect() {
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 8000, DegradationStrategy.BISECT, 10);

        assertThat(manager.getDegradationLevel()).isEqualTo(0);

        manager.recordFailure(new RecordSerializer.SerializationException("Error 1"));
        assertThat(manager.getDegradationLevel()).isEqualTo(0); // First entry

        manager.recordFailure(new RecordSerializer.SerializationException("Error 2"));
        assertThat(manager.getDegradationLevel()).isEqualTo(1); // Deepened

        manager.recordFailure(new RecordSerializer.SerializationException("Error 3"));
        assertThat(manager.getDegradationLevel()).isEqualTo(2); // Deepened again
    }

    @Test
    void suspectsAreRegisteredBeforeDegradedModeBecomesVisible() {
        // recordFailure() and markBatchSuspect() used to be two calls from the
        // loop. Between them another listener thread committing a clean batch
        // could see "degraded, enough successes, no suspects outstanding" and
        // restore the full batch size while the poison was still in flight.
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 16, DegradationStrategy.BISECT, 1);

        manager.recordFailure(new RecordSerializer.SerializationException("bad"),
                java.util.List.of("ID:1", "ID:2"));

        assertThat(manager.isInDegradedMode()).isTrue();
        assertThat(manager.getSuspectCount()).isEqualTo(2);

        // A success must not restore while those suspects are outstanding
        manager.recordSuccess();
        assertThat(manager.isInDegradedMode()).isTrue();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(8);

        manager.clearSuspects(java.util.List.of("ID:1", "ID:2"));
        manager.recordSuccess();
        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(16);
    }

    @Test
    void infrastructureBlipDoesNotDiscardProgressTowardRestore() {
        // While isolating a poison message, an unrelated HDFS wobble used to
        // reset consecutiveSuccesses, so routine flakiness could pin the
        // binding in reduced-throughput mode indefinitely.
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 100, DegradationStrategy.BATCH_OF_ONE, 3);

        manager.recordFailure(new RecordSerializer.SerializationException("bad"));
        assertThat(manager.isInDegradedMode()).isTrue();

        manager.recordSuccess();
        manager.recordSuccess();

        // Infrastructure failure: does not trigger degraded mode, and must not
        // wipe the two successes already banked
        manager.recordFailure(new IOException("HDFS wobble"));

        manager.recordSuccess();

        assertThat(manager.isInDegradedMode()).isFalse();
        assertThat(manager.getCurrentBatchSize()).isEqualTo(100);
    }

    // --- Transition-edge reporting ---
    //
    // recordFailure/recordSuccess report the entered/exited edge themselves,
    // from inside the synchronized transition. The receive loop keys its
    // health updates and degraded-entry/exit metrics off these flags, so
    // "entering" must be distinguishable from "deepening", and "restored"
    // from "still counting".

    @Test
    void firstDataFailureReportsEntry_deepeningDoesNot() {
        DegradedModeManager manager = new DegradedModeManager(
                "claims", 64, DegradationStrategy.BISECT, 3);

        assertThat(manager.recordFailure(
                new RecordSerializer.SerializationException("bad"))
                .enteredDegradedMode()).isTrue();

        // Already degraded: this halves the batch again but is not an entry
        assertThat(manager.recordFailure(
                new RecordSerializer.SerializationException("bad again"))
                .enteredDegradedMode()).isFalse();
        assertThat(manager.isInDegradedMode()).isTrue();
    }

    @Test
    void nonDataFailureNeverReportsEntry() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 100, DegradationStrategy.BATCH_OF_ONE, 3);

        assertThat(manager.recordFailure(new IOException("HDFS wobble"))
                .enteredDegradedMode()).isFalse();
        assertThat(manager.isInDegradedMode()).isFalse();
    }

    @Test
    void recordSuccessReportsExitExactlyOnTheRestoringCall() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 100, DegradationStrategy.BATCH_OF_ONE, 3);

        // Not degraded: no edge to report
        assertThat(manager.recordSuccess()).isFalse();

        manager.recordFailure(new RecordSerializer.SerializationException("bad"));

        assertThat(manager.recordSuccess()).isFalse();  // 1/3
        assertThat(manager.recordSuccess()).isFalse();  // 2/3
        assertThat(manager.recordSuccess()).isTrue();   // 3/3 — restores
        assertThat(manager.isInDegradedMode()).isFalse();

        // Restored: back to no-edge
        assertThat(manager.recordSuccess()).isFalse();
    }

    @Test
    void outstandingSuspectsSuppressTheExitEdgeUntilCleared() {
        DegradedModeManager manager = new DegradedModeManager(
                "rms", 100, DegradationStrategy.BATCH_OF_ONE, 1);

        manager.recordFailure(new RecordSerializer.SerializationException("bad"),
                java.util.List.of("ID:poison"));

        // Threshold met, but the suspect is unresolved: no restore, no edge
        assertThat(manager.recordSuccess()).isFalse();
        assertThat(manager.isInDegradedMode()).isTrue();

        manager.clearSuspects(java.util.List.of("ID:poison"));
        assertThat(manager.recordSuccess()).isTrue();
        assertThat(manager.isInDegradedMode()).isFalse();
    }

}
