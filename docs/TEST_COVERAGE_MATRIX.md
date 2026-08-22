# DESIGN §15 Test Coverage Matrix

Maps each §15 failure scenario to the automated test that proves it on the
**production path** (real BindingRuntimeFactory → BindingRuntime →
TransactedReceiveLoop → SequenceFileBatchWriter → JMS transaction → tracker →
audit → health), or records why it is blocked.

Legend: **PP** = production-path integration test, **U** = unit/component test.

| # | DESIGN §15 scenario | Test class | Test method | Kind | Automated | Status | Notes |
|---|---------------------|-----------|-------------|------|-----------|--------|-------|
| 1 | Kill/fail while batch only in memory | `ProductionPathIntegrationTest` | `gracefulShutdownWithInFlightBatchLosesNothing` | PP | yes | PASS | Drain commits or rolls back; identity count proves zero loss. Also `TransactedReceiveLoopTest.failureMidBatchRollsBackAllMessages` (U). |
| 2 | Failure during `_tmp` SequenceFile write | `ProductionPathIntegrationTest` | `tmpWriteFailureRollsBackThenRedeliveryLandsEverything` | PP | yes | PASS | Injected serialization failure inside real writer; `_tmp` left clean; redelivery lands all identities. |
| 3 | Failure after rename, before MQ commit | `ProductionPathIntegrationTest` | `trackerFailureAfterRenameYieldsPermittedDuplicateNotLoss` | PP | yes | PASS | File visible, MQ rolled back → design-permitted duplicate, zero loss (§12.1). Reconciliation classifies duplicates: `PartitionReconciliationServiceTest.duplicateOrphanQuarantineIsAMoveNotADelete`. |
| 4 | Failure after MQ commit, before audit | `TransactedReceiveLoopTest` | `auditFailureDoesNotUndoCommittedTransaction` | U | yes | PASS | Commit survives audit outage; crash window closed retrospectively by `PartitionReconciliationServiceTest.soleCopyOrphanIsKeptAndRetrospectivelyAudited`. |
| 5 | Failure between RMS tracker send and commit | `TransactedReceiveLoopTest` | `trackerFailureRollsBackEntireBatch` | U | yes | PASS | Tracker messages and gets roll back as one unit. |
| 6 | Tracker queue failure | `ProductionPathIntegrationTest` | `trackerFailureAfterRenameYieldsPermittedDuplicateNotLoss` | PP | yes | PASS | Tracker outage → rollback → recovery → exactly one tracker per committed message. |
| 7 | Deterministic bad payload | `ClaimsBisectionIntegrationTest` | `batchOf16WithOnePoisonIsolatesItWithoutOneByOneProcessing`, `multiplePoisonMessagesAreAllIsolatedSafely` | PP | yes | PASS | Suspect-tracked bisection; only true poison reaches BOQ; < N transactions. |
| 8 | HDFS infrastructure failure | `TransactedReceiveLoopTest` | `infrastructureExceptionDoesNotEnterDegradedMode` | U | partial | PASS/BLOCKED | Classification + rollback automated. True HDFS outage/failover needs a real cluster (see below). |
| 9 | MQ reconnect / session recovery | `TransactedReceiveLoopTest` | `sessionRecoveryExposesReconnectCount`, `reconnectMetricsRecorded` | U | partial | PASS/BLOCKED | Recovery state machine unit-tested; forcing a broker restart needs real IBM MQ (Docker profile: `IbmMqIntegrationTest`). |
| 10 | Graceful shutdown with in-flight batch | `ProductionPathIntegrationTest` | `gracefulShutdownWithInFlightBatchLosesNothing` | PP | yes | PASS | Bounded drain; no force-rename of uncommittable batch. |
| 11 | Claims poison isolation / bisection | `ClaimsBisectionIntegrationTest` | all | PP | yes | PASS | Includes BOTHRESH/BISECT interplay validator rule (`BindingConfigValidator`). |
| 12 | Multiple listeners, redelivery to different thread | `ProductionPathIntegrationTest` + `ClaimsBisectionIntegrationTest` | `multipleListenersRedeliveryLandsAllMessages`; 2-listener bisection | PP | yes | PASS | Identity set proves zero loss across threads. |
| 13 | Binding isolation (RMS vs Claims) | `ProductionPathIntegrationTest` | `failingBindingDoesNotAffectHealthyBinding` | PP | yes | PASS | Failing binding DEGRADED + retained messages; healthy binding lands all. |
| — | Full Spring context, RMS | `RmsApplicationSpringBootTest` | `rmsProductionPathLandsMessagesAndSendsTrackers` | PP | yes | PASS | Real RmsApplication context; land + tracker + audit + health UP. |
| — | Full Spring context, Claims | `ClaimsApplicationSpringBootTest` | `claimsProductionPathLandsMessagesWithoutTracker` | PP | yes | PASS | Real ClaimsApplication context; LAND_ONLY, no tracker, health UP. |
| — | Real IBM MQ connectivity | `IbmMqIntegrationTest` (rms) | 5 tests | PP | opt-in | PASS (needs Docker) | `-Ddocker.mq` profile against IBM MQ Developer Edition. |

## Tests requiring a real environment

These cannot be proven with embedded substitutes and remain required before
production cutover:

1. **IBM MQ queue-manager restart / channel failure** mid-stream — session
   recovery against real MQRC codes, uncommitted batch replay, BOTHRESH
   accumulation by the real queue manager.
2. **HDFS NameNode failover / DataNode loss** during write, close, and rename —
   including behavior on erasure-coded paths.
3. **Kerberos ticket expiry and renewal** under load (KerberosManager relogin).
4. **Real BOTHRESH/BOQNAME queue-manager configuration** matching the
   application's backout thresholds.
5. **Tracker consumer compatibility** — golden-master verification of the
   rewritten MessageHeaderDetails against the legacy system (blocked on §20.4
   artifacts; RMS production startup is gated until then).
