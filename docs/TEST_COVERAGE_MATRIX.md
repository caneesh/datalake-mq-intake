# DESIGN §15 Test Coverage Matrix

Maps each §15 failure scenario to the automated test that proves it, using the
same four assurance levels as `READINESS_REVIEW.md`:

| Level | Meaning |
|---|---|
| **real-MQ** | Exercised on the production path against an actual IBM MQ queue manager |
| **prod-path** | Exercised on the production path (real `BindingRuntimeFactory` → `BindingRuntime` → `TransactedReceiveLoop` → `SequenceFileBatchWriter` → JMS transaction → tracker → audit → health), with embedded ActiveMQ standing in for MQ |
| **unit** | Component-level only; the production path is not driven end to end |
| **pending** | Not automatable here; requires real infrastructure |

The real-MQ / prod-path split is the load-bearing one. Embedded ActiveMQ cannot
reproduce the queue-manager behaviour poison handling depends on — delivery-count
accumulation across rollbacks, message-identity stability across redelivery, and
real connection-loss error codes — so a prod-path pass is weaker evidence than a
real-MQ pass for anything touching those.

## DESIGN §15 scenarios

| # | Scenario | Assurance | Test | Notes |
|---|----------|-----------|------|-------|
| 1 | Kill/fail while batch only in memory | prod-path | `ProductionPathIntegrationTest.gracefulShutdownWithInFlightBatchLosesNothing` | Drain either commits or rolls back; identity set proves zero loss in both outcomes. Component-level: `TransactedReceiveLoopTest.failureMidBatchRollsBackAllMessages`. |
| 2 | Failure during `_tmp` SequenceFile write | prod-path | `ProductionPathIntegrationTest.tmpWriteFailureRollsBackThenRedeliveryLandsEverything` | Injected serialization failure inside the real writer; `_tmp` left clean; redelivery lands every identity. |
| 3 | Failure after rename, before MQ commit | prod-path | `ProductionPathIntegrationTest.trackerFailureAfterRenameYieldsPermittedDuplicateNotLoss` | File visible, MQ rolled back → design-permitted duplicate, zero loss (§12.1). Duplicate is then classified by `PartitionReconciliationServiceTest.duplicateOrphanQuarantineIsAMoveNotADelete`. |
| 4 | Failure after MQ commit, before audit | unit | `TransactedReceiveLoopTest.auditFailureDoesNotUndoCommittedTransaction` | Commit survives an audit outage. The resulting crash window is closed by `PartitionReconciliationServiceTest.soleCopyOrphanIsKeptAndRetrospectivelyAudited`. |
| 5 | Failure between RMS tracker send and commit | unit | `TransactedReceiveLoopTest.trackerFailureRollsBackEntireBatch` | Tracker puts and source gets roll back as one unit of work. |
| 6 | Tracker queue failure | prod-path | `ProductionPathIntegrationTest.trackerFailureAfterRenameYieldsPermittedDuplicateNotLoss` | Tracker outage → rollback → recovery → exactly one tracker message per committed message. |
| 7 | Deterministic bad payload | **real-MQ** | `IbmMqFailureIntegrationTest.poisonIsolatedToBackoutQueueOnRealMq` | Real redelivery drives the bisection; 7 clean messages land, only the true poison reaches the BOQ, source drained. Embedded equivalents: `ClaimsBisectionIntegrationTest.batchOf16WithOnePoisonIsolatesItWithoutOneByOneProcessing`, `.multiplePoisonMessagesAreAllIsolatedSafely`. |
| 8 | HDFS infrastructure failure | unit + pending | `TransactedReceiveLoopTest.infrastructureExceptionDoesNotEnterDegradedMode` | Classification and rollback are automated: infrastructure failures must NOT trigger degraded mode. A true HDFS outage/failover needs a real cluster — see R2. |
| 9 | MQ reconnect / session recovery | **real-MQ** | `IbmMqFailureIntegrationTest.sessionRecoveryAfterRealChannelOutage`; `.recoveryAfterRealQueueManagerRestart` | Two outage severities. (a) `STOP CHANNEL ... MODE(FORCE)` breaks a live loop; `MQRC_CONNECTION_BROKEN` detected, Session+Consumer rebuilt from the same `Connection`, processing resumes. (b) Full queue-manager restart (`docker restart`, QM process down ~12s): the loop recovers *and* the batch that was uncommitted when the QM went down is replayed rather than lost. Both end with health back at HEALTHY. Component-level: `TransactedReceiveLoopTest.sessionRecoveryExposesReconnectCount`. |
| 10 | Graceful shutdown with in-flight batch | prod-path | `ProductionPathIntegrationTest.gracefulShutdownWithInFlightBatchLosesNothing` | Bounded drain; an uncommittable batch is never force-renamed. |
| 11 | Claims poison isolation / bisection | **real-MQ** | `IbmMqFailureIntegrationTest.poisonIsolatedToBackoutQueueOnRealMq`; `ClaimsBisectionIntegrationTest` (4 tests) | Suspect-tracked bisection isolates the poison in fewer than N transactions. The BOTHRESH/BISECT interplay rule (`backout_threshold ≥ ceil(log2(batch_size)) + 1`) is enforced by `BindingConfigValidator` and covered by `ClaimsBisectionIntegrationTest.restoreIsBlockedWhileSuspectsOutstanding` / `.suspectTrackingIsIdBased`. |
| 12 | Multiple listeners, redelivery to a different thread | prod-path | `ProductionPathIntegrationTest.multipleListenersRedeliveryLandsAllMessages` | Identity set proves zero loss across threads. Two-listener variant in `ClaimsBisectionIntegrationTest.batchOf16WithOnePoisonIsolatesItWithoutOneByOneProcessing`. |
| 13 | Binding isolation (RMS vs Claims) | prod-path | `ProductionPathIntegrationTest.failingBindingDoesNotAffectHealthyBinding` | Failing binding goes DEGRADED and retains its messages; the healthy binding lands everything. |

## Contracts the design depends on

Not §15 scenarios, but assumptions that would silently invalidate the design if
untrue. Each was previously assumed and is now verified on the product.

| Contract | Assurance | Test | Why it matters |
|---|---|---|---|
| Queue manager increments `JMSXDeliveryCount` across rollbacks | **real-MQ** | `IbmMqFailureIntegrationTest.deliveryCountAccumulatesAcrossRollbacksOnRealMq` | It is the sole input to poison detection. Unit tests could only assert the property *name* was spelled correctly, since ActiveMQ does not reproduce the semantics. |
| `JMSMessageID` is stable across redelivery | **real-MQ** | `IbmMqFailureIntegrationTest.messageIdIsStableAcrossRedeliveryOnRealMq` | The bisection coordinator tracks suspects by message id across rollback and redelivery to any listener thread. If the id changed, suspect tracking would silently never converge. |
| A `Connection` survives an outage well enough to create a new `Session` | **real-MQ** | `IbmMqFailureIntegrationTest.sessionRecoveryAfterRealChannelOutage`; `.recoveryAfterRealQueueManagerRestart` | `recoverSession()` reuses the injected `Connection` rather than rebuilding it, and `MqConnectionManager` caches connections with no invalidation path — so if the object were dead after an outage the loop could never recover. Verified across both a channel bounce and a full QM restart: the IBM MQ client re-establishes lazily on `createSession()`, even though `WMQ_CLIENT_RECONNECT` is not enabled. |
| An uncommitted batch is replayed, not lost, when the QM restarts | **real-MQ** | `IbmMqFailureIntegrationTest.recoveryAfterRealQueueManagerRestart` | Messages consumed into an in-flight transaction when the QM goes down are rolled back by the QM and redelivered. The test holds three messages uncommitted (batch size 5, long flush interval), restarts the QM, then completes the batch — all five land. |
| Basic MQ connectivity and transaction semantics | **real-MQ** | `IbmMqIntegrationTest` — `connectToMq`, `sendAndReceiveTransacted`, `rollbackLeavesMessage`, `mqConnectionManagerWorks`, `batchReceiveAndCommit` | Rollback returning the message to the queue is the foundation of at-least-once delivery. |

## Application-context coverage

| Scope | Assurance | Test | Notes |
|---|---|---|---|
| Full Spring context, RMS | prod-path | `RmsApplicationSpringBootTest.rmsProductionPathLandsMessagesAndSendsTrackers` | Boots the real `RmsApplication`: startup validation → land → tracker → audit → actuator health UP. Only the MQ connection is substituted. |
| Full Spring context, Claims | prod-path | `ClaimsApplicationSpringBootTest.claimsProductionPathLandsMessagesWithoutTracker` | Boots the real `ClaimsApplication`: LAND_ONLY, no tracker producer anywhere, audit, health UP. |

## R. Still requires a real environment

Cannot be proven with embedded substitutes; required before production cutover.

1. **HDFS NameNode failover / DataNode loss** during write, close, and rename —
   including behaviour on erasure-coded paths.
2. **Kerberos ticket expiry and renewal** under load (`KerberosManager` relogin).
3. **BOTHRESH at production batch sizes** — the Docker drill runs at batch 8 /
   threshold 4; claims production is batch 8000 / threshold 14.
4. **Crash-kill (-9)** after rename before commit, and after commit before
   audit, on real infrastructure; verify reconciliation classifies the debris.
5. **Tracker consumer compatibility** — golden-master comparison of the
   rewritten `MessageHeaderDetails` against the legacy system (blocked on §20.4
   artifacts; RMS production startup is gated until then).
6. **Sustained throughput/soak** at production volumes (claims high-volume path).
7. **MQ outage variants beyond a clean restart** — the QM restart test covers a
   short (~12s), orderly stop/start with one binding and a single in-flight
   batch. A network partition (no clean FIN), a multi-minute outage that
   exhausts the 10-attempt reconnect budget, and many concurrent in-flight
   batches across bindings are all still unexercised.

## Running the real-MQ tests

`IbmMqIntegrationTest` and `IbmMqFailureIntegrationTest` are gated on the
`MQ_USER` environment variable (`@EnabledIfEnvironmentVariable`) — there is no
Maven profile. Without it they skip silently, so **a green build does not imply
they ran**. Confirm the run reports `Skipped: 0`:

```bash
docker-compose up -d ibm-mq
MQ_USER=app MQ_PASSWORD=passw0rd mvn test
```

Expected with MQ available: 465 tests, 0 failures, 0 skipped (10 of them
real-MQ). Without `MQ_USER`: the same build passes with 10 skipped and every
real-MQ assurance above degraded to untested.

Two tests additionally need the `docker` CLI and the `mq-intake-ibmmq`
container, and self-skip otherwise:

- `sessionRecoveryAfterRealChannelOutage` stops and restarts the channel. It
  does so in a `finally` block backed by an `@AfterEach` safety net — without
  that, a failing assertion leaves the channel stopped, which silently degrades
  every later MQ test to "skipped" while the build still reports success.
- `recoveryAfterRealQueueManagerRestart` restarts the whole container, so the
  queue manager is genuinely down for ~12s. It dominates the suite's runtime
  (~17s of the class's ~37s). Its waits are deliberately tight: an earlier
  version took **18 minutes** to fail when recovery was broken, because
  per-message retry windows compounded with JMS calls that block for tens of
  seconds on a dead connection. Bounded worst case is now ~2 minutes.
