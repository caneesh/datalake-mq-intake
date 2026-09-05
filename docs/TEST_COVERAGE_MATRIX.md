# DESIGN §15 Test Coverage Matrix

Maps each §15 failure scenario to the automated test that proves it, using the
same four assurance levels as `READINESS_REVIEW.md`:

| Level | Meaning |
|---|---|
| **real-MQ** | Exercised on the production path against an actual IBM MQ queue manager |
| **prod-path** | Exercised on the production path (real `BindingRuntimeFactory` → `BindingRuntime` → `TransactedReceiveLoop` → `BatchAccumulator` → `BatchTransactionProcessor` → `SequenceFileBatchWriter` → JMS transaction → tracker → audit → health), with embedded ActiveMQ standing in for MQ |
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
| 3 | Failure after rename, before MQ commit | prod-path | `ProductionPathIntegrationTest.failureAfterRenameBeforeCommitYieldsPermittedDuplicateNotLoss` | File visible, MQ rolled back → design-permitted duplicate, zero loss (§12.1). Duplicate is then classified by `PartitionReconciliationServiceTest.duplicateOrphanQuarantineIsAMoveNotADelete`. |
| 4 | Failure after MQ commit, before audit | unit | `TransactedReceiveLoopTest.auditFailureRollsBackSoNoUnauditedDataIsCommitted`; `.auditFailureCanBeConfiguredToCommitAnyway` | Commit survives an audit outage. The resulting crash window is closed by `PartitionReconciliationServiceTest.soleCopyOrphanIsKeptAndRetrospectivelyAudited`. |
| 5 | Failure between RMS tracker send and commit | unit | `TransactedReceiveLoopTest.trackerContentFailureRollsBackWhenConfiguredToFailTheBatch`; `.trackerContentFailureDoesNotRollBackByDefault`; `.trackerPutFailureRollsBackEvenByDefault` | Now two FAILURE KINDS, not two policies. A content failure (the payload breaks the header rewrite) is logged, counted and committed — MDB parity — unless `fail_batch_on_tracker_error` escalates it. A put failure (`JMSException`: queue full, message too big, broken producer) rolls back regardless of that flag, because it will refuse the next message too and the alternative is landing every message with its acknowledgement dropped. |
| 6 | Tracker queue failure | prod-path | `ProductionPathIntegrationTest.trackerContentFailureLosesOnlyTheNotificationNotTheData` | **Default policy now matches the legacy MDB:** the failure is logged and counted, the batch still commits, so data lands once and only that notification is lost — no rollback, no duplicate. The stricter behaviour is retained under `fail_batch_on_tracker_error` and covers scenario 3. |
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
| Backout-depth gauge wiring | prod-path | `ClaimsApplicationSpringBootTest.backoutQueueDepthGaugeIsPopulatedThroughTheRealWiring` | Proves the whole chain through the real context: the factory built a monitor, the runtime started it, an empty queue reads as an observed zero, a message put on the BOQ moves the gauge, and sampling did not consume it. Verified to fail when the factory is unwired. |

## Regression tests for review findings

Added with the fixes for the six defects in `READINESS_REVIEW.md` §D″. Each was
run against the unfixed code first and confirmed to fail — a passing test that
was never seen to fail proves nothing about the bug it claims to cover.

| Defect | Test | Observed failure before the fix |
|---|---|---|
| Post-commit bookkeeping triggered a rollback and marked committed messages suspect, wedging the binding in degraded mode | `TransactedReceiveLoopTest.postCommitBookkeepingFailureDoesNotRollBackOrMarkSuspect` | `rollbackCount` expected 0, was 1 |
| A data failure whose message mentioned "shutdown" classified as SHUTDOWN, so the poison was never isolated | `FailureClassifierTest.serializationFailureMentioningShutdownIsStillADataFailure` | expected `MESSAGE_DATA`, was `SHUTDOWN` |
| Genuine interrupts must still classify as SHUTDOWN after the reordering | `FailureClassifierTest.genuineInterruptionIsStillClassifiedAsShutdown` | (guards the fix, not a bug) |
| An infrastructure blip reset progress toward leaving degraded mode | `DegradedModeManagerTest.infrastructureBlipDoesNotDiscardProgressTowardRestore` | expected not degraded, was degraded |
| Suspects were registered after the degraded-mode flip, letting a concurrent success restore full batch size with the poison in flight | `DegradedModeManagerTest.suspectsAreRegisteredBeforeDegradedModeBecomesVisible` | — |
| `batch_bytes` counted UTF-16 code units, not the UTF-8 bytes actually written | `FlushTriggerTest.byteTriggerCountsUtf8BytesNotUtf16CodeUnits`, `.utf8ByteCountHandlesSurrogatePairs` | — |

## Defects found by measuring coverage before refactoring

Four refactors were proposed in review. Each began by mutating the existing
code to find out which invariants the suite actually held, before any code
moved. Three of the four turned up a real defect — none of them was the
refactor, and none would have been visible from reading the class.

The fourth (`IntakeRuntimeManager`) turned up no defect: the code was correct,
but two of its invariants were held by nothing, which is a different problem
with the same cause and is recorded under the lifecycle decomposition below.
Measuring first is worth doing for that outcome too — a clean probe result is
evidence, where a green suite on its own is not.

| Defect | Found by | Test | Observed before the fix |
|---|---|---|---|
| Message identifiers could stop being collected entirely, silently disabling suspect tracking and with it poison isolation | mutating `TransactedReceiveLoop` before decomposing it | `LoopInvariantCharacterisationTest.aFailedBatchMarksTheIdentifiersItActuallyConsumed` | 67 tests passed with collection removed |
| The shutdown drain could commit with the thread still marked interrupted, rolling back the final batch on every clean shutdown | same | `.theShutdownDrainCommitsWithTheInterruptFlagCleared` | 67 tests passed with the clear removed |
| An unwritable landing path passed startup validation, so the service would start, report healthy, and stall on its first batch | mutating `StartupValidator` before splitting it | `StartupValidatorTest.aLandingPathThatExistsButIsNotWritableFailsStartup` | removing `fileSystem.access(path, WRITE)` changed no test result |
| An incomplete audit scan authorised quarantine, so a correctly-audited file could be MOVED because its audit record was corrupt | testing a review's claim about `PartitionReconciliationService` | `PartitionReconciliationServiceTest.anIncompleteAuditScanMustNotAuthoriseQuarantine` | the file was moved |

The last is the one to remember: reporting a condition and refusing to act on it
are different guarantees. An earlier fix made the corrupt-audit case VISIBLE —
`UNREADABLE_AUDIT`, `retryLater` — and left it authorised. Withholding the
irreversible half needed its own change.

**Two probe failures worth copying as method.** A probe that renames a method
breaks compilation, and a probe whose target string appears twice silently does
not apply. Both report "no failures", which means *the tests never ran against a
mutation* rather than *the mutation is covered*. Always confirm a probe compiled
and applied before believing a green result.

## Staging area reclamation

Destructive: `StagingAreaReclaimer` deletes files. Both of its safety conditions
were verified to fail independently, which is what makes the pair safe rather
than merely likely to be safe.

| Rule | Test | Mutation that breaks it |
|---|---|---|
| A live peer's staging files are never touched | `AbandonedInstanceReclamationTest.aLivePeersFilesAreNeverTouched` | ignoring the lease check |
| A recent file survives even when the lease looks stale | `.aRecentFileSurvivesEvenWhenTheLeaseLooksStale` | ignoring file age inside a reclaimed directory |
| The own-directory sweep never reaches a sibling | `.ownInstanceCleanupTouchesOnlyItsOwnDirectory` | — |
| A running instance never reclaims its own directory | `.theRunningInstanceNeverReclaimsItsOwnDirectory` | — |

## Verifying the receive loop's decomposition

`TransactedReceiveLoop` was split into `BatchAccumulator`,
`BatchTransactionProcessor`, `SessionRecoveryCoordinator` and
`LoopStateReporter`. The suite alone was **measured to be insufficient** to
confirm that split preserved behaviour, so the evidence for it is a different
kind and is recorded here rather than inferred from a green build.

Before the extraction, deliberately breaking the loop showed which invariants
the suite actually held:

| Mutation | Before the characterisation tests |
|---|---|
| `committed` flag set one line later | caught (1 failure) |
| tracker send moved after the audit | caught (8 failures) |
| message identifiers never collected | **not caught** — 67 tests passed |
| drain commits with the interrupt still set | **not caught** — 67 tests passed |

`LoopInvariantCharacterisationTest` closes those, and every test in it was
verified to FAIL against the specific mutation it names. It pins behaviour, not
structure, which is what let it survive the extraction unchanged:

| Invariant | Pinned by |
|---|---|
| identifiers collected before anything is sent | `aCommittedBatchClearsTheIdentifiersItActuallyConsumed`, `aFailedBatchMarksTheIdentifiersItActuallyConsumed`, `identifiersSurviveARoutedPoisonMessageInTheSameBatch` |
| drain clears the interrupt before committing | `theShutdownDrainCommitsWithTheInterruptFlagCleared` |
| post-commit failure never marks a suspect | `aFailureAfterTheCommitIsNeverMarkedSuspect` |
| balance checked before tracker send and audit | `anUnbalancedBatchSendsNoTrackerMessageAndWritesNoAudit` |
| a flush leaves no accumulator state | `eachFlushStartsFromAnEmptyBatch` |
| recovery budget resets on success | `SessionRecoveryTest.aBrokenSessionIsRecoveredAndConsumptionResumes` (pre-existing) |

**Changing any of these classes warrants re-running the mutations, not just the
suite.** Three of the five now mutate an extracted class and are still caught by
tests written against the loop, which is the property that makes the
decomposition checkable at all.


Two fixes are not directly covered and rely on the existing suite plus
inspection: clearing the thread interrupt before the shutdown drain (it fails
only against a real IBM MQ client that honours the flag, which the embedded
broker does not), and temp-file cleanup when `rename()` returns false (HDFS
returning false rather than throwing is not reproducible against the local
filesystem). Both are called out in §D″ / R.

## Verifying the lifecycle root's decomposition

`IntakeRuntimeManager` owned validation, the instance lease, staging cleanup,
component construction, binding startup and rollback, reconciliation, shutdown,
metrics and health. Review proposed splitting it. The same measure-first step
ran again, and this time found no defect — but found two invariants the suite
did not hold, both of them code added earlier in the same work, and both of them
exactly what the proposed split would move:

| Mutation | Before the characterisation tests |
|---|---|
| rollback after a failed start does nothing | caught (1 failure) |
| lease taken AFTER the staging sweep instead of before | **not caught** |
| lease never released on clean shutdown | **not caught** |

`InstanceLeaseLifecycleTest` closes both, and each test was verified to fail
against the mutation it names, before and again after the code moved:

| Invariant | Pinned by | Why it matters |
|---|---|---|
| the lease is written before any staging directory is swept | `theLeaseIsWrittenBeforeAnyStagingDirectoryIsSwept` | two instances starting at the same moment must each see the other's claim before either decides a directory is abandoned |
| the lease is held while running and released on clean shutdown | `theLeaseIsHeldWhileRunningAndReleasedOnCleanShutdown` | releasing it is what lets an ordinary restart reclaim its predecessor's directory immediately instead of waiting out the lease timeout |

Both assert on **filesystem calls** — the lease file is created before any
staging root is listed — rather than on the manager's internals. That is the
observable form of the invariant, and it is why both tests survived the two
subsequent commits unchanged while the code they cover moved to another class.

The split itself:

| Change | Effect |
|---|---|
| `initializeRuntimeFactory()` / `setRuntimeFactoryForTest()` / `startReconciliation()` replaced by injected suppliers | `PartialStartupRollbackTest` no longer subclasses the production lifecycle root (it did three times); the `@Autowired` constructor Spring uses is unchanged |
| `StagingLifecycleManager` extracted | claim-then-sweep ordering was a comment between two adjacent calls in a sixty-line `start()`; it is now `claim()`, the only way to call the class |
| `BindingRuntimeRegistry` **not** extracted | it would be a Map, three `clear()` calls, two iterations and two accessors — a hop with no decision in it |

Post-extraction probes, each caught by the test that names it:

| Mutation | Result |
|---|---|
| sweep before the claim instead of after | 1 failure |
| `close()` drops the lease reference without releasing it | 1 failure |
| shutdown never releases the staging claim | 1 failure |
| late-startup failure leaves the bindings running | 1 failure |

## Backout-depth monitoring

`BackoutQueueDepthMonitor` feeds the gauge DESIGN §14 nominates as the pager
condition. Component coverage in `BackoutQueueDepthMonitorTest` (embedded
ActiveMQ, so the browse is real):

| Behaviour | Test |
|---|---|
| Empty queue reads as an observed zero | `emptyBackoutQueueReportsZero` |
| Depth reflects what is on the queue | `depthReflectsMessagesOnTheBackoutQueue` |
| **Sampling does not consume what it counts** — otherwise monitoring would destroy the messages an operator was paged to inspect | `samplingDoesNotConsumeTheMessagesItCounts` |
| Depth falls back to zero once the queue is cleared | `depthReturnsToZeroAfterTheQueueIsCleared` |
| Count is capped so a deep queue cannot cost an unbounded enumeration | `countIsCappedSoADeepQueueCannotCostAnUnboundedEnumeration` |
| **A failed sample leaves the gauge alone rather than zeroing it** — zeroing on error would suppress a page exactly when visibility was lost | `aFailedSampleLeavesTheGaugeAloneRatherThanZeroingIt` |
| The scheduler actually drives sampling | `scheduledMonitorPopulatesTheGaugeWithoutBeingDrivenByHand` |
| A non-positive interval disables it (and its alert) | `nonPositivePollIntervalDisablesTheMonitor` |

Not covered here: behaviour against a real IBM MQ queue manager. Browsing
semantics are standard JMS and ActiveMQ implements them faithfully, but depth
sampling under a real BOTHRESH-driven routing event is part of R-3.

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

Expected with MQ available: 526 tests, 0 failures, 0 skipped (10 of them
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
