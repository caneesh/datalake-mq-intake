# Deployment Readiness Review — datalake-mq-intake

Date: 2026-08-22. Scope: full repository against DESIGN(1).md, round 2
prompts 0–11 applied, plus real-queue-manager failure tests.
This review distinguishes four levels of assurance:

- **code exists** — the class is in the repository
- **runtime invokes it** — the production call chain actually reaches it
- **proven** — an automated test exercises it on the production path
- **proven on real MQ** — exercised against an actual IBM MQ queue manager,
  not an embedded broker standing in for one

The last distinction matters because embedded ActiveMQ cannot reproduce the
queue-manager behaviour poison handling depends on: delivery-count
accumulation across rollbacks, message-identity stability across redelivery,
and real connection-loss error codes. Those are now covered by
`IbmMqFailureIntegrationTest`, which is skipped unless `MQ_USER` is set — so
a green build without it proves less than a green build with it.

## Production call chains (traced)

**RMS (TRACKED)** — proven end-to-end by `RmsApplicationSpringBootTest`
(embedded broker standing in for IBM MQ):

```
RmsApplication (@SpringBootTest boots real context)
→ BindingConfigValidator.validate (incl. BISECT/BOTHRESH rule)
→ SerializerValidator (placeholder gate, MQ_INTAKE_PRODUCTION)
→ RmsConfiguration.validateTrackerContract (§20.4 gate)
→ HdfsConfiguration: hadoopConfiguration → KerberosManager (disabled) → FileSystem → HdfsPathValidator bean
→ MqConfiguration → MqConnectionManager (named connections, bounded reconnect)
→ IntakeRuntimeManager (SmartLifecycle) → StartupValidator (path writability, _tmp sweep)
→ BindingRuntimeFactory → BindingRuntime → N × TransactedReceiveLoop
→ per-thread transacted Session → receive(timeout) → poison check (BOTHRESH, same session)
→ RmsRecordSerializer → SequenceFileBatchWriter: _tmp write → close → atomic rename
→ tracker send (same session) → session.commit
→ immutable per-batch audit write → BindingHealthManager / BindingMetrics
→ BindingsHealthIndicator (/actuator/health)
```

**Claims (LAND_ONLY)** — proven by `ClaimsApplicationSpringBootTest`: same
chain minus tracker send, with BISECT + suspect-tracking poison isolation
(proven by `ClaimsBisectionIntegrationTest`) and explicit-identity gating.

## Previous blockers — verification

| # | Blocker | Level reached | Evidence |
|---|---------|---------------|----------|
| 1 | HdfsPathValidator production bean | proven | Was silently missing (`@ConditionalOnBean` on a scanned component never matched); now an explicit `@Bean` in HdfsConfiguration, exercised by both SpringBootTests |
| 2 | Complete startup validation runs | proven | BindingConfigValidator + SerializerValidator + tracker/identity gates + StartupValidator all on `IntakeRuntimeManager.start()` path |
| 3 | BOQ failure always causes rollback | proven | `BackoutFailureException` → rollback; `PoisonMessageHandlerTest.throwsBackoutFailureExceptionWhenRoutingFails` |
| 4 | Degradation state binding-scoped | proven | One `DegradedModeManager` shared by all loops; bisection test uses two listeners |
| 5 | Claims bisection real & redistribution-safe | proven, incl. real MQ | Suspect message-ID tracking survives redelivery to any thread; `ClaimsBisectionIntegrationTest` (16 msgs, 1–2 poisons, < 16 transactions, only poison in BOQ); validator enforces `backout_threshold ≥ ceil(log2(batch_size))+1`. The premise — that `JMSMessageID` is stable across redelivery — was an assumption, now verified on the product by `messageIdIsStableAcrossRedeliveryOnRealMq`, and the whole isolation runs against a real queue manager in `poisonIsolatedToBackoutQueueOnRealMq` |
| 6 | MQ reconnect recreates JMS resources | proven for channel outage; QM restart still pending | Bounded exponential backoff w/ jitter, non-recoverable detection, interruptible. `sessionRecoveryAfterRealChannelOutage` breaks a live loop with `STOP CHANNEL ... MODE(FORCE)`, and the loop detects `MQRC_CONNECTION_BROKEN`, rebuilds Session+Consumer **from the same Connection object**, and resumes. A full queue-manager restart is a stronger event and remains untested — see G1 |
| 7 | Audit store concurrency-safe | proven | Immutable one-file-per-batch (`audit_{datafile}.json`); no append anywhere |
| 8 | Partition reconciliation exists | code + tests, **not scheduled** | `PartitionReconciliationService` fully tested (11 tests) but no production scheduler invokes it yet — see D2 |
| 9 | Serializers contractual or gated | proven | `PlaceholderSerializer` marker + `SerializerValidator.validateOrFail` now invoked in `IntakeRuntimeManager.start()`; production mode fails fast |
| 10 | RMS tracker contract complete or gated | gated (proven) | §20.4 artifacts still missing → `RmsConfiguration.validateTrackerContract` blocks TRACKED production startup; `RmsTrackerContractGatingTest` |
| 11 | Claims identity explicit & approved | gated (proven) | `claims.identity-field` required; production fails without it; missing identity in payload fails the batch; `ClaimsIdentityGatingTest` |
| 12 | Health/metrics wired to live runtime | proven | Loop drives HEALTHY/DEGRADED/RECOVERING/UNHEALTHY; `BindingsHealthIndicator` reports per-binding via actuator; reconnect/audit/reconciliation counters exist (reconciliation counter fires only when reconciliation runs — see D2) |
| 13 | One proven shutdown path | proven | `GracefulShutdownHandler` (unused duplicate) **removed**; the single path is SmartLifecycle → `IntakeRuntimeManager.stop()` → bounded drain → commit-or-rollback, proven by `gracefulShutdownWithInFlightBatchLosesNothing` |
| 14 | Real production-path failure tests | proven | `ProductionPathIntegrationTest` (6), `ClaimsBisectionIntegrationTest` (4), 2 × `@SpringBootTest`, plus `IbmMqFailureIntegrationTest` (4) against a real queue manager; matrix in `docs/TEST_COVERAGE_MATRIX.md` |

## Repository scan classification

- `TODO`/`FIXME` (7, all in `RmsTrackerMessageBuilder`): the §20.4 legacy
  tracker artifacts — intentional, production-gated, not silent.
- `PLACEHOLDER`/`NON-CONTRACTUAL` (RMS/Claims serializers, marker interface,
  gates): intentional and enforced at startup, not silent.
- `assertTrue(true)`: none. `UnsupportedOperationException`: none in main.
- Hardcoded passwords: none (credentials via `CredentialProvider`/env).
- Hardcoded queues/paths: only in `application.yml` defaults (overridable),
  none in code.
- Silent fallbacks: none found; every fallback (mq_message_id identity,
  fixture extractor) logs loudly and/or is production-gated.
- Test stub in production sources (`CountingBatchWriter`): moved to test tree.

## A. READY FOR DEV INTEGRATION
Both applications: full pipeline, poison handling, bisection, health,
metrics, audit, shutdown — proven on embedded infrastructure.

## B. READY FOR TEST/UAT
Ready, with real IBM MQ + HDFS + Kerberos, provided the environment tests in
G are executed there. Docker IBM MQ profile exists for connectivity checks.

## C. READY FOR PRODUCTION
**No.** Production startup is intentionally impossible today:
- RMS: tracker contract gate (§20.4) fails TRACKED startup.
- Claims: identity gate fails startup until `claims.identity-field` is set to
  an approved field; serializer placeholder gate fails both apps under
  `MQ_INTAKE_PRODUCTION=true`.
This is by design — the gates convert unresolved contracts into fail-fast
instead of silent wrong output.

## D. CODE BLOCKERS
1. Production `RecordSerializer`s once metadata placement (open item #2) is
   approved — replace placeholders, drop the marker, keep value bytes
   contract-compatible.
2. Reconciliation scheduling: `PartitionReconciliationService` needs a
   production trigger (in-process scheduler or external cron) and quarantine
   policy decision. Code and tests exist; nothing invokes it in production.
3. RMS tracker rewrite completion once §20.4 artifacts arrive (tagList,
   root-end constants, golden-master fixture) — then flip the readiness flags.

## E. ENVIRONMENT/PLATFORM DEPENDENCIES
- IBM MQ: BOTHRESH/BOQNAME configured per queue to match app thresholds
  (claims BISECT requires BOTHRESH ≥ 14 for batch 8000); MAXUMSGS ≥ 2×batch.
- HDFS: binding base paths + `_tmp` + audit base pre-created and writable;
  RECORD/NONE compression assumption on erasure-coded paths.
- Kerberos: principal/keytab; renewal interval tuning.
- Credentials: `MQ_CREDENTIAL_REF`/environment-based; no secrets in repo.

## F. OPEN BUSINESS/CONTRACT DECISIONS
1. Metadata placement Option A/B (§9.1, open item #2) — blocks serializers.
2. Claims stable identity (§9.2, open item #17) — blocks claims
   reconciliation readiness (`CLM_XMITSN_ID` vs `REC_CTL_NBR` vs wrapper).
3. Legacy tracker rewrite artifacts (§20.4) — blocks RMS TRACKED production.
4. Quarantine/retention policy for reconciliation duplicates.

## G. REQUIRED REAL-ENVIRONMENT TESTS

Automated against a real queue manager (`IbmMqFailureIntegrationTest`, run
with `MQ_USER` set against the Docker IBM MQ container):

- **Connection loss and session recovery** — channel stopped under a running
  loop; `MQRC_CONNECTION_BROKEN` detected, session rebuilt, processing
  resumes, health returns to HEALTHY.
- **Real delivery-count accumulation** — the queue manager increments
  `JMSXDeliveryCount` across rollbacks, which is what drives poison
  detection. Previously only the property *name* was asserted.
- **Real redelivery identity stability** — `JMSMessageID` unchanged across
  redelivery, the premise the bisection coordinator depends on.
- **Poison drill with real BOQ** — full production path with real redelivery
  driving bisection; clean messages land, only the poison reaches the BOQ.

Still required before production cutover:

1. IBM MQ **queue-manager restart** mid-stream (`endmqm`/`strmqm`, not just a
   channel stop): uncommitted batch replay and real MQRC handling while the
   QM is fully down. Residual risk: recovery reuses the injected `Connection`
   object, which survives a channel bounce but has not been shown to survive
   a QM restart — if it does not, `MqConnectionManager` must hand the loop a
   fresh Connection.
2. HDFS NameNode failover / DataNode loss during write, close, and rename.
3. Kerberos ticket expiry + renewal under load.
4. Crash-kill (-9) after rename before commit, and after commit before audit,
   on real infrastructure; verify reconciliation classifies the debris.
5. Tracker consumer golden-master comparison (legacy vs rewritten headers).
6. Sustained throughput/soak at production volumes (claims high-volume path),
   including BOTHRESH behaviour at the real claims batch size.

## Survivability summary (production gate criteria)

| Event | Design answer | Assurance today |
|---|---|---|
| MQ connection loss | session recovery + redelivery | **proven on real MQ** (channel outage) |
| MQ queue-manager restart | session recovery + redelivery | still pending (G1); stronger event than a channel bounce |
| HDFS failover | rollback + redelivery; rename atomicity | classification tested; real-cluster proof pending (G2) |
| Kerberos renewal | KerberosManager relogin | code + unit tests; real proof pending (G3) |
| Poison message | BOTHRESH + BOQ on same transaction; bisection | **proven on real MQ** (real delivery counting + real BOQ) |
| Tracker outage | full rollback; permitted duplicate after rename | proven (embedded) |
| Crash after rename, before commit | redelivery duplicate; reconciliation flags it | proven (embedded + reconciliation tests) |
| Crash after commit, before audit | file kept; retrospective audit | proven (embedded + reconciliation tests) |
