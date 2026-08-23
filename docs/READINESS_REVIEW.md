# Deployment Readiness Review — datalake-mq-intake

Date: 2026-08-22. Scope: full repository against DESIGN(1).md, round 2
prompts 0–11 applied, plus real-queue-manager failure tests and the first
evidence obtained directly from the legacy MDB.
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

## Evidence from the legacy MDB — changes several assumptions

Extracts obtained from the WebSphere MDB (full detail and open questions in
`MDB_QUESTIONS.md`). Three items bear directly on readiness:

**1. The output contract is better understood, and DESIGN §9.1 is partly
wrong.** Production writes SequenceFiles with `LongWritable` keys and `Text`
values under `RECORD` compression — established by the 129-byte header length
observed on both feeds, which is a fingerprint of the key/value class names.
Our serializers declared `Text`/`BytesWritable` — a different wire format,
which would have made our files unreadable to a consumer opening them with the
production classes. **Corrected**; each module now guards the layout with
`ProductionLayoutFingerprintTest`.

DESIGN §9.1 reasons that the key is a constant carrying no information and is
therefore free to repurpose for metadata (Option A). That inference does not
hold: `129` is the header length of an empty SequenceFile, so it is simply what
record 1 always receives, and the samples were truncated to record 1. The MDB
rolls files only on partition-path change with no record cap, so files hold
many records and keys vary within a file. **Option A is unjustified** until a
consuming team confirms nothing reads the key.

**2. A prior migration on this exact path failed under load.** An attempt to
replace SequenceFile with JSONL was reverted because the WebSphere MQ listeners
came down. It wrote one file per message. This is the only real-world evidence
anyone has about how this path behaves under production volume, and it points
at file count / write latency as the failure mode.

**3. Our flush cadence was heading for the same shape.** Before this review's
changes, a fixed 30 s interval meant a quiet feed produced one file per
interval — and, because the interval was measured from batch reset rather than
from the first message, an idle gap left the next message already timed out, so
it flushed alone. At trickle volume that is one file per message: the JSONL
failure shape. Corrected in `154de2c`/`dfe402b` — see risk R-1 below.

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
| 6 | MQ reconnect recreates JMS resources | proven on real MQ, both outage severities | Bounded exponential backoff w/ jitter, non-recoverable detection, interruptible. `sessionRecoveryAfterRealChannelOutage` breaks a live loop with `STOP CHANNEL ... MODE(FORCE)`; `recoveryAfterRealQueueManagerRestart` takes the whole queue manager down (~12s) and additionally proves the batch left uncommitted is replayed, not lost. The documented residual risk — that recovery reuses the injected `Connection` — **did not materialise**: the IBM MQ client re-establishes lazily on `createSession()` across both outages, so no change to `MqConnectionManager` is needed. Remaining gaps are outage *variants*, not the mechanism — see G6 |
| 7 | Audit store concurrency-safe | proven | Immutable one-file-per-batch (`audit_{datafile}.json`); no append anywhere |
| 8 | Partition reconciliation exists | code + tests, **not scheduled** | `PartitionReconciliationService` fully tested (11 tests) but no production scheduler invokes it yet — see D3 |
| 9 | Serializers contractual or gated | gate proven; contract now partly known | `PlaceholderSerializer` marker + `SerializerValidator.validateOrFail` invoked in `IntakeRuntimeManager.start()`; production mode fails fast. MDB evidence since narrows the target: types are `LongWritable`/`Text` under `RECORD` (now matched — D1 fixed), the payload is whitespace-normalised not verbatim (now reproduced — D2 done), and metadata Option A is unjustified because the key varies per record |
| 10 | RMS tracker contract complete or gated | gated (proven) | §20.4 artifacts still missing → `RmsConfiguration.validateTrackerContract` blocks TRACKED production startup; `RmsTrackerContractGatingTest` |
| 11 | Claims identity explicit & approved | gated (proven) | `claims.identity-field` required; production fails without it; missing identity in payload fails the batch; `ClaimsIdentityGatingTest` |
| 12 | Health/metrics wired to live runtime | proven | Loop drives HEALTHY/DEGRADED/RECOVERING/UNHEALTHY; `BindingsHealthIndicator` reports per-binding via actuator; reconnect/audit/reconciliation counters exist (reconciliation counter fires only when reconciliation runs — see D3) |
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

Two further reasons, independent of the gates, that were not visible at the
last review:

- **Output is closer but not confirmed contract-compatible.** Writable types
  and payload normalisation now match production (D1, D2 done), but the key's
  value expression is still unconfirmed (MDB_QUESTIONS A3) and record metadata
  is absent pending open item #2.
- **Load behaviour is unproven** on a path where the last attempted change
  failed under load (R-1, R-2).

Removing the gates alone would therefore not make this deployable.

## D. CODE BLOCKERS
1. ~~Writable types are wrong.~~ **FIXED.** Both serializers now declare
   `LongWritable` key / `Text` value, matching production. Guarded by
   `ProductionLayoutFingerprintTest` in each module, which writes an empty
   SequenceFile from the serializer's declared classes and asserts the header
   is 129 bytes — the production fingerprint. The previous
   `Text`/`BytesWritable` gave 130.

   *This was not the independent fix it was described as here.* Metadata rode
   in the composite Text key (Option A), and a `LongWritable` key has no room
   for it, so matching the production types **removed record metadata from the
   files**. Consequences: `payload_guid` is no longer written, so
   `SequenceFileIdentityReader` returns no identities and reconciliation
   reports INCONCLUSIVE — the safe direction (INCONCLUSIVE means KEEP, never
   delete), and now logged once rather than degrading silently. Reconciliation
   cannot classify duplicates until item #2 gives metadata a home; Option C
   (sidecar) would restore it without touching these data files.
2. Production `RecordSerializer`s once metadata placement (open item #2) is
   approved — replace placeholders, drop the marker, keep value bytes
   contract-compatible.

   *Payload normalisation is now done:* `PayloadNormalizer` in core reproduces
   `processMessage` — each `\n`, `\r`, `\t` replaced by one space, runs not
   collapsed, no `trim()` — and both serializers apply it. It lives in core
   because the MDB applies it upstream of the per-feed write, so both feeds
   share it. Claims extracts identity *after* normalising, so the identity
   corresponds to what is actually written rather than to a raw form no reader
   of the file could recover.

   *Still unresolved:* the key is a positional ordinal in our implementation,
   but the live writer's exact key expression is unconfirmed (MDB_QUESTIONS
   A3) — the *type* matches production, the *value* may not.
3. Reconciliation scheduling: `PartitionReconciliationService` needs a
   production trigger (in-process scheduler or external cron) and quarantine
   policy decision. Code and tests exist; nothing invokes it in production.
4. RMS tracker rewrite completion once §20.4 artifacts arrive (tagList,
   root-end constants, golden-master fixture) — then flip the readiness flags.
   Resolve open item #26 first: if the tracker session is never committed,
   trackers may not be arriving in production today, which makes this a repair
   rather than a rewrite.

## D′. Load and file-cadence risk

**R-1 — file count at low volume (mitigated, not proven).** The one known
production failure on this path is the reverted JSONL migration, which wrote
one file per message and brought the MQ listeners down. Our flush cadence was
converging on the same shape at trickle volume. Two changes address it:

- A batch is now bounded to one partition window (`FlushTrigger.Trigger.PARTITION`,
  unconditional), matching the legacy writer's roll-on-path-change behaviour.
- `batch_interval_ms` now measures from the batch's first message rather than
  from reset, and is set to `0` (disabled) in both applications, leaving
  size, bytes and the partition boundary as the triggers.

A quiet quarter-hour window therefore produces exactly one file, which is the
legacy cadence. It is **not possible to configure an unbounded batch** — the
partition trigger is unconditional. Cost: a message may wait until the end of
its window, worst case ~15 minutes, against 30 s previously. Raise
`batch_interval_ms` if a feed has a tighter freshness SLA.

Mitigated in design, **not proven under load** — see the scale gap below.

**R-2 — scale is untested by roughly three orders of magnitude.** The largest
volume ever pushed through the production path in a test is 12 messages.
Production is batch 8000 × 4 threads (claims) and 4000 × 4 (RMS). The acute
case is bisection: isolating one poison at batch 8000 takes ~13 halvings, each
rolling back up to 8000 messages, with `backout_threshold` 14. Whether that
converges acceptably on the high-volume feed — the exact scenario BISECT was
chosen for — is unmeasured. Given R-1's precedent, load behaviour on this path
should not be assumed from unit-scale tests.

## E. ENVIRONMENT/PLATFORM DEPENDENCIES
- IBM MQ: BOTHRESH/BOQNAME configured per queue to match app thresholds
  (claims BISECT requires BOTHRESH ≥ 14 for batch 8000); MAXUMSGS ≥ 2×batch.
- HDFS: binding base paths + `_tmp` + audit base pre-created and writable;
  RECORD/NONE compression assumption on erasure-coded paths.
- Kerberos: principal/keytab; renewal interval tuning.
- Credentials: `MQ_CREDENTIAL_REF`/environment-based; no secrets in repo.

## F. OPEN BUSINESS/CONTRACT DECISIONS
1. Metadata placement (§9.1, open item #2) — blocks serializers. **The
   decision has changed shape:** Option A rested on the key being a constant,
   which the MDB disproves. The question is now simply *does any consumer read
   the SequenceFile key?* If yes, Option A is out and Option C (sidecar file,
   data files byte-identical) is the only route that preserves parity while
   still carrying the metadata reconciliation needs.
2. Claims stable identity (§9.2, open item #17) — blocks claims
   reconciliation readiness (`CLM_XMITSN_ID` vs `REC_CTL_NBR` vs wrapper).
   **Scope-defining:** if the MDB extracts no payload identity and performs no
   dedup, this field exists solely to serve reconciliation — a capability the
   MDB does not have. Dropping reconciliation removes this gate entirely.
3. Legacy tracker rewrite artifacts (§20.4) — blocks RMS TRACKED production.
   Gate open item #26 first (is the tracker session ever committed?).
4. Quarantine/retention policy for reconciliation duplicates.
5. **Freshness SLA per feed.** `batch_interval_ms` is now 0, so a message may
   wait until its partition window closes (~15 min worst case). Confirm both
   feeds tolerate that; it is the price of the legacy file cadence (R-1).

## G. REQUIRED REAL-ENVIRONMENT TESTS

Automated against a real queue manager (`IbmMqFailureIntegrationTest`, run
with `MQ_USER` set against the Docker IBM MQ container):

- **Connection loss and session recovery** — channel stopped under a running
  loop; `MQRC_CONNECTION_BROKEN` detected, session rebuilt, processing
  resumes, health returns to HEALTHY.
- **Full queue-manager restart** — QM process down ~12s; the loop recovers and
  the batch that was uncommitted at the moment of failure is replayed rather
  than lost. Also settles the open question about `Connection` reuse: the
  injected object stays usable, so the loop's recovery design is sound as
  written.
- **Real delivery-count accumulation** — the queue manager increments
  `JMSXDeliveryCount` across rollbacks, which is what drives poison
  detection. Previously only the property *name* was asserted.
- **Real redelivery identity stability** — `JMSMessageID` unchanged across
  redelivery, the premise the bisection coordinator depends on.
- **Poison drill with real BOQ** — full production path with real redelivery
  driving bisection; clean messages land, only the poison reaches the BOQ.

Still required before production cutover:

1. HDFS NameNode failover / DataNode loss during write, close, and rename.
2. Kerberos ticket expiry + renewal under load.
3. Crash-kill (-9) after rename before commit, and after commit before audit,
   on real infrastructure; verify reconciliation classifies the debris.
4. Tracker consumer golden-master comparison (legacy vs rewritten headers).
5. **Sustained throughput/soak at production volumes — now the highest-value
   outstanding test.** The claims high-volume path, including BOTHRESH
   behaviour at the real batch size and bisection convergence at batch 8000.
   Priority is raised because the one known production failure on this path
   (the reverted JSONL migration, R-1) was load-related, and because our own
   coverage tops out at 12 messages (R-2). Measure file count per partition
   per feed at both peak and trickle rates, to confirm R-1's mitigation holds
   in practice rather than only in design.
6. MQ outage *variants* beyond the clean restart now covered: a network
   partition with no clean FIN, an outage long enough to exhaust the
   10-attempt reconnect budget, and many concurrent in-flight batches across
   bindings. The recovery mechanism is proven; its envelope is not.

## Survivability summary (production gate criteria)

| Event | Design answer | Assurance today |
|---|---|---|
| MQ connection loss | session recovery + redelivery | **proven on real MQ** (channel outage) |
| MQ queue-manager restart | session recovery + uncommitted batch replay | **proven on real MQ** (QM down ~12s, zero loss); longer/dirtier outages still open (G6) |
| HDFS failover | rollback + redelivery; rename atomicity | classification tested; real-cluster proof pending (G1) |
| Kerberos renewal | KerberosManager relogin | code + unit tests; real proof pending (G2) |
| Poison message | BOTHRESH + BOQ on same transaction; bisection | **proven on real MQ** (real delivery counting + real BOQ) |
| Tracker outage | full rollback; permitted duplicate after rename | proven (embedded) |
| Crash after rename, before commit | redelivery duplicate; reconciliation flags it | proven (embedded + reconciliation tests) |
| Crash after commit, before audit | file kept; retrospective audit | proven (embedded + reconciliation tests) |
| Production message volume | batching bounds file count; partition-aligned flush | **not proven** — 12 messages max in test vs batch 8000 (R-2). The comparable prior attempt failed here (R-1) |
| Quiet-period file cadence | one file per partition window | mitigated in design and unit-tested; unmeasured against a real feed (R-1) |
