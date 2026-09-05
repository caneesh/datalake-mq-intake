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

**2a. RETRACTED — there is no durability window in the legacy system.** An
earlier revision of this review reasoned that CMT plus an always-open file
meant MQ acknowledged messages whose data was still unflushed. That was wrong:
`HDFSWriter.write(...)` calls `sequenceFileWriter.hsync()` after **every**
append, so each record is durable before `onMessage()` returns. The inference
was sound given what was known; the fact was missing.

What does differ is **visibility, not durability**. The MDB appends into an
open file already sitting in the partition directory, so a reader scanning that
partition can observe a partially written file. Our `_tmp` → close → rename
makes a file visible only once complete. That remains a deliberate divergence
from parity, mandated by the standing constraint that nothing is acknowledged
before HDFS visibility.

*Carry into sizing:* `hsync()` per message is a datanode round trip on every
record. Our design syncs once per batch and then closes, so it should perform
far fewer — a point in favour of batching that is independent of file count.

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
→ SerializerValidator (placeholder gate, via ProductionMode: prod/production profile OR MQ_INTAKE_PRODUCTION)
→ RmsConfiguration.validateTrackerContract (§20.4 gate)
→ HdfsConfiguration: hadoopConfiguration → KerberosManager (disabled) → FileSystem → HdfsPathValidator bean
→ MqConfiguration → MqConnectionManager (named connections, bounded reconnect)
→ IntakeRuntimeManager (SmartLifecycle) → StartupValidator (path writability, _tmp sweep)
→ BindingRuntimeFactory → BindingRuntime → N × TransactedReceiveLoop
  (each: BatchAccumulator → BatchTransactionProcessor → writer/tracker/audit,
   with SessionRecoveryCoordinator and LoopStateReporter alongside)
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
| 10 | RMS tracker contract complete or gated | **COMPLETE — gate now opens** | The full legacy source has been captured and reproduced: `tagList`, root-end constants (`MessageHeaderDetailsType`), `getCompleteStartTag`/`getCompleteEndTag`, `setReplacedTagData`, `buildResultData` and the `yyyy-MM-dd'T'HH:mm:ss` timestamp. `RmsTrackerContractGatingTest` now asserts readiness, and `RmsApplicationSpringBootTest` proves the rewrite end to end through the real context. Remaining: validate output against the live tracker consumers at cutover (DESIGN item #24) — an operational check, no longer a code gate |
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

**RMS: no remaining code gate — but this section previously said so while it
was false.** RMS has two production gates, not one. The tracker contract gate
(§20.4) was completed earlier and this section was updated on that basis, but
the *serializer* gate (§9.1) was missed: `RmsRecordSerializer` still carried
the `PlaceholderSerializer` marker, which `SerializerValidator` rejects in
production mode. `MQ_INTAKE_PRODUCTION=true` would have failed startup outright
with "Binding 'rms' uses placeholder serializer".

Nothing caught it because every boot test ran without a production profile,
where the same condition only logs a warning. The marker has now been removed —
legitimately, since the contract it was gating (byte-offset `LongWritable` key,
normalised `Text` value) is confirmed against the MDB — and
`RmsProductionProfileSpringBootTest` boots the real application with the `prod`
profile so the gates are exercised by the suite rather than first met in
production.

A second defect sat behind it: the gates read only `MQ_INTAKE_PRODUCTION`,
though a `prod`/`production` Spring profile was equally documented. An
application started with `--spring.profiles.active=prod` and no environment
variable ran with **every production check silently disabled** — the likeliest
combination on a container platform. `ProductionMode` is now the single source
of truth and honours both signals.

What stands between RMS and production is environment validation, not code:

- HDFS, Kerberos and volume are still unproven — see D′ and G. This is the
  dominant risk and UAT is where it gets addressed.
- Environment prerequisites must be confirmed: `MAXMSGL` above 4 MB where
  large messages occur, `BOTHRESH`/`BOQNAME` matching on **both** queue
  managers, and `-Xmx` sized for retained heap.
- The hardcoded credential found in the legacy `EJBHelper` should be rotated
  independently of this project (DESIGN item #27).

**Claims: deferred by decision.** Reconciliation and the claims identity
question are out of scope for this iteration, so the placeholder-serializer and
identity gates still apply to Claims under production mode.

The gates were never the whole story: output correctness and load behaviour
were the other two, and only the first of those is now closed.

## D. CODE BLOCKERS
1. ~~Writable types are wrong.~~ **FIXED and CONFIRMED.** Both serializers now
   declare `LongWritable` key / `Text` value. The live writer's actual append
   line — `sequenceFileWriter.append(new LongWritable(offset), new Text(message))`
   in `HDFSWriter.write(...)` — confirms this directly, independently of the
   header-length fingerprint that first established it. Guarded by
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
1a. ~~Key VALUES are wrong.~~ **FIXED.** Keys are now the record's byte offset
   in the file, matching `long offset = sequenceFileWriter.getLength()` read
   before each append. `SequenceFileBatchWriter` reads `writer.getLength()` per
   record and passes it via a new `RecordMetadata.fileByteOffset`, which keeps
   the `RecordSerializer` seam intact — the writer supplies the fact, the
   serializer still decides the key. `recordOffset` remains the batch index for
   traceability; the two are deliberately separate fields.

   Proven end to end by `ByteOffsetKeyIntegrationTest`, which drives the real
   writer and reads the file back: the first record of a fresh file lands on
   **129** — the value seen in production samples — offsets grow by each
   record's encoded size rather than by 1, a longer payload advances the offset
   further, and keys restart at the header in each new file. A third test
   re-derives the expected keys independently from `getLength()`.

2. Production `RecordSerializer`s once metadata placement (open item #2) is
   approved — replace placeholders, drop the marker, keep value bytes
   contract-compatible.

   *Payload normalisation is done and CONFIRMED:* `PayloadNormalizer` in core
   reproduces `processMessage` — each `\n`, `\r`, `\t` replaced by one space,
   runs not collapsed, no `trim()` — and both serializers apply it. The MDB
   confirms both ingest paths call `processMessage` before `writeToHDFS`, so it
   genuinely applies to the SequenceFile branch; no revert needed. Claims
   extracts identity *after* normalising, so the identity corresponds to what
   is actually written rather than to a raw form no reader could recover.

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

**R-3 — claims message sizes make the current batch config unsafe.** Source
documentation gives typical claims messages at ~30 KB with **some over 10 MB**.
Against `batch_bytes` 128 MB and `batch_size` 8000:

| Message size | Batch fills at | Notes |
|---|---|---|
| ~30 KB (typical) | ~4,400 messages | bytes trigger, well before `batch_size` |
| >10 MB (tail) | **~13 messages** | a batch is a handful of very large objects |

Two consequences:

- **`batch_size` 8000 is effectively dead config** for claims — the bytes
  trigger always fires first. Harmless, but it means the batch-size figure
  gives a misleading impression of how many messages a unit of work holds.
- **Heap risk is larger than the budget suggests.** `estimateMessageSize`
  returns `text.length()` — a character count. A Java `String` holds ~2 bytes
  per char (UTF-16), plus the JMS message and MQ client buffers, so retained
  heap runs ~2–3× the estimate. A 128 MB budget × 4 threads is ~512 MB
  estimated but plausibly **1–1.5 GB retained**, for claims alone. The
  heap-derived ceiling (50% of max heap) exists to absorb exactly this, but it
  means `-Xmx` must be sized for the *retained* figure, not the configured one.

For UAT, lower claims `batch_bytes` (32 MB is a reasonable starting point) and
size the heap from the retained estimate rather than the budget.

**R-4 — 10 MB messages may exceed the queue manager's `MAXMSGL`.** IBM MQ
defaults to 4 MB. If `MAXMSGL` is not raised on the claims queues *and* the
channel, messages in the >10 MB tail cannot be put or got at all. Verify before
UAT — this would present as a hard failure on the largest and most valuable
messages, not as degradation.

**R-2 — scale is untested by roughly three orders of magnitude.** The largest
volume ever pushed through the production path in a test is 12 messages.
Production is batch 8000 × 4 threads (claims) and 4000 × 4 (RMS). The acute
case is bisection: isolating one poison at batch 8000 takes ~13 halvings, each
rolling back up to 8000 messages, with `backout_threshold` 14. Whether that
converges acceptably on the high-volume feed — the exact scenario BISECT was
chosen for — is unmeasured. Given R-1's precedent, load behaviour on this path
should not be assumed from unit-scale tests.

## D″. Reviewed, known, deliberately not fixed before cutover

A structured review of the delivery-critical classes (loop, writer/flush,
poison/degraded) found twelve issues. Six were fixed, each with a regression
test verified to fail against the unfixed code. The remaining six are recorded
here rather than silently carried: none can lose a message, and all were judged
higher-risk to change than to leave two days before cutover.

| # | Finding | Why it can wait | Cost if it fires |
|---|---|---|---|
| 1 | **Suspect set has no TTL or cap.** `collectMessageIds` silently skips a message whose `getJMSMessageID()` throws, so that ID is never passed to `clearSuspects` and stays in the set forever. An admin purge or MQ-side expiry orphans IDs the same way. | Requires `getJMSMessageID()` to throw, which has not been observed. | `recordSuccess()` can never satisfy "no suspects outstanding" — the binding stays at reduced batch size until restarted. Throughput only; delivery is unaffected. Restart clears it. |
| 2 | **Session-fault detection is substring matching.** Whether a `JMSException` triggers recovery is decided by searching the message, the linked exception and the error code for keywords. A connection loss whose text matches none of them falls outside the backoff path and the loop retries tightly against a dead session. **Unchanged in behaviour, but no longer buried:** it lives in `DefaultSessionFaultPolicy` behind the `SessionFaultPolicy` interface with 11 direct tests, and the retry machinery around it now sits in `SessionRecoveryCoordinator`, so it is replaceable without touching the loop. The same substring-matching defect was separately found and FIXED in `MqConnectionManager.isConfigurationError`, which searched only the top-level message and so recognised none of the five reasons it listed; it now searches the linked exception too. | The error codes that actually occur are covered, and both are exercised against a real queue manager (channel bounce and full QM restart). | Log flood and a spinning thread, not loss — each iteration still rolls back and clears the batch. Needs a restart. |
| 3 | **`BatchWriteException` data-classification is text matching.** `FailureClassifier` looks for "serialize"/"parse"/"malformed"/"invalid"; the only production site that matches does so because its message happens to contain "serialize". No shared constant, no test pinning the coupling. | Works today; the primary data path (`SerializationException`) is classified by type, not text. | If the string is ever edited, that failure silently becomes `UNKNOWN`, which never enters degraded mode — a poison message would stop being isolated. Worth a typed fix in the next iteration. |
| 4 | **HDFS rename failure classifies as `UNKNOWN`.** The `rename() == false` exception carries no cause and matches no HDFS keyword. | Both `UNKNOWN` and `HDFS_INFRASTRUCTURE` skip degraded mode, and `UNKNOWN` alerts more aggressively. | Muddies on-call triage of the rename step. No behavioural difference. |
| 5 | **A clean message can reach the backout queue during a long outage.** Poison detection is delivery-count-only. Infrastructure failures deliberately do not shrink the batch, so every message in every rolled-back batch accrues delivery count at full rate; a long enough outage pushes an undamaged message past `BOTHRESH`. | **This is legacy parity, not a regression** — `BOTHRESH` is an MQ-level mechanism and the existing MDB behaves identically. | Valid messages diverted to the BOQ during an incident. Mitigated by sizing `BOTHRESH` above plausible outage windows; confirm that sizing with the MQ team (see §E). |
| 6 | ~~**A `RuntimeException` in the loop body outside `processBatch` kills the listener thread.**~~ **RESOLVED.** The thread can still die, but it is no longer silent: `ListenerSupervisor` retains each loop's `Future`, detects unexpected termination, and reports DEGRADED while some listeners survive or UNHEALTHY when none do. | — | The open question in this row — "confirm the supervisor notices a dead loop thread" — is what prompted building one. |

Item 5 needs a sign-off decision from the MQ team rather than a code change.
Items 1 and 3 are the two most likely to matter in week one.

Item 6 was closed by the listener supervision added since; items 2 and 3 are
unchanged in behaviour but are now isolated behind interfaces and directly
tested, so replacing either is a contained change rather than surgery on the
receive loop.

**7. ~~The queue-depth gauges are never populated.~~ RESOLVED for backout
depth.** `BackoutQueueDepthMonitor` now samples each binding's backout queue
on its own daemon thread and writes `backoutQueueDepth`, so the alert DESIGN
§14 nominates as the pager condition can fire. Interval is
`backout_depth_poll_interval_ms` (default 30s, 0 disables).

Depth is read with a `QueueBrowser`, which does not consume — an operator
paged about the backout queue still finds the messages there. There is no
portable JMS depth API, and the alternatives were worse: PCF needs a command
server and admin authority, and the IBM inquire APIs would tie the class to
one provider. Browsing is cheap precisely because a healthy backout queue is
empty, and the count is capped at 1000 so a deep queue cannot cost an
unbounded enumeration.

Two behaviours worth knowing when writing the alert:
- **A failed sample leaves the gauge at its last value rather than zeroing
  it.** Zeroing on error would suppress a page exactly when visibility was
  lost. Staleness is exposed via the monitor's `isDepthAvailable()` and
  `getLastSuccessfulPollMs()`, and repeated failures log at ERROR.
- **The gauge is sampled, not event-driven**, so it lags a routing event by up
  to one interval. For a faster signal, alert on `poisonMessagesRouted`, which
  is incremented synchronously on the routing path; use depth for "messages
  are sitting in the BOQ right now".

Still outstanding: `sourceQueueDepth` and `trackerQueueDepth` remain
unpopulated. Deliberately not wired the same way — those queues are deep by
design, so browsing them would be genuinely expensive, unlike the backout
queue. They need an MQ admin query (PCF or equivalent) if they are wanted.

## E. ENVIRONMENT/PLATFORM DEPENDENCIES
- IBM MQ: BOTHRESH/BOQNAME configured per queue to match app thresholds
  (claims BISECT requires BOTHRESH ≥ 14 for batch 8000); MAXUMSGS ≥ 2×batch.
  Where a feed is presented on two independent queue managers it is modelled as
  two bindings, so both queue managers need matching settings.
- **JVM heap sizing is now load-bearing.** `aggregate_memory_ceiling_bytes` is
  derived from 50% of max heap when unset, so `-Xmx` must be set deliberately
  per environment. Batch budget is `batch_bytes × listener_threads` per
  binding, counted once per binding — a feed split across two queue managers
  counts twice. Startup fails with the arithmetic if the config does not fit.
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
2. **Reconciliation: in or out?** This is now the decision, and it subsumes
   the claims-identity question. **Confirmed: the MDB extracts no payload
   identity and performs no dedup.** So record identity exists in our design
   solely to serve reconciliation and §10 orphan classification — capabilities
   the legacy system never had.
   - *Keep it* → open item #17 (approved claims identity) and item #2 (metadata
     placement) both need answers, and metadata needs a home the production
     layout permits. The key is a byte offset and the value is contractual, so
     that means **Option C (sidecar)**.
   - *Drop it* → the claims-identity gate and the metadata-placement decision
     both disappear, along with a code blocker. Cost: no automated way to
     classify a file that landed without an audit record — the §10 safety net
     for at-least-once duplicates.
3. ~~Legacy tracker rewrite artifacts (§20.4)~~ **RESOLVED.** Full source
   captured and reproduced. What remains is DESIGN item #24 — validate the
   rewritten header against the live tracker consumers at cutover. That is an
   operational sign-off, not a build blocker.
4. Quarantine/retention policy for reconciliation duplicates.
5. **Freshness SLA per feed.** `batch_interval_ms` is now 0, so a message may
   wait until its partition window closes (~15 min worst case). Confirm both
   feeds tolerate that; it is the price of the legacy file cadence (R-1).
6. **Which quarter-hour does a boundary-crossing batch belong to?** The
   partition path is computed from the wall clock at flush time. A PARTITION
   flush by definition fires just *after* the window closes, so a batch opened
   at 10:14 is filed under the 10:15 window. The legacy MDB has no equivalent
   skew because it writes one message per file at receive time.

   The alternative — filing under the window the batch was opened in — was
   considered and **not** adopted, because it writes into a partition that has
   already closed, and any downstream job that sweeps partitions on a schedule
   would miss the late arrival. Filing forward keeps every write landing in a
   still-open partition. The cost is that a file in window N may contain
   messages received in the last moments of window N-1.

   Confirm with the downstream consumers that forward-filing is the behaviour
   they want. This is a contract decision, not a code defect; it was left as
   built rather than changed immediately before cutover.

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
| Tracker outage | **MDB parity: logged and counted, batch still commits** — data lands once, that notification is lost | proven (embedded); strict rollback retained behind `fail_batch_on_tracker_error` |
| Crash after rename, before commit | redelivery duplicate; reconciliation flags it | proven (embedded + reconciliation tests) |
| Crash after commit, before audit | file kept; retrospective audit | proven (embedded + reconciliation tests) |
| Production message volume | batching bounds file count; partition-aligned flush | **not proven** — 12 messages max in test vs batch 8000 (R-2). The comparable prior attempt failed here (R-1) |
| Quiet-period file cadence | one file per partition window | mitigated in design and unit-tested; unmeasured against a real feed (R-1) |
| Out-of-order / redistributed delivery | landing order non-authoritative | **confirmed acceptable by the claims consumer**, which stores all messages and builds the current view on read |
