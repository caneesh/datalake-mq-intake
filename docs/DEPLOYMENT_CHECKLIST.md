# RMS Production Deployment Checklist

**Application:** `datalake-mq-intake` — RMS binding (`rms` module, separate Spring Boot app)
**Release baseline:** commit `e61bbf8` — 835 unit/integration green (0 skipped) plus 11 real-MQ drill tests green with the ABC balance check active.
**Review verdict:** `RMS GO WITH CONDITIONS` — no confirmed code defect blocks promotion; the conditions are the environment checks and pre-cutover tests below.
**Claims:** ships as a **separate application** with deliberately limited scope this release. Its checklist is a short appendix at the end; nothing Claims does can affect the RMS process.

Deployment mechanics (build locally, ship, run, roll back) live in `docs/TEST_DEPLOYMENT_GUIDE.md`; the same scripts serve production.

Conventions: `[ ]` = must be checked off by a named person before cutover. Items marked **GATE** are verified automatically at startup — if the pod comes up, they passed; they are listed so the operator knows what a startup failure means. Items marked **[preflight]** are proven by the step below, which produces the evidence for them.

---

## 0 — Preflight (run this first, and again after any environment change)

Preflight connects to the real dependencies with the deployment's own configuration, checks one fact at a time, prints a report naming the fix for each failure, and exits. **It starts no listener, consumes no message, sends nothing to a queue another system reads, and writes only inside `_tmp/{instanceId}`, removing what it wrote.** Safe against an environment carrying live data.

```bash
./preflight.sh rms              # everything
./preflight.sh rms mq           # MQ only (also: hdfs, app)
```

Without the script: `java -jar rms/target/*.jar --intake.preflight.enabled=true [--preflight=mq]`. Point it at the same manifest/config the service will use — `MQ_HOST`, `MQ_CREDENTIAL_REF`, `KERBEROS_*` and the rest are read exactly as the service reads them. Exit status is 0 on a clean run and 1 otherwise, so a deployment pipeline can gate on it.

- [ ] `PREFLIGHT PASSED`, exit status 0, against the production configuration
- [ ] Re-run with production mode armed (`MQ_INTAKE_PRODUCTION=true`); `production-mode` must report **ARMED**
- [ ] Report archived as cutover evidence

**What it proves, and what it cannot.** Preflight settles reachability, permissions and the durability sequence. It cannot read queue-manager attributes (`BOTHRESH`, `MAXDEPTH`, `MAXUMSGS`, `MAXMSGL`), decide the TLS question, confirm the legacy credential was rotated, or tell you a downstream consumer is attached — those stay manual below.

| Preflight check | Settles |
|---|---|
| `<binding>.connection` | host, port, queue manager, channel, credentials |
| `<binding>.source-queue.input` | the listener can open the source queue for input |
| `<binding>.tracker-queue.output` | tracker queue reachable **on the connected queue manager** |
| `<binding>.backout-queue.output` | backout queue reachable **on the connected queue manager** |
| `<binding>.backout-queue.browse` | browse authority for the depth gauge, plus current depth |
| `filesystem.connect` | storage reachable, and under which identity (Kerberos included) |
| `<binding>.landing-path` / `.temp-path` / `.audit-path` | all three directories exist and accept writes |
| `<binding>.durability-roundtrip` | write → hsync → close → **atomic rename** → read-back, byte-compared |
| `production-mode` | whether the startup safety gates are armed in this process |
| `<binding>.serializer` / `.tracker-builder` | components build; placeholder serializers are refused in production mode |
| `<binding>.controls` | one line listing every control the binding will run with |

**The check to look at hardest** is `backout-queue.output`. The tracker and backout producers run on the listener's own transacted session, so a queue defined on a *sibling* queue manager is unreachable however healthy it looks in a console — and the symptom in production is the first poison message rolling back forever while the binding stalls. `MQRC 2085` there is exactly that, and the report says so.

## 1 — IBM MQ environment

Queue names are environment-specific and are **not** the repo's placeholder values. The shipped YAML reads them from the environment, so set these in the manifest:
`MQ_SOURCE_QUEUE`, `MQ_TRACKER_QUEUE`, `MQ_BACKOUT_QUEUE` (and `HDFS_BASE_PATH`, `HDFS_AUDIT_BASE_PATH`).

> Do **not** try `INTAKE_BINDINGS_0_SOURCE_QUEUE` or `--intake.bindings[0].source-queue`. Spring supplies a whole collection from one source, so a single indexed override discards the rest of the binding and startup fails with `Binding 'null' must specify mq-connection`. Verified, not theoretical. Behaviour changes (batch size, thresholds) need a config file carrying the complete `bindings:` block — see `docs/TEST_DEPLOYMENT_GUIDE.md`.

- [ ] Source queue exists on the queue manager the application will connect to. **[preflight]**
- [ ] **Tracker queue exists on that SAME queue manager.** **[preflight]** The tracker producer is created from the listener's own transacted session, so a tracker queue on a different QM cannot be reached — the binding fails when the session opens.
- [ ] **Backout queue exists on that SAME queue manager.** **[preflight]** Same reason, worse consequence: the poison screen puts to the BOQ on the consuming session. If the BOQ is only defined on a sibling QM, the first poison message fails to route, the batch rolls back, and the message is redelivered forever — a permanent stall of the binding (nothing lost; nothing progresses either). In a multi-QM pair, verify the definition on **each** QM the app may connect to, not just one.
- [ ] Downstream tracker consumer attached and draining the tracker queue.
- [ ] **BOQ `MAXDEPTH` sized against the batch, not against expected poison volume.** Any rollback increments the delivery count of *every* message in the batch, so without the routing gate a prolonged outage diverts a whole in-flight batch of good messages to the BOQ. `backout.route-only-on-data-failures: true` is the primary defence and `batch.size: 1000` bounds the residue, but a BOQ shallower than a couple of batches is still the wrong shape: once full, BOQ puts fail and the binding stalls (safely).
- [ ] `BOQNAME` set on the source queue and matching the app's `backout.queue`.
- [ ] `BOTHRESH` on the source queue matches the app's `backout.threshold`. **The application never reads the QM attribute** — its own value is what governs, and the mapping is exact (`deliveryCount = backoutCount + 1`, app routes when `deliveryCount > threshold`, so app threshold *N* reproduces `BOTHRESH(N)`). Keep them equal so the queue attribute does not mislead operators. Prefer the *higher* value when in doubt: a low threshold combined with a large batch turns a brief infrastructure outage into thousands of good messages on the BOQ.
- [ ] Queue manager `MAXUMSGS ≥ 2 × batch.size`. A TRACKED batch is a unit of work of up to 2N messages (N gets + N tracker puts); at `batch.size: 1000` that is 2000. The application assumes 10000 and validates the batch against MAXUMSGS/2.
- [ ] `MAXMSGL` on channel and queues covers the largest real RMS payload. The application enforces no per-message size cap of its own.
- [ ] **TLS decision confirmed with the MQ team.** Nothing in the repository configures TLS; if the production channel requires it, that is new work and blocks cutover.
- [ ] Channel auth / CHLAUTH rules permit the service account; credential delivered via `MQ_CREDENTIAL_REF` (never in a file, never in YAML). **[preflight]** — a clean `connection` check is the proof.
- [ ] **Legacy credential rotated** (DESIGN item #27): the service account password that was hardcoded in the legacy `EJBHelper` must be rotated before this service goes live with it.

## 2 — HDFS and Kerberos

Every variable, where it goes, and which preflight check proves it: **[Property reference](PROPERTY_REFERENCE.md)**. When deploying onto a host that already runs another Hadoop client, the cluster-side values can be read off the working application rather than requested — see the mapping table there, and [Deploying alongside another Hadoop client](WEBSPHERE_HOST_DEPLOYMENT.md).

- [ ] `HDFS_CONFIG_RESOURCES` points at the **target** cluster's conf directory **[preflight]** — `cluster-config.resources` prints the files it read. (**GATE**: production mode refuses an empty value, and refuses a filesystem that resolves to local disk.)
- [ ] `HDFS_EXPECTED_NAMESERVICE` set **[preflight]** — `filesystem.nameservice` must **pass, not skip**. A skip means nothing is guarding which cluster the data lands on, and every other check passes against the wrong one.
- [ ] `dfs.client.use.datanode.hostname` settled — set via `JAVA_OPTS`, or confirmed present in the cluster's `hdfs-site.xml`. NameNode connectivity does not imply DataNode connectivity.
- [ ] Landing base path exists and is writable by the service principal **[preflight]** (**GATE**: `StartupValidator` refuses to start otherwise, including the `_tmp` subtree).
- [ ] Audit base path exists and is writable **[preflight]** (**GATE**: validated per binding at startup; audit is fail-closed, so an unwritable audit path stalls the first batch — validation turns that into a refusal to start).
- [ ] Kerberos keytab and principal valid **against the production KDC** **[preflight]** — `filesystem.connect` reports the identity it authenticated as, which is the real proof; a manual `kinit` remains useful if that check fails.
- [ ] Deployment sets `KERBEROS_ENABLED=true` plus principal/keytab variables (defaults are `false` for local dev).
- [ ] Atomic-rename semantics sanity-checked on the real cluster **[preflight]** — `durability-roundtrip` performs the exact write → hsync → close → rename → read-back sequence a batch performs.
- [ ] Replication factor on the landing and audit paths matches durability expectations (hsync forces DataNode fsync across the replica set — that is the durability floor).

## 3 — Deployment manifest

- [ ] `MQ_HOST`, `MQ_PORT`, `MQ_QUEUE_MANAGER`, `MQ_CHANNEL`, `MQ_CREDENTIAL_REF` all explicitly set. (**GATE**: `DevDefaultConnectionGate` — production mode refuses the dev placeholders `localhost`/`QM1`/`DEV.APP.SVRCONN`, so a forgotten variable fails startup loudly instead of dialing a dev queue manager.)
- [ ] Production mode armed: Spring profile `prod`/`production` active **or** `MQ_INTAKE_PRODUCTION=true` **[preflight]** — `production-mode` reports ARMED. This is what arms every gate (dev-default, placeholder-serializer, tracker-contract). Without it the app runs in permissive dev posture.
- [ ] `-Xmx` sized so that 512 MB of raw in-flight batches (4 listeners × 128 MB `batch.bytes`) fits under the memory validator's ceiling **with the real copy multiplier** — payload copies during normalization/serialization put realistic worst case at 1–1.5 GB. Recommend `-Xmx ≥ 4g`. (**GATE**: `AggregateMemoryRule` fails startup if the configured budget exceeds 50% of max heap.)
- [ ] Container liveness probe → `/actuator/health/liveness` (ping only). Readiness/alerting → main health endpoint. Do **not** point the restart probe at the main endpoint.
- [ ] No legacy flat binding keys in any override layer (**GATE**: `LegacyBindingKeyDetector` fails startup naming each stale key and its replacement).

## 4 — Application configuration (verify in the deployed config, not just the repo)

| Setting | Required value | Why |
|---|---|---|
| `batch.size` | 1000 | ≤ MAXUMSGS/2 for TRACKED mode, and small relative to BOQ depth |
| `listener-threads` | 4 | one transacted session per thread |
| `hdfs.hsync-on-flush` | `true` | durability floor; closes the post-commit power-loss window |
| `hdfs.record-index-enabled` | `true` (RMS only) | sidecar index feeds identity reconciliation |
| `audit.balance-check-enabled` | `true` (RMS only) | transaction-time ABC: consumed = written + backout before every commit |
| `audit.fail-batch-on-error` | `true` (default) | audit is a control; no unaudited commits |
| `tracker.body-mode` | `FULL_COPY` | `CUSTOM` now fails loudly; `HEADER_ONLY` would drop payloads |
| `tracker.fail-batch-on-error` | `false` (default) | legacy MDB parity — tracker failure never rolls back the message |
| `backout.threshold` | 5 | keep in step with QM `BOTHRESH` (see §1) |
| `backout.route-only-on-data-failures` | `true` (both bindings) | an infrastructure outage must not divert healthy messages to the BOQ |
| `reconciliation.enabled` | `true` | the post-write half of ABC |
| `mq-connections.*.receive-timeout-ms` | positive (1000) | `receive(0)` waits forever and starves partition flush (**GATE**) |

Preflight's `<binding>.controls` check prints this whole set as one line from the running configuration — the fastest way to verify the deployed values rather than the repo's.

Note on reconnects: `reconnect-attempts`/`reconnect-delay-ms` govern the **initial connect only**. Session recovery during operation is fixed at 10 attempts with exponential backoff (1s→60s, jittered) and is not operator-tunable in this release.

## 5 — Pre-cutover functional verification (the review's conditions)

- [ ] Drill suite green against Docker IBM MQ at the release commit: `./test-with-docker.sh` (11 tests: connectivity, channel outage, QM restart, poison isolation — all with the balance check active).
- [ ] **Chaos test 1 — dead TCP connection:** kill the network path to the QM (not just a session). Confirm the binding either recovers or stops cleanly and alertably. This validates the IBM-client lazy-reconnect assumption that source review cannot prove.
- [ ] **Chaos test 2 — stop during in-flight commit:** issue shutdown under load repeatedly. Confirm no loss (analysis says worst case is one extra duplicate; this closes the residual doubt about client behavior under thread interrupt).
- [ ] Representative load run: sustained production-like volume (the largest volume ever tested in dev is tiny relative to production). Watch flush latency, heap, and commit rate.
- [ ] Downstream tracker consumer validates real rewritten tracker messages (DESIGN item #24 — golden-master verified in code; live-consumer acceptance is a cutover step).

## 6 — Monitoring and alerting (set up before cutover)

All metrics are per-binding, prefix `mq_intake_`.

| Signal | Condition | Severity |
|---|---|---|
| `balance_check_failures_total` | any increment | **Page.** A pre-commit accounting mismatch; the batch rolled back — investigate before backlog builds. |
| `backout_queue_depth` | > 0 | **Page** (the design's nominated pager condition — messages parked). First triage question: *poison or diversion?* A handful of messages alongside data-classified failures is genuine poison. A large jump arriving with a spike in `rollbacks_total` / `audit_failures_total` / HDFS errors is an infrastructure outage pushing **good** messages past the backout threshold — those must be replayed from the BOQ, never discarded. |
| `reconciliation_discrepancies_total` | any increment | Page/urgent — the balance control found something. |
| `healthy` gauge | 0 for more than a few minutes | Alert — batches failing or a fault loop. |
| `reconnect_failures_total` | increments | Alert — a listener exhausted its recovery budget and stopped (binding limps at reduced threads until pod restart; visible as DEGRADED). |
| `identity_misses_total` | sustained climb | Warn — upstream `<MessageID>` regression; each miss removes its file from identity-based reconciliation. |
| `degraded` gauge / health `DEGRADED` | sustained | Warn — poison isolation active or listener lost. |
| `tracker_failures_total` | sustained climb | Warn — tracker notifications being lost (messages still land, by design). |

Health endpoint semantics: `PARTIAL_OUTAGE` and `DEGRADED` return **HTTP 200** (alert on them; do not let the orchestrator restart the pod for them). `DOWN`/503 means nothing is consuming.

## 7 — Cutover

1. [ ] Confirm the legacy MDB drain/stop plan with the platform team (both consuming simultaneously is safe for delivery — both are transacted consumers — but splits throughput and complicates validation).
2. [ ] Run `./preflight.sh rms` against the production configuration one final time — clean run, exit 0, report archived.
3. [ ] Deploy with production mode armed; confirm clean startup log: config validated → serializer gate → paths validated → temp sweep → bindings started → reconciliation started.
4. [ ] Smoke test: send one representative test message; confirm within one partition window (15 min max, or immediately at batch flush): the `.seq` file lands in the correct partition, the sidecar index sits beside it, the audit record exists with `balance_status: BALANCED` and `consumed_count` = expected, and the tracker message reached the tracker queue.
5. [ ] Watch the first quiet-hour partition boundary: confirm the timer/partition flush lands a file for a quiet queue (this exercises the idle-flush path).
6. [ ] Confirm the first scheduled reconciliation pass over a closed partition reports CLEAN.

## 8 — Rollback plan

- Stop the service (graceful: drains and commits in-flight batches). **Messages are never at risk during rollback** — anything unprocessed simply queues on MQ.
- Restart the legacy MDB consumer if reverting; no data migration is needed in either direction.
- Files already landed stay landed and audited; re-consumption of any redelivered messages by either consumer produces at-least-once duplicates, which reconciliation classifies. No cleanup action required before rollback.

## 9 — Accepted behaviors (do not treat as incidents)

- **Duplicates are design-permitted.** Any crash between HDFS rename and MQ commit yields a duplicate file on redelivery; reconciliation detects and classifies them, quarantine is a move (never a delete).
- **A suppression notice in the log during an outage** (*"routing … is suppressed: the last failure was infrastructure-classified"*). Delivery-count routing cannot tell a malformed message from a healthy one that sat in batches which rolled back because HDFS was down, so with `route-only-on-data-failures: true` the diversion is withheld while failures look like infrastructure. Messages are retried, not diverted; a genuine poison message still reaches the BOQ, because its own failure classifies as message data. If the gate is ever turned off, expect the old behaviour instead: good messages on the BOQ after an outage, intact and requiring manual replay.
- **Audit-path outage stalls ingestion.** Fail-closed by design: messages wait on the queue until the audit store recovers. This is the correct trade for a completeness-critical feed.
- **A listener that exhausts recovery stops and stays stopped** (binding runs at reduced threads, reported DEGRADED) until pod restart. Deliberate: no in-process thread resurrection.
- **Tracker per-message failures are swallowed** (counted + logged): legacy MDB parity. The landed data is kept; one notification is lost.

---

## Appendix — Claims application (deliberately limited this release)

Separate deployment, separate process; nothing here affects RMS. Deploy with its shipped config: `LAND_ONLY`, `BISECT` degradation with `backout.threshold: 14` (matching `BOTHRESH(14)` on the real QM — verify like §1), `route-only-on-data-failures: true`, reconciliation **disabled**, record index **off**, balance check **off**.

The routing gate matters more here than for RMS, not less: Claims carries the higher volume and BISECT drives the threshold to 14, so a storage outage spends longer accumulating delivery counts across more messages before diverting anything — and diverts far more when it does. Preflight's `claims.controls` line reports `gated=true` when it is on. Known and accepted: no per-message identity, no reconciliation/dedup/completeness guarantee. In production mode the placeholder-serializer gate refuses startup unless explicitly resolved — that is intentional; confirm the intended Claims posture with the data owners before arming production mode on the Claims app.
