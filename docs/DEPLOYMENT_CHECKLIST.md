# RMS Production Deployment Checklist

**Application:** `datalake-mq-intake` — RMS binding (`rms` module, separate Spring Boot app)
**Release baseline:** commit `1bc1655` — full suite 804 green (0 skipped), 11 real-MQ drill tests green with the ABC balance check active.
**Review verdict:** `RMS GO WITH CONDITIONS` — no confirmed code defect blocks promotion; the conditions are the environment checks and pre-cutover tests below.
**Claims:** ships as a **separate application** with deliberately limited scope this release. Its checklist is a short appendix at the end; nothing Claims does can affect the RMS process.

Conventions: `[ ]` = must be checked off by a named person before cutover. Items marked **GATE** are verified automatically at startup — if the pod comes up, they passed; they are listed so the operator knows what a startup failure means.

---

## 1 — IBM MQ environment

- [ ] Source queue `MQ.HPS.MEMBERSHIP.IN` exists on the production queue manager.
- [ ] Tracker queue `MQ.HPS.MEMBERSHIP.TRACKER` exists; downstream tracker consumer is attached and drains it.
- [ ] Backout queue `MQ.HPS.MEMBERSHIP.BACKOUT` exists with adequate `MAXDEPTH`.
- [ ] `BOTHRESH(5)` and `BOQNAME(MQ.HPS.MEMBERSHIP.BACKOUT)` set **on the source queue**. The application reads its own `backout.threshold: 5` and never asserts the QM attribute — the two must be kept in step manually (same values as the drill environment in `docker/mq-config/20-queues.mqsc`).
- [ ] Queue manager `MAXUMSGS ≥ 8000`. A TRACKED batch of 4000 is a unit of work of up to 8000 messages (4000 gets + 4000 tracker puts). The application assumes 10000.
- [ ] `MAXMSGL` on channel and queues covers the largest real RMS payload. The application enforces no per-message size cap of its own.
- [ ] **TLS decision confirmed with the MQ team.** Nothing in the repository configures TLS; if the production channel requires it, that is new work and blocks cutover.
- [ ] Channel auth / CHLAUTH rules permit the service account; credential delivered via `MQ_CREDENTIAL_REF` (never in a file, never in YAML).
- [ ] **Legacy credential rotated** (DESIGN item #27): the service account password that was hardcoded in the legacy `EJBHelper` must be rotated before this service goes live with it.

## 2 — HDFS and Kerberos

- [ ] Landing base path `/data/raw/membership/hps` exists and is writable by the service principal (**GATE**: `StartupValidator` refuses to start otherwise, including the `_tmp` subtree).
- [ ] Audit base path exists and is writable (**GATE**: validated per binding at startup; audit is fail-closed, so an unwritable audit path stalls the first batch — validation turns that into a refusal to start).
- [ ] Kerberos keytab and principal valid **against the production KDC**. The app validates keytab file existence/permissions only, not principal validity — do a manual `kinit` with the deployment keytab.
- [ ] Deployment sets `KERBEROS_ENABLED=true` plus principal/keytab variables (defaults are `false` for local dev).
- [ ] Atomic-rename semantics sanity-checked on the real cluster (proven on MiniDFS in CI; HDFS rename is atomic by contract, this is a belt-and-braces smoke check).
- [ ] Replication factor on the landing and audit paths matches durability expectations (hsync forces DataNode fsync across the replica set — that is the durability floor).

## 3 — Deployment manifest

- [ ] `MQ_HOST`, `MQ_PORT`, `MQ_QUEUE_MANAGER`, `MQ_CHANNEL`, `MQ_CREDENTIAL_REF` all explicitly set. (**GATE**: `DevDefaultConnectionGate` — production mode refuses the dev placeholders `localhost`/`QM1`/`DEV.APP.SVRCONN`, so a forgotten variable fails startup loudly instead of dialing a dev queue manager.)
- [ ] Production mode armed: Spring profile `prod`/`production` active **or** `MQ_INTAKE_PRODUCTION=true`. This is what arms every gate (dev-default, placeholder-serializer, tracker-contract). Without it the app runs in permissive dev posture.
- [ ] `-Xmx` sized so that 512 MB of raw in-flight batches (4 listeners × 128 MB `batch.bytes`) fits under the memory validator's ceiling **with the real copy multiplier** — payload copies during normalization/serialization put realistic worst case at 1–1.5 GB. Recommend `-Xmx ≥ 4g`. (**GATE**: `AggregateMemoryRule` fails startup if the configured budget exceeds 50% of max heap.)
- [ ] Container liveness probe → `/actuator/health/liveness` (ping only). Readiness/alerting → main health endpoint. Do **not** point the restart probe at the main endpoint.
- [ ] No legacy flat binding keys in any override layer (**GATE**: `LegacyBindingKeyDetector` fails startup naming each stale key and its replacement).

## 4 — Application configuration (verify in the deployed config, not just the repo)

| Setting | Required value | Why |
|---|---|---|
| `batch.size` | 4000 | ≤ MAXUMSGS/2 for TRACKED mode |
| `listener-threads` | 4 | one transacted session per thread |
| `hdfs.hsync-on-flush` | `true` | durability floor; closes the post-commit power-loss window |
| `hdfs.record-index-enabled` | `true` (RMS only) | sidecar index feeds identity reconciliation |
| `audit.balance-check-enabled` | `true` (RMS only) | transaction-time ABC: consumed = written + backout before every commit |
| `audit.fail-batch-on-error` | `true` (default) | audit is a control; no unaudited commits |
| `tracker.body-mode` | `FULL_COPY` | `CUSTOM` now fails loudly; `HEADER_ONLY` would drop payloads |
| `tracker.fail-batch-on-error` | `false` (default) | legacy MDB parity — tracker failure never rolls back the message |
| `backout.threshold` | 5 | must match QM `BOTHRESH` |
| `reconciliation.enabled` | `true` | the post-write half of ABC |
| `mq-connections.*.receive-timeout-ms` | positive (1000) | `receive(0)` waits forever and starves partition flush (**GATE**) |

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
| `backout_queue_depth` | > 0 | **Page** (the design's nominated pager condition — poison messages parked). |
| `reconciliation_discrepancies_total` | any increment | Page/urgent — the balance control found something. |
| `healthy` gauge | 0 for more than a few minutes | Alert — batches failing or a fault loop. |
| `reconnect_failures_total` | increments | Alert — a listener exhausted its recovery budget and stopped (binding limps at reduced threads until pod restart; visible as DEGRADED). |
| `identity_misses_total` | sustained climb | Warn — upstream `<MessageID>` regression; each miss removes its file from identity-based reconciliation. |
| `degraded` gauge / health `DEGRADED` | sustained | Warn — poison isolation active or listener lost. |
| `tracker_failures_total` | sustained climb | Warn — tracker notifications being lost (messages still land, by design). |

Health endpoint semantics: `PARTIAL_OUTAGE` and `DEGRADED` return **HTTP 200** (alert on them; do not let the orchestrator restart the pod for them). `DOWN`/503 means nothing is consuming.

## 7 — Cutover

1. [ ] Confirm the legacy MDB drain/stop plan with the platform team (both consuming simultaneously is safe for delivery — both are transacted consumers — but splits throughput and complicates validation).
2. [ ] Deploy with production mode armed; confirm clean startup log: config validated → serializer gate → paths validated → temp sweep → bindings started → reconciliation started.
3. [ ] Smoke test: send one representative test message; confirm within one partition window (15 min max, or immediately at batch flush): the `.seq` file lands in the correct partition, the sidecar index sits beside it, the audit record exists with `balance_status: BALANCED` and `consumed_count` = expected, and the tracker message reached the tracker queue.
4. [ ] Watch the first quiet-hour partition boundary: confirm the timer/partition flush lands a file for a quiet queue (this exercises the idle-flush path).
5. [ ] Confirm the first scheduled reconciliation pass over a closed partition reports CLEAN.

## 8 — Rollback plan

- Stop the service (graceful: drains and commits in-flight batches). **Messages are never at risk during rollback** — anything unprocessed simply queues on MQ.
- Restart the legacy MDB consumer if reverting; no data migration is needed in either direction.
- Files already landed stay landed and audited; re-consumption of any redelivered messages by either consumer produces at-least-once duplicates, which reconciliation classifies. No cleanup action required before rollback.

## 9 — Accepted behaviors (do not treat as incidents)

- **Duplicates are design-permitted.** Any crash between HDFS rename and MQ commit yields a duplicate file on redelivery; reconciliation detects and classifies them, quarantine is a move (never a delete).
- **Audit-path outage stalls ingestion.** Fail-closed by design: messages wait on the queue until the audit store recovers. This is the correct trade for a completeness-critical feed.
- **A listener that exhausts recovery stops and stays stopped** (binding runs at reduced threads, reported DEGRADED) until pod restart. Deliberate: no in-process thread resurrection.
- **Tracker per-message failures are swallowed** (counted + logged): legacy MDB parity. The landed data is kept; one notification is lost.

---

## Appendix — Claims application (deliberately limited this release)

Separate deployment, separate process; nothing here affects RMS. Deploy with its shipped config: `LAND_ONLY`, `BISECT` degradation with `backout.threshold: 14` (matching `BOTHRESH(14)` on the real QM — verify like §1), reconciliation **disabled**, record index **off**, balance check **off**. Known and accepted: no per-message identity, no reconciliation/dedup/completeness guarantee. In production mode the placeholder-serializer gate refuses startup unless explicitly resolved — that is intentional; confirm the intended Claims posture with the data owners before arming production mode on the Claims app.
