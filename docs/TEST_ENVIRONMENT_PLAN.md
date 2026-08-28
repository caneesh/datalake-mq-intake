# RMS Test-Environment Validation — Step by Step

**Application:** `datalake-mq-intake` — RMS binding
**Baseline:** commit `1bc1655` or later · full suite 804 green · 11 real-MQ drill tests green
**Purpose:** prove, in a test environment against real IBM MQ and real HDFS, everything unit tests and the Docker drill cannot: real connectivity, real Kerberos, real partition behaviour, real recovery, real throughput.
**Audience:** whoever runs the test cycle. Follow the parts in order — later parts assume earlier ones passed.

Companion document: `docs/DEPLOYMENT_CHECKLIST.md` (production cutover). This document is what you execute *before* that one.

---

## Part 0 — Before you start

### 0.1 What you need

| Need | Detail |
|---|---|
| Test queue manager | Own QM or dedicated queues; you will stop channels and restart the QM, so it must not be shared with anyone else's testing |
| Test HDFS | Writable landing + audit paths; Kerberos if the real cluster uses it |
| A JMS/MQ producer | `amqsputc` (MQ samples), MQ Explorer, JMSToolBox, or a small JMS client. You need one that can set a **JMS string property** — see 0.3 |
| `hdfs` CLI | Or any way to list/read HDFS files |
| Host access | To start/stop the app, read logs, and `kill -9` it |

### 0.2 Two facts that will otherwise cost you an afternoon

**A single test message will not land immediately.** Production config is `batch.size: 4000` and `batch.interval-ms: 0`, meaning a batch flushes on size, on bytes, or at the **quarter-hour partition boundary** — so one message sent at 10:07 lands at 10:15. For functional testing, use the test overlay in 0.4; switch back to production values for the load test (Part 6).

**A message without the `MessageHeaderDetails` property produces no tracker message.** That is the deliberate §20.3 null guard, not a bug. Every functional test message must set it or the tracker checks in Test 5 will "fail" for the wrong reason.

### 0.3 The standard test message

Payload (RMS carries its identity in `<MessageID>`):

```xml
<Member><MessageID>11111111-1111-1111-1111-000000000001</MessageID><Name>TEST-1</Name></Member>
```

Required JMS string property:

```
MessageHeaderDetails = <MessageHeaderDetailsType><Origin>TEST</Origin></MessageHeaderDetailsType>
```

Use a **unique `MessageID` per message** — the sidecar index and reconciliation identify records by it, and duplicates make Test 8 ambiguous.

### 0.4 Test-environment config overlay

Apply for Parts 3–5 and 7. **Revert to production values for Part 6.**

```yaml
intake:
  bindings:
    - id: rms
      batch:
        size: 10           # production 4000 — small batches so tests flush promptly
        interval-ms: 5000  # production 0 — a timer so a partial batch lands in 5s
```

Everything else stays at production values — especially `hsync-on-flush: true`, `record-index-enabled: true`, `audit.balance-check-enabled: true`, `backout.threshold: 5`.

Record here what you actually ran with:

| Setting | Value used | | Setting | Value used |
|---|---|---|---|---|
| `batch.size` | | | `listener-threads` | |
| `batch.interval-ms` | | | `backout.threshold` (app) | |
| QM `BOTHRESH` | | | Landing path | |

---

## Part 1 — Prepare the environment

### 1.1 MQ objects

On the test queue manager (`runmqsc QM_NAME`):

```
DEFINE QLOCAL(MQ.HPS.MEMBERSHIP.IN)      BOTHRESH(5) BOQNAME(MQ.HPS.MEMBERSHIP.BACKOUT) REPLACE
DEFINE QLOCAL(MQ.HPS.MEMBERSHIP.TRACKER) REPLACE
DEFINE QLOCAL(MQ.HPS.MEMBERSHIP.BACKOUT) REPLACE
DISPLAY QLOCAL(MQ.HPS.MEMBERSHIP.IN) BOTHRESH BOQNAME MAXDEPTH MAXMSGL
DISPLAY QMGR MAXUMSGS
```

The names above are the repo's placeholders. Real environments use their own — override them in the launch environment rather than editing the jar:

```bash
export INTAKE_BINDINGS_0_SOURCE_QUEUE=<real source queue>
export INTAKE_BINDINGS_0_TRACKER_QUEUE=<real tracker queue>
export INTAKE_BINDINGS_0_BACKOUT_QUEUE=<real backout queue>
```

- [ ] **Source, tracker and backout queues all exist on the SAME queue manager the app connects to.** Both the tracker producer and the backout producer are created from the listener's own transacted session, so neither can reach a queue on a sibling QM. A backout queue defined only on the other QM of a pair is the worst case: the first poison message fails to route, rolls back, and redelivers forever — the binding stalls permanently. In a multi-QM pair, check every QM the app may connect to.
- [ ] If the feed arrives on **more than one queue manager**, decide now: one binding per QM, each with its **own `hdfs.base-path`**. Two bindings sharing a base path make reconciliation report each other's files as orphans on every pass.
- [ ] `BOTHRESH` matches the app's `backout.threshold` (the app never reads the QM attribute; its own value governs. `deliveryCount = backoutCount + 1`, and the app routes when `deliveryCount > threshold`, so app threshold *N* reproduces `BOTHRESH(N)` exactly)
- [ ] BOQ `MAXDEPTH` comfortably exceeds `batch.size` — an outage can divert a whole in-flight batch of good messages there (see Test 7 and the accepted-behaviours appendix)
- [ ] `MAXUMSGS ≥ 8000` (a TRACKED batch of 4000 is a unit of work of up to 8000)
- [ ] `MAXMSGL` covers your largest test payload
- [ ] Channel and credentials work for the app's service account

### 1.2 HDFS paths

```bash
hdfs dfs -mkdir -p /data/raw/membership/hps /data/audit
hdfs dfs -chown <service-principal> /data/raw/membership/hps /data/audit
hdfs dfs -ls -d /data/raw/membership/hps /data/audit
```

Leave both **empty** at the start of the cycle so counts are unambiguous.

### 1.3 Environment variables

```bash
export MQ_HOST=<test-qm-host>          # NOT localhost — production mode refuses dev defaults
export MQ_PORT=1414
export MQ_QUEUE_MANAGER=<TESTQM>       # NOT QM1
export MQ_CHANNEL=<TEST.SVRCONN>       # NOT DEV.APP.SVRCONN
export MQ_CREDENTIAL_REF=<secret-ref>
export KERBEROS_ENABLED=true           # if the test cluster is kerberised
export KERBEROS_PRINCIPAL=<principal>
export KERBEROS_KEYTAB_PATH=<path>
```

If the test environment should exercise production gates (recommended), also arm production mode:

```bash
export MQ_INTAKE_PRODUCTION=true       # or run with --spring.profiles.active=prod
```

---

## Part 2 — Build and start

```bash
mvn clean install                                   # expect: BUILD SUCCESS, 0 failures
java -jar rms/target/datalake-mq-intake-rms-*.jar    # add --spring.profiles.active=prod if used
```

- [ ] Build succeeded
- [ ] Process started

---

## Part 3 — Startup verification

### Test 1 — Clean startup

**Steps:** start the app, read the log from the top.

**Expected log sequence:**

```
Binding configurations validated successfully
Serializer validation passed / All binding configurations validated successfully
Cleaned up N stale temp files          (only if debris existed)
Starting binding 'rms': mode=TRACKED, threads=4
Initialized session for binding 'rms': source='MQ.HPS.MEMBERSHIP.IN', mode=TRACKED   ×4
Binding 'rms' started with 4 listener threads
IntakeRuntimeManager started: 1 bindings, 4 total listener threads
```

**Pass:** all four listener sessions initialise; no ERROR; no stack trace.

### Test 2 — Health endpoint

```bash
curl -s localhost:8080/actuator/health | python3 -m json.tool
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/actuator/health/liveness
```

**Pass:** status `UP`, the `rms` binding present and healthy, liveness returns `200`.

### Test 3 — Metrics endpoint

```bash
curl -s localhost:8080/actuator/metrics | python3 -m json.tool | grep mq_intake | head -20
curl -s 'localhost:8080/actuator/metrics/mq_intake_messages_consumed_total' | python3 -m json.tool
```

**Pass:** `mq_intake_*` meters listed, tagged `binding=rms`. (If you get 404, your build predates the actuator exposure fix — upgrade to `1bc1655` or later.)

### Test 4 — The gates actually bite (negative tests)

Run each with production mode armed; the app **must refuse to start** with the stated message. Restore the correct value before continuing.

| # | Change | Expected refusal |
|---|---|---|
| 4a | unset `MQ_HOST` (falls back to `localhost`) | "dev-placeholder defaults … missing MQ_HOST / MQ_QUEUE_MANAGER / MQ_CHANNEL" |
| 4b | point `hdfs.base-path` at a path the principal cannot write | "Binding 'rms' base path is not writable: …" |
| 4c | add legacy flat key `intake.bindings[0].batch-size=10` | "Legacy binding configuration keys found … -> intake.bindings[0].batch.size" |
| 4d | set `receive-timeout-ms: 0` | "receive-timeout-ms must be positive: receive(0) blocks forever …" |

**Pass:** all four refuse at startup with the message naming the cause. These prove a misconfigured production deploy fails loudly rather than running wrong.

---

## Part 4 — Core functional path

### Test 5 — Happy path: MQ → HDFS → index → audit → tracker

**Steps**

1. Note the time (UTC) and the current queue depths.
2. Put **10 messages** (per 0.3, unique `MessageID`s, `MessageHeaderDetails` set) on `MQ.HPS.MEMBERSHIP.IN`.
3. Wait for the flush (≤ 5 s with the test overlay).

**Verify — data file**

```bash
hdfs dfs -ls -R /data/raw/membership/hps | grep '\.seq$'
```
Path shape: `/data/raw/membership/hps/year=YYYY/month=MM/day=DD/hour=HH/quarter=Q/rms_<instance>_<epochMs>_<seq>.seq`
Quarter is `minute/15` → `0`–`3`, in **UTC**.

```bash
hdfs dfs -text <the .seq file> | head -3          # key<TAB>value per record
hdfs dfs -text <the .seq file> | wc -l            # expect 10
```
The key is the record's **byte offset**, the value the payload with `\n \r \t` collapsed to spaces (so one record = one line — that is why `wc -l` is valid).

**Verify — sidecar index**

```bash
hdfs dfs -cat <the .seq file>.index.jsonl | head -3
```
Line 1 is the header: `{"schema":1,"binding":"rms",...,"records":10}`. Following lines: `{"offset":<n>,"identity":"<your MessageID>"}`.

**Verify — audit record**

```bash
hdfs dfs -ls /data/audit/rms/$(date -u +%Y%m%d)/
hdfs dfs -cat /data/audit/rms/$(date -u +%Y%m%d)/audit_rms_*.json | python3 -m json.tool
```

**Verify — tracker**: browse `MQ.HPS.MEMBERSHIP.TRACKER`; expect **10** messages.

**Pass criteria**

- [ ] Exactly one `.seq` file, in the correct UTC partition, containing 10 records
- [ ] `.index.jsonl` beside it: header `records: 10`, 10 entries, identities = the `MessageID`s you sent
- [ ] Audit record exists with `record_count: 10`, `backout_count: 0`, **`consumed_count: 10`**, `balance_delta: 0`, `balance_status: "BALANCED"`
- [ ] 10 tracker messages, each carrying a rewritten `MessageHeaderDetails`
- [ ] Source queue depth back to 0
- [ ] `mq_intake_messages_consumed_total` = 10, `mq_intake_commits_total` = 1, `mq_intake_balance_check_failures_total` = **0**

### Test 6 — ABC independence (the point of the control)

This proves `consumed_count` is a real source-side observation, not `record_count + backout_count`.

**Steps:** repeat Test 5 with 7 messages, then read the audit record.

**Pass:** `consumed_count: 7`, `record_count: 7`, `backout_count: 0`, `balance_delta: 0`. Keep the JSON as evidence — in Test 7 the same three fields must move independently.

### Test 7 — Poison message → backout queue → balance still closes

**How to make a poison message for RMS:** the serializer accepts any `TextMessage`; it rejects a **non-text message** (MQMD `Format` not `MQSTR` arrives as a `BytesMessage`) and a null body. Produce one with MQ Explorer ("put test message" with blank format), JMSToolBox, or a small JMS client sending a `BytesMessage`. Confirm the tool available in your environment before starting.

**Steps**

1. Put 8 good messages and 1 poison message.
2. Watch the log: repeated rollbacks for the batch containing the poison, batch size shrinking (`BATCH_OF_ONE`), then the poison routed to the BOQ once its backout count exceeds `BOTHRESH(5)`.
3. Wait until the source queue drains.

**Pass criteria**

- [ ] 8 good messages landed in HDFS; **none** of them on the BOQ
- [ ] Exactly 1 message on `MQ.HPS.MEMBERSHIP.BACKOUT` — the poison one
- [ ] Audit records account for everything: summed `record_count` = 8, and the unit of work that routed the poison shows `backout_count: 1` with `balance_status: "BALANCED"`
- [ ] `mq_intake_balance_check_failures_total` still **0** (a poison message is accounted for, not a balance failure)
- [ ] `mq_intake_poison_routed_total` = 1; `mq_intake_backout_queue_depth` = 1 → **this is a page-worthy alert in production**
- [ ] Health shows `DEGRADED` (HTTP **200**) while isolating, returning to `UP` after 10 clean batches
- [ ] `mq_intake_degraded_entries_total` ≥ 1, `mq_intake_suspect_count` returns to 0

### Test 8 — Rollback leaves nothing behind

**Steps:** make HDFS unwritable mid-run (revoke write on the landing path, or put the NameNode in safemode: `hdfs dfsadmin -safemode enter`), then put 10 messages. Restore afterwards (`-safemode leave`).

**Pass criteria**

- [ ] Log shows the batch write failing and rolling back
- [ ] **All 10 messages remain on the source queue** (nothing acknowledged)
- [ ] No `.seq` file in the partition, and **no leftover file under `_tmp/`**
- [ ] `mq_intake_rollbacks_total` increased; no audit record was written
- [ ] After restoring HDFS, the same 10 messages land normally — no loss

### Test 9 — Audit is fail-closed

**Steps:** make only the **audit** path unwritable; put 10 messages.

**Pass criteria**

- [ ] Batch rolls back — log: "Audit record could not be written … rolling back so no unaudited data is committed"
- [ ] Messages stay on the queue (ingestion stalls — **this is by design**, not an incident)
- [ ] `mq_intake_audit_failures_total` increased
- [ ] After restoring the audit path, the messages land and are audited

---

## Part 5 — Resilience and recovery

### Test 10 — Channel outage (recovery)

```
runmqsc: STOP CHANNEL(TEST.SVRCONN)      # then, after ~60s
runmqsc: START CHANNEL(TEST.SVRCONN)
```

**Pass:** log shows session recovery attempts with growing backoff; after restart consumption resumes with no intervention; `mq_intake_reconnects_total` increased; messages sent during the outage land afterwards; **zero loss**.

### Test 11 — Queue manager restart

Stop and restart the whole QM (`endmqm -i` / `strmqm`).

**Pass:** same as Test 10 — the app recovers on its own, no loss, no manual restart.

### Test 12 — Dead TCP connection (**review condition — chaos 1**)

Kill the network path, not the session: drop the connection with a firewall rule (`iptables -A OUTPUT -p tcp --dport 1414 -j DROP`) or kill the socket, leave it broken ~2 minutes, then restore.

**Pass:** the binding either recovers on its own **or** stops cleanly with `mq_intake_reconnect_failures_total` increasing and health going `DEGRADED`/`DOWN` — i.e. it is *visible*. A silent stall with health still `UP` is a **FAIL**; report it immediately.

### Test 13 — Graceful shutdown drains

**Steps:** while messages are flowing, send `SIGTERM` (`kill <pid>`).

**Pass:** log shows "Draining N messages on shutdown"; the partial batch lands and is audited; no message is lost; process exits cleanly.

### Test 14 — Hard kill before commit (crash window)

**Steps:** while a batch is being processed, `kill -9 <pid>`. Restart the app. Let it run until the queue drains.

**Pass criteria**

- [ ] **No message lost** — every `MessageID` sent appears in HDFS at least once
- [ ] A duplicate file may exist (the crash landed a file whose MQ commit never happened, and MQ redelivered) — **this is expected and acceptable**
- [ ] Any `_tmp` debris from the kill is swept at the next startup
- [ ] Reconciliation (Test 16) classifies the duplicate rather than reporting loss

### Test 15 — Shutdown during in-flight commit (**review condition — chaos 2**)

**Steps:** under sustained load, `SIGTERM` repeatedly at random moments (≥ 10 cycles), restarting each time.

**Pass:** across all cycles, no `MessageID` is missing from HDFS. Duplicates are acceptable; loss is not.

---

## Part 6 — Load and throughput

**Restore production config first** (`batch.size: 4000`, `batch.interval-ms: 0`, 4 listener threads) and restart.

**Steps:** drive a representative production volume — ideally a full peak-hour replay; at minimum several hundred thousand messages sustained.

**Measure**

```bash
curl -s localhost:8080/actuator/metrics/mq_intake_flush_latency_seconds | python3 -m json.tool
curl -s localhost:8080/actuator/metrics/mq_intake_messages_consumed_total | python3 -m json.tool
curl -s localhost:8080/actuator/metrics/jvm.memory.used | python3 -m json.tool
```

**Pass criteria**

- [ ] Consumption keeps up with production arrival rate (source queue depth stable, not climbing)
- [ ] Heap stable across the run — no upward trend, no `OutOfMemoryError` (watch against the 512 MB raw batch budget × payload-copy multiplier)
- [ ] `mq_intake_flush_latency_seconds` steady; no growth over time
- [ ] `mq_intake_balance_check_failures_total` = **0** for the entire run
- [ ] `mq_intake_rollbacks_total` = 0 (or explained)
- [ ] Total records landed = total messages sent (sum `record_count` across audit records)
- [ ] File sizes and counts sane — no partition full of tiny files

---

## Part 7 — Reconciliation (the post-write control)

### Test 16 — A closed partition reconciles clean

**Steps:** stop sending. Wait for the current quarter to close **plus the 5-minute grace period** (up to ~20 min), then watch the log for the reconciliation pass (runs every 15 min).

**Pass:** the pass reports the partition CLEAN — file count and record counts match the audit trail; `mq_intake_reconciliation_discrepancies_total` = 0.

### Test 17 — Reconciliation detects the Test 14 duplicate

If Test 14 produced a duplicate file, the pass covering that partition should report it as an orphan classified `ORPHAN_DUPLICATE` (reported only — `quarantine-duplicates: false` by default) or `ORPHAN_SOLE_COPY` with a retrospective audit written.

**Pass:** the discrepancy is reported with the filename and classification; **no file is deleted** (quarantine, if enabled, is a move to `_quarantine/`).

---

## Part 8 — Results log

| # | Test | Run by | Date | Result | Evidence / notes |
|---|---|---|---|---|---|
| 1 | Clean startup | | | | |
| 2 | Health endpoint | | | | |
| 3 | Metrics endpoint | | | | |
| 4 | Gates refuse (4a–4d) | | | | |
| 5 | Happy path end-to-end | | | | |
| 6 | ABC independence | | | | |
| 7 | Poison → BOQ, balance holds | | | | |
| 8 | HDFS failure → rollback | | | | |
| 9 | Audit failure → fail-closed | | | | |
| 10 | Channel outage recovery | | | | |
| 11 | QM restart recovery | | | | |
| 12 | Dead TCP connection | | | | |
| 13 | Graceful shutdown drain | | | | |
| 14 | Hard kill / crash window | | | | |
| 15 | Shutdown during commit ×10 | | | | |
| 16 | Load / throughput | | | | |
| 17 | Reconciliation clean | | | | |
| 18 | Reconciliation duplicate | | | | |

**Evidence to keep for each test:** application log extract, the audit JSON, `hdfs dfs -ls` output, queue depths before/after, and the relevant metric values.

**Sign-off:** _________________________ (test lead) Date: ____________

---

## Appendix A — Known-acceptable observations

Do not raise these as defects:

- **Duplicates after a crash or forced shutdown.** At-least-once by design; reconciliation classifies them; quarantine never deletes.
- **Ingestion stalling when the audit path is down.** Fail-closed by design — messages wait on the queue.
- **A stopped listener after exhausted recovery.** The binding runs on the remaining threads and reports `DEGRADED` until a restart; deliberate, no in-process thread resurrection.
- **`DEGRADED` / `PARTIAL_OUTAGE` returning HTTP 200.** Intentional — these must alert, never restart the pod.
- **Tracker send failures logged and counted while the message still commits.** Legacy MDB parity.
- **No tracker message for a source message lacking `MessageHeaderDetails`.** The §20.3 null guard.
- **A warning about a payload with no extractable `<MessageID>`** — counted in `mq_intake_identity_misses_total`; the message still lands.
- **Good messages on the backout queue after Test 7 or Test 8.** Backout routing is delivery-count based (legacy MDB behaviour): it cannot tell a malformed message from a good one that sat in several batches which rolled back because HDFS or the audit path was down. Expect this when you deliberately break infrastructure with a low `backout.threshold`. The messages are intact and must be replayed from the BOQ — never discarded. Note it in the results log rather than raising a defect.

## Appendix B — Troubleshooting

| Symptom | Likely cause |
|---|---|
| App refuses to start naming `MQ_HOST`/`QM1`/`DEV.APP.SVRCONN` | production mode armed and env vars unset — set them (Test 4a is this on purpose) |
| App refuses naming a legacy key | an override still uses the pre-migration flat key; the message names the replacement |
| Nothing lands, no errors | fewer than `batch.size` messages and the partition boundary hasn't passed — apply the test overlay (0.4) |
| No tracker messages | source messages lack the `MessageHeaderDetails` property (0.3) |
| `/actuator/metrics` returns 404 | build predates the actuator-exposure fix; use `1bc1655` or later |
| Messages redelivering forever, batch size 1 | a poison message is being isolated; it reaches the BOQ once its backout count exceeds `BOTHRESH` |
| Files land but reconciliation says NOT READY | reconciliation is disabled or identity unapproved for that binding (expected for Claims, not RMS) |

## Appendix C — Claims (if deployed alongside)

Claims is a **separate application/process**; nothing it does affects RMS. Test it only for basic landing: messages arrive on `MQ.DMIH.CLAIMS.IN` and land under `/data/raw/claims/dmih`. Reconciliation, identity, sidecar index and the balance check are intentionally **off** for Claims this release — do not test or raise them.
