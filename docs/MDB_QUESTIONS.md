# Questions for the legacy WebSphere MDB

What the replacement service still needs from the current implementation, in
priority order. Grouped by what each answer unblocks.

Most are answerable by reading the MDB source. Items marked **[human]** need a
consuming team or the source-system team and cannot be settled from code.

Guiding constraint: the replacement should not introduce behaviour the MDB does
not already have. Several questions below exist to establish where our design
has *added* something, so that addition can be kept or dropped deliberately
rather than by accident.

---

## Production format: SequenceFile. Read this before trusting code analysis.

**SequenceFile is the production contract.** The JSONL path
(`OdpJsonLineWriter`, `.tmp` → `.jsonl`) was an *attempt to replace*
SequenceFile that was **backed out**: it brought the WebSphere MQ listeners
down. `AbstractDualWriteIngest` is the residue of that migration.

This matters procedurally, not just factually. A static reading of the MDB
source described the JSONL path as "the current authoritative path" — the
opposite of operational reality. Treat **what is deployed** as the contract,
confirmed by an operator, not what reads as authoritative in the repository.
Two independent conclusions in this document were wrong because they trusted
code shape over deployment reality.

**Open discrepancy to resolve:** if HEAD presents JSONL as authoritative while
production runs SequenceFile, then the deployed artifact is not HEAD — or a
flag/config selects the path. Establish which revision is deployed before
extracting any contract detail from source.

### Settled

| Question | Answer | Confidence |
|---|---|---|
| Production output format | **SequenceFile** | Operator-confirmed |
| Key Writable type | `LongWritable` | High — the 129 header length observed on both feeds is a fingerprint of the key/value class names, and `LongWritable`/`Text` reproduces it exactly |
| Value Writable type | `Text` | High — same fingerprint. `Text` and `BytesWritable` have different wire formats, so this is not cosmetic |
| Compression | `RECORD` | High — with `NONE` the same classes give header 86, not 129 |
| Is `129` a constant key? | Not resolvable yet — see A2 below. `129` is the empty-file header length, so it is what record 1 always receives | — |

### Known-not-live

`HDFSWriter.flushBatchInternal` (batched SequenceFile write) is **not running
in production**, and is internally inconsistent besides — it reads `Foffset`
from `getLength()` but appends using an unresolved `offset`. Useful as evidence
of intended types; not authoritative for behaviour.

### Carried over from the JSONL analysis — verify against the SequenceFile path

These were answered about the JSONL path and may not describe production:

| Detail | JSONL path | Must confirm for SequenceFile path |
|---|---|---|
| Temp-then-rename | Yes, `.tmp` → atomic rename | Does the live SequenceFile path also stage and rename? |
| Durability | `BufferedWriter.flush()`, no `hflush`/`hsync` | What does the live SequenceFile path call? |
| Payload transform | `processMessage` replaces `\n`, `\r`, `\t` each with one space, no `trim()` | Almost certainly shared — `processMessage` sits upstream of `writeToHDFS` — but confirm it applies on the SequenceFile branch too (A6). **Now reproduced** by `PayloadNormalizer` in core; both serializers apply it. If the SequenceFile branch turns out NOT to normalise, this must be reverted. |

---

## A. The live SequenceFile write path — highest priority

`flushBatchInternal` is not it, and the JSONL path is not it. We still have not
seen the code that actually writes production SequenceFiles.

1. ~~Which method writes the production SequenceFile?~~ **ANSWERED —
   `HDFSWriter.write(String message)`.** Call path:

   ```
   HPSHDFSIngest.onMessage(...) | HDFSIngest.onMessage(...)
     → processMessage(...)
     → writeToHDFS(...)
     → odpHdfsWriter.write(...) / hdfsWriter.write(...)
     → HDFSWriter.write(...)
       → updateWriter()
         → openOutput(...) if the path bucket changed
       → sequenceFileWriter.append(new LongWritable(offset), new Text(message))
   ```

   Confirms three things we had inferred: the append really is
   `LongWritable`/`Text` (our declared types are right); rollover is driven by
   the **path bucket changing**, which is what our partition-aligned flush
   reproduces; and `processMessage` runs upstream of the dispatch, so it
   reaches this branch (see A6).

   *Still open:* `writeToHDFS` dispatches to **both** `odpHdfsWriter` and
   `hdfsWriter`. Confirm whether the ODP/JSONL half is still invoked in the
   deployed revision or was disabled with the reverted migration — if both run,
   both formats are live contracts.

2. ~~How many records per file?~~ **ANSWERED — no fixed record count.**
   From `DMIHRiskStrat_ejb/.../hdfs/writer/HDFSWriter.java`: `write(...)` calls
   `updateWriter()` before each append, and `updateWriter()` rolls the file
   **only when `namedPath.getPathName()` changes**. There is no `maxRecords`,
   no record counter, no "N messages then rotate". Records per file are
   variable — all messages written while the path name holds steady — so the
   count depends on message rate per instance during that path window.
   Filenames include path + PID + IP + writerId + instance to keep concurrent
   writers apart.

   *Consequence:* files hold **many** records, so keys vary within a file and
   no consumer-safety argument can be built on "the key is always 129". The
   design's identical samples across two feeds are explained by truncation to
   record 1, which always receives 129. **Metadata Option A remains
   unjustified** pending G32 (does any consumer read the key?).

3. ~~What is `offset`?~~ **ANSWERED — it is the record's BYTE OFFSET in the
   file, not an ordinal.**

   ```java
   updateWriter();
   long offset = sequenceFileWriter.getLength();   // local, re-read every write
   sequenceFileWriter.append(new LongWritable(offset), new Text(message));
   ```

   `offset` is a **local variable**, recomputed from `getLength()` on every
   append — not a field, not a running counter. So the key is the byte position
   at which that record begins. On rollover `updateWriter()` does
   `closeOutput()` then `openOutput(newPath)`, and because the value is
   re-derived per write it does not carry across files: a new file's first
   record gets that file's header length (129), and a file reopened in append
   mode continues from its current byte length.

   This finally explains the `129` samples completely — not a constant, not an
   ordinal, but "first record of a fresh file starts at byte 129".

   **Reproduced.** Both serializers now emit `metadata.getFileByteOffset()`,
   supplied by `SequenceFileBatchWriter` from `writer.getLength()` before each
   append. Verified end to end by `ByteOffsetKeyIntegrationTest`: first record
   of a fresh file lands on 129, offsets grow by encoded record size, and keys
   restart at the header per file.

4. **Does `updateWriter()` stage to a temp file and rename on roll, or write in
   place?** Path-triggered rollover implies the file stays *open across many
   MQ transactions*, and an open file cannot be renamed — so this may be a
   direct write with no visibility barrier at all. If so, our close→rename
   ordering is an addition, and a deliberate improvement rather than parity.
5. ~~Which durability call does `HDFSWriter.write(...)` make?~~ **ANSWERED —
   `hsync()` after every single append.**

   ```java
   sequenceFileWriter.append(...);
   sequenceFileWriter.hsync();
   ```

   This **disproves the durability window** hypothesised earlier. Each record is
   synced to the datanodes before `onMessage()` returns, so the container never
   commits ahead of durable data. The legacy system is safe on this axis.

   What remains is a *visibility* difference, not a durability one: the MDB
   appends into an open file that already sits in the partition directory, so a
   reader scanning that partition can see a partially written file. Our
   `_tmp` → close → rename makes files visible only when complete.

   *Throughput note:* an `hsync()` per message is a round trip to the datanode
   pipeline on every record. That is a significant per-message cost and is
   worth carrying into any sizing comparison — our design syncs once per batch
   and then closes, so it should do far fewer.

6. ~~Is `processMessage`'s whitespace normalisation applied on this branch?~~
   **ANSWERED — yes.** `processMessage` maps `\n`, `\r` and `\t` each to a
   space, and **both** ingest paths call it before `writeToHDFS(...)`, so it
   applies to the SequenceFile branch. `PayloadNormalizer` is therefore correct
   and does **not** need reverting.
7. **What is in `namedPath.getPathName()`?** Its granularity sets the real file
   cadence. If it is time-based, what window — hourly, quarter-hour, daily?
   This is needed to compare file counts (see A″).
8. On crash with a file open mid-window, what is left behind, and does anything
   close or clean it up?

**Unblocks:** the production `RecordSerializer`, the metadata-placement
decision, and therefore the placeholder-serializer startup gate.

## A″. File-count regression risk — raised by the A2 answer

The MDB rolls a file **only when the path name changes**. Our service rolls on
whichever of three triggers fires first: `batch_size` (8000 claims / 4000 RMS),
`batch_bytes` (128 MB), or `batch_interval_ms` (**30 s**), per listener thread.

Those cadences diverge sharply at low volume, in the wrong direction:

| Arrival rate | MDB files per path window | Ours per window (4 threads) |
|---|---|---|
| High | 1 per writer instance (large file) | volume ÷ 8000, size-triggered |
| Low / trickle | **still 1** — file stays open, accumulating | up to one file **per message** |

At trickle volume the 30-second timer dominates and a batch of one flushes to
its own file — **the same one-file-per-message shape as the JSONL attempt that
brought the listeners down**. Nights, weekends, and quiet periods on either
feed would sit in exactly that regime.

This is not necessarily wrong: the 30 s interval buys freshness, and unlike the
MDB we close and rename so data is visible promptly rather than trapped in an
open file until the window ends. But it is a real trade the design should make
knowingly, given the one operational failure we know about on this path was
file-count related.

Questions:

- What is the actual arrival-rate profile per feed, including quiet periods?
- Is `batch_interval_ms = 30 s` driven by a stated freshness requirement, or is
  it a default?
- Should flushing align to the **partition boundary** (as the MDB effectively
  does via path change) rather than a fixed timer, so quiet windows produce one
  file instead of many?

## A′. The failed JSONL migration — read this as a design constraint

The JSONL attempt was reverted because **the WebSphere MQ listeners came
down**. That is the most valuable operational signal we have, and it bears
directly on the replacement's risk.

7. **What was the actual failure mechanism?** The likely chain is: one file per
   message → NameNode/RPC pressure → HDFS writes slow → `onMessage` blocks →
   listener thread pool exhausts → WebSphere stops the listener port. Confirm,
   because it determines whether the replacement is immune.
8. Was the trigger file *count*, write *latency*, or transaction timeout?
9. At what volume did it fail, and on which feed?

**Why it matters:** our design batches, so it creates far fewer files — which
addresses the presumed cause directly. But it also holds a transaction open
across N messages, so slow HDFS produces long-running transactions and MQ log
pressure instead. Different mechanism, same root cause. The replacement is not
automatically safe just because it writes fewer files, and this incident is the
closest thing to a load test anyone has run on this path.

## B. File lifecycle and visibility

6. Is there `_tmp` staging followed by a rename into the partition, or does the
   MDB write **directly** into the target directory?
   *(If direct: our rename-based visibility barrier is an addition, not parity.
   Worth an explicit decision.)*
7. What closes/rolls a file — message count, byte size, elapsed time, or one
   file per message?
8. Filename convention, exactly. Our audit and reconciliation derive identity
   from filenames.
9. Partition directory layout — does it match `year=/month=/day=/hour=/quarter=`?
   Is the path computed per write or cached at startup?
10. On crash mid-write, what is left behind, and does anything clean it up?

**Unblocks:** whether our `_tmp`/rename/partition-stamping design is parity or
addition; the reconciliation file-discovery logic.

## C. RMS tracker contract

The four artifacts named in DESIGN §20.4, all expected to be in the MDB source:

11. ~~Contents of `tagList`~~ **CAPTURED:** ReportingSystem, SourceSystem,
    DestSystem, MesgStatus, CreatedTimeStamp (in that order).
12. ~~`ROOT_END_TAG` / `ROOT_END_TAG_CHAR`~~ **CAPTURED:**
    `</MessageHeaderDetailsType>` and `&lt;/MessageHeaderDetailsType&gt;`.
    Note the element is `MessageHeaderDetailsType`, not `MessageHeaderDetails`
    — our placeholder had the latter, which would have made every splice miss.
    Escape lists are `["<", "&lt;"]` and `[">", "&gt;"]`, index 0 raw, 1 escaped.
13. **Bodies of `setReplacedTagData` and `buildResultData` — STILL MISSING, and
    now the only thing blocking the RMS gate.** `getStringMessageHeader` is
    captured, so the surrounding algorithm is known:

    ```
    for each tag in tagList:
        build start/end tags in raw and escaped form
        if header contains raw start   -> header = setReplacedTagData(header, raw start, raw end)
        else if contains escaped start -> header = setReplacedTagData(header, esc start, esc end)
        if header contains ROOT_END_TAG      -> buildResultData(sb, raw start, raw end, 4 values)
        else if contains ROOT_END_TAG_CHAR   -> buildResultData(sb, esc start, esc end, 4 values)
    if sb non-empty: append the matching root end tag, then replace it in header
    ```

    **Why it cannot be inferred:** `buildResultData` receives all four values
    (`reportingSystem`, `sourceSystem`, `messageStatus`, `destinationStatus`) on
    every iteration, so which tag consumes which is decided inside it. Two
    specific unknowns: `DestSystem` does not obviously map to the parameter
    named `destinationStatus`, and `CreatedTimeStamp` has no supplied value at
    all — presumably a generated timestamp, but of what format? Guessing
    produces well-formed tracker messages carrying wrong values, which is worse
    than failing to start.
14. A before/after `MessageHeaderDetails` sample — still the cheapest way to
    validate whatever #13 turns out to be.

    **Also captured, from the call site:** the values actually passed are
    reportingSystem `DMIH/DL`, sourceSystem `IIB`, messageStatus `RCVD`,
    destinationStatus empty — matching `TrackerFields.defaultRms()`. Body is a
    full copy (`session.createTextMessage(textMessage.getText())`), confirming
    `FULL_COPY` and closing DESIGN item #25. Null-header early return confirms
    our suppress behaviour.

15. ~~Is the tracker session ever committed?~~ **ANSWERED — yes, by the
    container, not by application code.**

    `EJBHelper.forwardToMessageTracker(...)` creates a transacted session with
    `connection.createSession(true, 0)` and never calls `session.commit()`.
    Read alone that looks like a defect. It is not: **the MDBs rely on
    container rollback by throwing exceptions out of `onMessage()`**, which is
    the container-managed (CMT) idiom — bean-managed code would drive
    `UserTransaction` explicitly. Under CMT the session is enlisted in the
    container's transaction and committed at method end, and an explicit
    `session.commit()` would itself be an error.

    So DESIGN item #26's suspicion does **not** hold, and the §20.4 tracker
    work is real: trackers are being delivered and there is a live consumer
    contract to preserve. The RMS gate must be satisfied, not removed.

    *One narrow thing left to confirm:* enlistment requires the tracker
    connection factory to be a **container-managed / XA-capable** resource
    (JNDI `java:comp/env`, WebSphere-managed). If `EJBHelper` instead
    instantiates an `MQConnectionFactory` directly, the session would be
    locally transacted, outside JTA, and genuinely never committed. Check how
    that factory is obtained. The empirical check (production tracker queue
    `MSGENQCOUNT`, or asking the consumers) still outranks reading the code.

16. Is the tracker put in the **same transaction** as the source get?
17. Does the tracker message carry the full source body, or only the rewritten
    header properties? (DESIGN item #25 — `FULL_COPY` roughly doubles RMS MQ
    traffic and log volume.)
18. What happens when `MessageHeaderDetails` is absent? We suppress the send;
    confirm that matches.

**Unblocks:** the RMS tracker startup gate, which currently blocks RMS
production entirely.

## D. Identity and duplicate handling — scope-defining

19. ~~Does the MDB extract any identity/GUID from the payload?~~ **ANSWERED —
    no.**
20. ~~Is there any dedup, idempotency, or replay-detection logic?~~
    **ANSWERED — no.**

    **This is scope-defining and now actionable.** Record identity exists in our
    design solely to serve reconciliation and §10 orphan classification —
    neither of which the MDB has. So the claims-identity gate guards a
    capability the legacy system never provided, and under a "nothing new
    beyond the MDB" rule it is optional:

    - **Keep reconciliation** → open item #17 (approved claims identity) and
      item #2 (metadata placement) must both be answered, and metadata needs a
      home the production layout allows — Option C (sidecar), since the key is
      a byte offset and the value is contractual.
    - **Drop reconciliation** → the claims-identity gate and the metadata
      placement decision both disappear, along with a code blocker. What is
      lost is any automated way to classify a file that landed without an audit
      record, which is the §10 safety net for at-least-once duplicates.

21. **[human]** Claims identity — **partially answered, and it is neither
    candidate DESIGN proposed.** Source documentation (confidential; only the
    decision-relevant facts recorded here) gives two keys:

    - **Claim ID** — a surrogate key generated by HCSC for their database.
    - **Natural key** — DCN + corp/plan code + receive date + sequence number.

    Neither is `CLM_XMITSN_ID` nor `REC_CTL_NBR`, so the DESIGN §9.2 candidate
    list is superseded.

    **The open question is narrower now: is either unique per MESSAGE?** The
    documentation says "Claim ID is used to identify a message", but the same
    claim generates Insert / Update / Partial / Delete events, which would all
    carry the same Claim ID. That makes it a **grouping key** in §9.2 terms, not
    the payload-GUID equivalent RMS has. Reconciliation needs per-message
    uniqueness to compare identity sets between files.

    Ask specifically: does any single field, or the natural key including its
    sequence number, uniquely identify one *message* rather than one *claim*?
    If nothing does, claims reconciliation cannot use identity-set comparison
    regardless of how open item #2 resolves, and the keep/drop decision on
    reconciliation is effectively made for us.

**Why this is scope-defining:** if the MDB extracts no identity and does no
dedup, then record-level identity exists in our design solely to serve
reconciliation and §10 orphan classification — capabilities the MDB does not
have. Dropping reconciliation would remove the claims-identity gate entirely.
Keeping it requires answering 21.

## E. Transaction and failure semantics

22. ~~Is the MDB container-managed or bean-managed?~~ **ANSWERED —
    container-managed.** The MDBs rely on container rollback by throwing
    exceptions out of `onMessage()`. Still useful to capture the exact
    transaction attribute (`Required`, etc.) and whether the MQ and tracker
    connection factories are XA/managed resources — that last point is what
    C15's remaining question turns on.
23. Is the HDFS write inside the MQ transaction, and in which order relative to
    the commit?
24. On HDFS failure, does the MQ transaction roll back, or is the message
    acknowledged and the data lost?
25. How is a repeatedly-failing message handled — backout queue, discard, or
    infinite redelivery?
26. Does the MDB read `JMSXDeliveryCount`, MQMD `BackoutCount`, or neither?
27. Are `BOTHRESH` / `BOQNAME` actually configured on the production queues, and
    to what values?

**Unblocks:** confirmation that our transactional ordering (close → rename →
commit) and application-owned poison handling are improvements on a known
baseline rather than guesses about one.

## F. Validation, audit, operations

28. Is `validationService` live in production, and what does it do? Does it
    write anything comparable to our audit records?
29. Does anything today reconcile landed files against expected counts, or
    classify files with no audit record?
30. How is Kerberos handled — relogin inline per write, scheduled, or at
    startup only?
31. **[human]** DESIGN item #27: the hardcoded `hdpapp` credential in
    `EJBHelper.java` — where else does it appear, and who owns rotation?

## G. Consumer-side — cannot be answered from MDB code

32. **[human]** Does any downstream consumer **read the SequenceFile key**?
    This is now the deciding question for metadata placement: the key varies
    per record, so the original "it is a constant, safe to repurpose" argument
    no longer holds. If any consumer reads it, Option A is out and Option C
    (sidecar file, data files untouched) is the only choice that preserves both
    parity and reconciliation.
33. **[human]** How does each consumer parse the value — strict positional
    parsing, or tolerant? (Determines whether Option B is survivable.)
34. **[human]** DESIGN item #24: validate the captured tracker format against
    the actual tracker-queue consumers before cutover.

---

## H. Environment topology — from the RMS/tracker connection details

Test-environment connection details reviewed 2026-08-23. Specific hostnames,
queue-manager names, queue names and the service account are **deliberately not
recorded here** — this repository is not the right place for environment
identifiers. They belong in deployment config held wherever that environment's
secrets live. Only the structural facts are captured, which is all the design
needs.

The RMS tracker destination is presented on **two distinct queue managers**,
same port and channel, same queue name on each.

**They are two independent queue managers, not a multi-instance pair.** A
multi-instance QM shares one name across hosts; these have different names. So
the same queue name exists on both and traffic is spread between them — meaning
**both must be consumed**, and a message's tracker put must go to the tracker
queue on *its own* queue manager, since the put shares the transacted session
with the get.

Consequences for this service:

- Modelled as **two bindings**, one per `mq-connection`. That is already what
  the binding model supports. A connection-name-list would be wrong here: that
  gives failover semantics, but these messages genuinely live on both.
- **Fixed:** `validateNoDuplicateSourceQueues` keyed uniqueness on queue name
  alone and would have rejected this configuration at startup as a false
  duplicate. It now keys on (mq-connection, queue name).
- Each binding gets its own `DegradedModeManager`, metrics and health entry.
  That is correct — a poison loop on one queue manager must not degrade the
  other — but it means one logical feed reports as two bindings, which
  dashboards and alerting need to expect.

Open:

35. Are the two queue managers **active/active** (traffic shared) or
    **active/passive** (one idle until failover)? Active/active means both
    bindings run continuously; active/passive means one sits idle and its lag
    and health signals need interpreting differently.
36. Does the **source** queue for each feed follow the same two-QM pattern, or
    only the tracker destination?
37. Are `BOTHRESH`/`BOQNAME` configured identically on both queue managers? A
    binding's threshold must match the queue manager it actually consumes.
38. Does the aggregate memory ceiling still hold once a feed is two bindings?
    `batch_bytes × listener_threads` is now counted twice per logical feed.
