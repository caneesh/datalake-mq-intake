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
| Payload transform | `processMessage` replaces `\n`, `\r`, `\t` each with one space, no `trim()` | Almost certainly shared — `processMessage` sits upstream of `writeToHDFS` — but confirm it applies on the SequenceFile branch too. **Parity requires reproducing this**; our serializers currently store the body untouched. |

---

## A. The live SequenceFile write path — highest priority

`flushBatchInternal` is not it, and the JSONL path is not it. We still have not
seen the code that actually writes production SequenceFiles.

1. **Which method writes the production SequenceFile?** Full call path from
   `onMessage` to `SequenceFile.Writer.append(...)`, in the **deployed**
   revision.
2. **How many records per file?** This is the decisive question for metadata
   placement. If each file holds exactly **one** record, then every production
   file has key `129` — not because the key is constant by design, but because
   `getLength()` on an empty file returns the header length and there is never a
   second record. That would explain the identical samples across two unrelated
   feeds far better than truncation does, and it would mean no consumer *can*
   depend on a varying key. If files hold many records, keys run
   `129, 130, 131, …` and the key carries positional meaning.
3. **What is the key expression** on that path — `getLength()`, a counter, or a
   constant?
4. Does the live path stage to a temp file and rename, or write directly into
   the partition?
5. Which durability call precedes close: `hflush()`, `hsync()`, or neither?
6. Is `processMessage`'s whitespace normalisation applied on this branch?

**Unblocks:** the production `RecordSerializer`, the metadata-placement
decision, and therefore the placeholder-serializer startup gate.

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

11. Contents of `tagList` — the actual tag names rewritten.
12. `ROOT_END_TAG` and `ROOT_END_TAG_CHAR` values.
13. Bodies of `getCompleteStartTag`, `getCompleteEndTag`, `setReplacedTagData`,
    `buildResultData`.
14. A sample `MessageHeaderDetails` value **before and after** rewriting —
    DESIGN calls this "the highest-value artefact for validating a
    reimplementation".

Behavioural questions on the same path:

15. **Is the tracker session ever committed?** DESIGN open item #26 suspects it
    is not, which per spec would roll back on close — meaning tracker messages
    may not be arriving in production at all. Answer this *first*: it decides
    whether this is a rewrite or a repair, and could make items 11–14 moot.
16. Is the tracker put in the **same transaction** as the source get?
17. Does the tracker message carry the full source body, or only the rewritten
    header properties? (DESIGN item #25 — `FULL_COPY` roughly doubles RMS MQ
    traffic and log volume.)
18. What happens when `MessageHeaderDetails` is absent? We suppress the send;
    confirm that matches.

**Unblocks:** the RMS tracker startup gate, which currently blocks RMS
production entirely.

## D. Identity and duplicate handling — scope-defining

19. Does the MDB extract any **identity/GUID** from the payload at all, for any
    purpose? For RMS (`MessageID`) and for claims separately.
20. Is there any dedup, idempotency, or replay-detection logic anywhere, keyed
    on anything?
21. **[human]** For claims specifically: is there an approved stable identity in
    the payload — `CLM_XMITSN_ID`, `REC_CTL_NBR`, or a wrapper field? (DESIGN
    open item #17, owner: source system team.)

**Why this is scope-defining:** if the MDB extracts no identity and does no
dedup, then record-level identity exists in our design solely to serve
reconciliation and §10 orphan classification — capabilities the MDB does not
have. Dropping reconciliation would remove the claims-identity gate entirely.
Keeping it requires answering 21.

## E. Transaction and failure semantics

22. Is the MDB container-managed or bean-managed? What transaction attribute?
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
