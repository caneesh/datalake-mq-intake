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

## ⚠ Premise correction — production does not write SequenceFiles

Answers received 2026-08-22 establish that the **live** MDB path writes
**JSONL**, not SequenceFile:

```
HDFSIngest.onMessage / HPSHDFSIngest.onMessage
  → extractMessageContent(...) | extractMessageContentHps(...)
  → isValidMessage(...)
  → processMessage(...)
  → writeToHDFS(processedMessage, messageId, ingestionTs)   [AbstractDualWriteIngest]
  → odpJsonLineWriter.write(message, messageId, ingestionTs) [OdpJsonLineWriter]
  → ugi.doAs(...) → writeMessageSync(...)
  → create temp .tmp → write ONE JSON line → close → atomic rename to .jsonl
```

`SequenceFile.Writer.append(...)` is **never called on the active path**. It
exists only in `HDFSWriter.flushBatchInternal`, which is dormant *and*
internally inconsistent (`Foffset` is read from `getLength()` while the append
uses an unresolved `offset`), so it is not authoritative for anything.

**Consequences:**

- DESIGN §8/§9.1 is built on a SequenceFile contract. If consumers read
  `.jsonl`, that entire framing — and the Option A/B/C metadata decision —
  addresses a format the landing zone may no longer produce.
- Our `SequenceFileBatchWriter` may be targeting the wrong format outright.
- The `129` key observation in DESIGN cannot describe current output. It must
  come from historical files, a different path, or a different system.

**Retracted** (previously listed here as settled):

| Previously stated | Status |
|---|---|
| Key type `LongWritable` | **Retracted** — no SequenceFile key is written on the live path |
| Value type `Text` | **Retracted** — value is a JSON field, not a Writable |
| Compression `RECORD` | **Retracted** — no SequenceFile is produced |
| `129` is not a constant | Still true arithmetically (it is the empty-file header length), but moot if no SequenceFile is written |

**Still valid and useful:**

| Question | Answer |
|---|---|
| Does the MDB stage via temp and rename? (B6) | **Yes** — `.tmp` → close → atomic rename to `.jsonl`. Our rename-based visibility barrier is **parity, not an addition**. |
| Durability call before close? (A5) | `BufferedWriter.flush()` then close, then rename. **No `hflush`/`hsync`** on the live path. Our `hflush` is therefore *no weaker* than production. |
| Is the payload verbatim? (A4) | **No — transformed.** `processMessage` replaces `\n`, `\r`, `\t` each with a single space. No `trim()`. Result stored as JSON field `payload` in `JsonLineRecord`. Parity requires applying the same normalisation. |
| File granularity | **One JSON line per file** — one file per message. At claims volumes this is a NameNode small-file problem, and a strong motivation for the replacement's batching. |

---

## A. Output format — now the highest-priority open question

Section A as originally posed is **answered** (see the premise correction
above). It is replaced by a larger question the answers exposed.

1. **Which format is the contract the replacement must produce — `.jsonl` or
   SequenceFile?** Everything else depends on this. Our writer currently
   produces SequenceFiles; the live MDB produces JSONL.
2. **What does `AbstractDualWriteIngest` dual-write to?** The name implies two
   destinations. Is the SequenceFile path a disabled second destination, a
   different landing zone, or dead code retained from an earlier design? If
   dual-write is live, **both** formats are contracts.
3. **What is the exact `JsonLineRecord` schema?** Field names, types, ordering,
   null handling. `messageId`, `ingestionTs` and `payload` are known; the full
   set is not. This is the real metadata contract.
4. **Where did DESIGN's SequenceFile samples (key `129`) come from?** Historical
   files, a different zone, or another system? If historical, §8/§9.1 describes
   a superseded contract.
5. **[human]** Do consumers read `.jsonl`, SequenceFile, or both today? Ask the
   landing-zone consumers directly — this decides the answer to A1.

**Unblocks:** the `BatchWriter` implementation itself, not merely the
serializer. If the target is JSONL, `SequenceFileBatchWriter` is the wrong
component and metadata placement becomes "add JSON fields", collapsing the
Option A/B/C decision entirely.

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
