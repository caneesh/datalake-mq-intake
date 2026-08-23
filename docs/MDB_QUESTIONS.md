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

## Already answered — do not re-ask

| Question | Answer | How |
|---|---|---|
| SequenceFile key Writable type | `LongWritable` | Header length 129 on both feeds is a fingerprint of the key/value class names; `LongWritable`/`Text` reproduces it exactly. Corroborated by `HDFSWriter.flushBatchInternal`. |
| SequenceFile value Writable type | `Text` | Same fingerprint. Note: `Text` and `BytesWritable` have different wire formats, so this is not cosmetic. |
| Compression type | `RECORD` | With `NONE` the same classes give header 86, not 129. |
| Is the observed key `129` a constant? | **No** | 129 is the header length of an empty file, so `getLength()` returns it and record 1 always gets it. Samples were truncated to record 1. This invalidates the stated justification for metadata Option A (§9.1). |

---

## A. The live write path — highest priority

`HDFSWriter.flushBatchInternal` is **not running in production**, so everything
observed in it describes dormant code. We need the path that actually executes
per message.

1. Which method runs in production when a message arrives? Full call path from
   `onMessage` to the SequenceFile append.
2. The exact `append(...)` call on that path — same `LongWritable`/`Text`?
   (The 129 fingerprint says the types match; confirm the call site.)
3. **What is the key value?** `getLength()` (a true byte offset, growing
   through the file), a per-record ordinal, or something else? Both patterns
   give `129` for record 1, so the production sample cannot distinguish them.
4. Is the value the JMS `TextMessage` body **verbatim**, or transformed —
   trimmed, re-encoded, wrapped, header stripped? Anything done here is part of
   the contract consumers see.
5. Which durability call precedes close: `hflush()`, `hsync()`, or neither?
   *(We currently call `hflush`. If production calls `hsync` we are weaker; if
   it calls neither we are stronger — either way we should choose knowingly.)*

**Unblocks:** the production `RecordSerializer` for both feeds, and therefore
the placeholder-serializer startup gate.

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
