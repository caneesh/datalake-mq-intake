# Local throughput verification

Run from the repository root with Java 11 and the Maven dependencies already cached:

```bash
# Preloaded burst: 1,200 messages, each exactly 600 KiB, four listener threads.
bash scripts/performance/run.sh rms hdfs 1200 0
bash scripts/performance/run.sh claims hdfs 1200 0

# Offer 1,000 messages/minute for about two minutes, then drain residual batches.
bash scripts/performance/run.sh rms hdfs 2000 16.6666666667
bash scripts/performance/run.sh claims hdfs 2000 16.6666666667
```

`hdfs` starts an isolated Hadoop MiniDFSCluster with three DataNodes and replication
factor three. It needs local socket access. `local` uses Hadoop's local filesystem
for a quick smoke check; it does not measure HDFS. The launcher uses a 6 GiB maximum
heap and eight JVM-visible processors. Keep runs sequential to avoid resource
contention. A 1,200-message run needs several GiB of free memory and temporary disk.
Do not select a message count that exceeds available resources: burst inputs are
preloaded into an in-memory broker.

Logs are saved under `target/throughput/run-*.log`. Successful runs print one JSON
line prefixed `THROUGHPUT_RESULT`; any integrity check failure exits nonzero. The
generated broker and HDFS data live in a unique temporary directory and are removed
at the end. No external MQ queues or HDFS paths are used. Application source and
configuration are not modified.

Rate-limited runs also emit samples every 250 messages: committed count, outstanding
source messages (including uncommitted receives), and heap use. Heap includes the
embedded broker and HDFS; RMS retains tracker messages until final verification.
These samples cannot establish the application's production heap requirement.

## What the check measures

- Production hand-rolled receive loop, a transacted session per listener, production
  SequenceFile writer using RECORD compression and hsync, close/rename before commit,
  audit emission, poison screening, and shared degradation manager.
- RMS uses its current serializer and FULL_COPY tracker builder. Claims uses the
  current placeholder serializer with its explicitly nonproduction fixture identity
  extractor. Sidecar indexes are disabled. ABC balance checking is enabled for both.
- RMS message limit 1,000; claims 8,000; both use the shipped 128 MiB byte limit,
  four listeners, and no interval timer. At 600 KiB, a full byte-triggered batch has
  219 records. The real quarter-hour boundary can also trigger a flush.
- ASCII XML payloads have unique fixed-width identities and seeded pseudorandom
  alphanumeric padding. This avoids unrealistically compressible repeated-character
  payloads; it is still synthetic data, not a production payload distribution.
- After timing, every landed payload is compared byte-for-byte with its input, with
  checks for missing/duplicate identities, committed and written counts, audit record
  totals, zero rollbacks/backouts/balance failures, and an empty source queue. RMS
  additionally checks all tracker payloads and identities and zero suppressions.

The broker is **embedded, nonpersistent ActiveMQ over VM transport with prefetch
zero**, including when messages request persistent delivery. It does not measure
IBM MQ persistence, network/TLS, channel limits, or queue-manager commit cost.
The three HDFS DataNodes share one host/disk and do not model a distributed cluster
or Kerberos. Spring startup, scheduled reconciliation, health polling, production
JVM sizing, and recovery under load are outside this check.

Burst timing begins after input preload and includes listener startup, all commits,
and graceful shutdown of residual partial batches. Rate-limited timing begins after
listener readiness and includes message production, all commits, and the same drain.
Neither includes cluster startup or post-run output verification. There is no warmup
exclusion; short runs are startup-sensitive. `committedBeforeShutdownDrain` separates
normal running commits from the shutdown tail. A rate-limited run's overall rate
will be slightly below its offered rate because its denominator includes the tail.
Use burst runs for local capacity evidence and rate-limited runs for behavior under
the requested arrival rate. Neither is a sustained production soak certification.

## Production acceptance

For 1,000/minute at 600 decimal KB, ingress is 10 MB/s. These tests use 600 KiB,
which requires 10.24 MB/s (9.765625 MiB/s) at that rate. RMS also puts a full payload
copy onto the tracker queue. At even distribution across four listeners, filling
219-message batches takes about 52.6 seconds before write/commit overhead; this
accumulation latency must fit the freshness requirement.

In the deployment environment, run representative payloads through real IBM MQ
and HDFS with the intended JVM settings. Compare the per-binding delta of
`mq_intake_messages_consumed_total` over elapsed seconds against 16.67/s, alongside
`mq_intake_messages_written_total` and independent source queue depth from MQ.
Use windows spanning several batches; counters advance at commit and include
backout-only commits, so consumption alone does not prove successful landing.
Check zero unexpected changes to `mq_intake_batches_rolled_back_total`,
`mq_intake_poison_routed_total`, and `mq_intake_balance_check_failures_total`.
Verify payload/audit totals, RMS tracker delivery, bounded backlog, heap/GC behavior,
and latency over a peak-hour replay or soak. Counter resets invalidate a simple
two-sample rate. No production endpoint or workload was supplied for this check.
