# Throughput verification — September 13, 2026

The local burst measurements exceed the requested **1,000 messages/minute at
approximately 600 KB/message** for both bindings. This is evidence of local
application-path capacity, not certification of sustained production IBM MQ → HDFS
throughput.

## Measured burst capacity

Each run preloaded 1,200 messages of exactly 600 KiB (614,400 bytes). Timing includes
listener startup, HDFS landing, audit emission, MQ transaction commit, and graceful
shutdown of residual batches. Input preload and output verification are excluded.

| Binding | Elapsed | Messages/second | Messages/minute | Raw MiB/second | Multiple of target |
|---|---:|---:|---:|---:|---:|
| RMS, TRACKED / FULL_COPY | 22.85 s | 52.52 | 3,151 | 30.77 | 3.15× |
| Claims, LAND_ONLY | 29.91 s | 40.12 | 2,407 | 23.51 | 2.41× |

These are single cold burst runs, not statistically established maxima or a
comparison of relative binding performance. Mean HDFS flush times were 11.65 s
for RMS and 10.73 s for claims; listener writes overlap. The writer's flush timing
includes serialization, compression, sync, close, and rename, but excludes the
subsequent tracker/audit/commit stages.

## Arrival-rate checks

Each binding was offered 2,000 messages at 16.6667/second for about two minutes.
The denominator below includes completion of all outstanding work and final partial
batches, so the finite-run completion rate is lower than the offered arrival rate.

| Binding | Input duration | All commits complete | Time after input ended | Committed before shutdown drain |
|---|---:|---:|---:|---:|
| RMS | 119.94 s | 132.34 s | 12.39 s | 1,971 / 2,000 |
| Claims | 119.94 s | 129.33 s | 9.39 s | 1,752 / 2,000 |

RMS source outstanding counts, **including uncommitted receives**, were 1,000 at
60 s, 374 at 75 s, 874 at 105 s, and 905 at 120 s. Commits occur in batches; the
drop after the first write cycle confirms progress. This short observation does
not establish long-term backlog or heap stability. RMS's completion rate including
the final drain was 907 messages/minute; its separate burst result is the capacity
measurement. At input completion, RMS had committed 1,095 messages; another 876
committed normally before the last 29 were flushed by shutdown.

Claims outstanding counts were 1,000 at 60 s, 374 at 75 s, 436 at 105 s, and 686
at 120 s. Its completion rate including drain was 928 messages/minute. It had
committed 1,314 messages at input completion, then another 438 before shutdown
flushed the last 248. Both rate-limited runs passed all integrity checks. Neither
demonstrates that every arriving message becomes visible within that same minute.

## Integrity and configuration

Every successful run checked the full contents and unique identities of all landed
records, committed/written counts, audit record totals, an empty source queue, and
zero rollbacks, balance failures, audit failures, or poison routing. RMS also checked
every full-copy tracker payload and identity, with no missing/suppressed trackers.
Across the four measured runs, **6,400 landed records**, **6,400 audited records**,
and **3,200 RMS tracker records** were verified, with no missing or duplicate payloads.

The harness uses the production receive loop, current serializers, SequenceFile
writer, shared degradation manager, poison screening, RMS tracker builder, and HDFS
audit emitter. Close/rename precede MQ commit. It leaves application code and shipped
configuration unchanged. Four listeners use batch limits of 1,000 for RMS and 8,000
for claims, a 128 MiB byte limit, no interval timer, RECORD compression, and hsync.
ABC balance checking is enabled for both; sidecar indexes are disabled.

600 KiB messages reach the byte limit at **219 messages per listener**. At an even
share of 1,000/minute across four listeners, accumulation alone takes approximately
**52.6 seconds**, plus write/commit time. Raw in-flight payload at four full batches
is approximately 513 MiB before copies and overhead. These settings can meet a
throughput requirement while still missing a shorter freshness requirement. No
freshness target was supplied, so batch settings were not changed.

## Environment and limits

- Repository application revision: `951d171`; Java Temurin 11.0.25; Hadoop 3.3.6.
- Intel Core i7-10510U, four cores/eight logical CPUs, about 39 GiB total host RAM;
  JVM `-Xms1g -Xmx6g -XX:ActiveProcessorCount=8`.
- Real HDFS MiniDFSCluster: one NameNode and three DataNodes, replication three,
  all on the same host and local disk. Logs include slow disk write/sync warnings.
- Embedded ActiveMQ with nonpersistent storage, VM transport, and prefetch zero.
  This omits IBM MQ disk logging, network/TLS, channel limits, and real MQ commit
  latency. RMS tracker messages are retained until verification.
- Synthetic ASCII XML with seeded random alphanumeric padding and unique IDs;
  compressed files are approximately 75% of raw payload size. Real payload shapes
  and compressibility may differ. Claims uses its current placeholder serializer
  and nonproduction fixture identity extractor.
- No Spring lifecycle/reconciliation load, Kerberos, production JVM sizing, failure
  injection under load, peak-hour replay, or long soak. Heap samples include the
  broker, HDFS, and retained trackers and cannot size the standalone application.

The requested decimal-KB workload requires 10 MB/s of raw ingress. Using 600 KiB
in these tests raises that to 10.24 MB/s (9.77 MiB/s). RMS additionally sends a full
payload copy to its tracker queue. Production acceptance still requires a replay
against the intended IBM MQ/HDFS deployment, with per-binding committed/written
rates, independently measured MQ queue depth, verified tracker/audit totals, and
stable memory and latency over a meaningful peak interval.

## Reproduce

See [the harness instructions](../scripts/performance/README.md). Detailed results
are preserved in [the JSON measurements](THROUGHPUT_RESULTS_2026-09-13.json), with
local run-log paths. The load-test guide was also corrected to use the shipped
RMS batch size and the actual `mq_intake_batches_rolled_back_total` metric.
