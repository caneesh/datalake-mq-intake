# Deployment

One page. Longer references: `TEST_DEPLOYMENT_GUIDE.md` (mechanics),
`DEPLOYMENT_CHECKLIST.md` (production sign-off), `PROPERTY_REFERENCE.md` (every property),
`WEBSPHERE_HOST_DEPLOYMENT.md` (why the HDFS config variables below matter so much).

## 1. What you deploy

Two independent Spring Boot apps, on **two separate queue managers**. Deploy each to its own
base directory, with its own `env.sh`.

| | `rms` | `claims` |
|---|---|---|
| Queue manager | its own | its own, different |
| Source queue | consumed | consumed |
| Tracker queue | **yes** — one ack per message | none (LAND_ONLY) |
| Backout queue | yes | yes |
| Base directory | e.g. `~/mq-intake-rms` | e.g. `~/mq-intake-claims` |
| `env.sh` | its own | its own |

Within one module, its source, tracker and backout queues must all live on **that module's**
queue manager — the tracker and backout producers come off the listener's own transacted
session and cannot reach a sibling QM. Across the two modules nothing is shared: different
queue managers, different queues, different credentials.

## 2. Ship it

Give each module its own base directory — that separation is what keeps the two queue
managers apart.

```bash
./scripts/deploy.sh rms    user@host ~/mq-intake-rms
./scripts/deploy.sh claims user@host ~/mq-intake-claims
```

No network path to the server? Build a bundle, carry it across, install with the same installer:

```bash
./scripts/bundle.sh rms                        # -> dist/mq-intake-rms-<stamp>.tar.gz
# on the server:
tar xzf mq-intake-rms-<stamp>.tar.gz && cd mq-intake-rms-<stamp> && ./install.sh
```

Deploy **never** starts, stops, or overwrites configuration. Starting is step 4.

## 3. Set the variables

**One file per module: `<base>/env.sh`** on the server. The first install seeds it from the
template and chmods it 600; every later deploy leaves it alone.

```bash
ssh user@host
cd ~/mq-intake-rms && vi env.sh        # then again for ~/mq-intake-claims
```

> **Never share one `env.sh` between the two modules.** Both read the *same* variable
> names — `MQ_HOST`, `MQ_QUEUE_MANAGER`, `MQ_SOURCE_QUEUE`, `MQ_BACKOUT_QUEUE`,
> `MQ_USER`/`MQ_PASSWORD` — and differ only in their built-in defaults. A shared file
> silently points one app at the other's queue manager. Separate base directories keep them
> apart; that is the whole mechanism. The same applies to `preflight.sh`: run it from the
> module's own directory, or it will probe the wrong queue manager and pass.

### IBM MQ

| Variable | Notes |
|---|---|
| `MQ_HOST`, `MQ_PORT` | queue manager host and listener port |
| `MQ_QUEUE_MANAGER` | this module's queue manager. Its own queues must all be on **this** QM — the tracker and backout producers come off the listener's own transacted session and cannot reach a sibling QM |
| `MQ_CHANNEL` | SVRCONN channel |
| `MQ_SOURCE_QUEUE` | queue to consume |
| `MQ_TRACKER_QUEUE` | **rms only** — where the ack goes |
| `MQ_BACKOUT_QUEUE` | poison messages land here |
| `MQ_CREDENTIAL_REF` | `env:MQ_USER,MQ_PASSWORD` — a reference, never the secret itself |
| `MQ_USER`, `MQ_PASSWORD` | one account per queue manager. rms needs GET on source + PUT on tracker and backout; claims needs GET on source + PUT on backout |

Ask the MQ team, **for both queue managers**, to set `BOTHRESH` and `BOQNAME` on each source
queue to match its app (rms: 5, claims: 14) and `MAXUMSGS` ≥ 2 × batch size.

### HDFS

| Variable | Notes |
|---|---|
| `HDFS_CONFIG_RESOURCES` | path to the **target** cluster's conf dir. Without it Hadoop resolves `fs.defaultFS` to `file:///` and writes to local disk, silently and successfully |
| `HDFS_EXPECTED_NAMESERVICE` | `dfs.nameservices` from that `hdfs-site.xml`. The only check that catches a wrong conf dir — everything else passes either way |
| `HDFS_BASE_PATH` | where data lands |
| `HDFS_AUDIT_BASE_PATH` | audit records; must be writable or batches roll back |

### Kerberos (if the cluster is secured)

`KERBEROS_ENABLED=true`, `KERBEROS_PRINCIPAL`, `KERBEROS_KEYTAB_PATH`.

### Behaviour

| Variable | Notes |
|---|---|
| `MQ_INTAKE_PRODUCTION=true` | arms every startup gate. Leave on anywhere that stands in for production |
| `JAVA_OPTS=-Xmx4g` | floor. Startup fails if the batch budget exceeds 50% of max heap |
| `CLAIMS_IDENTITY_FIELD` | **claims only, required in production** — `CLM_XMITSN_ID` or `REC_CTL_NBR`, once the data owner confirms which. Startup is blocked until set |
| `STOP_TIMEOUT_SECONDS` | how long `stop` waits for the drain (default 90) |

Leave `INTAKE_INSTANCE_ID` unset — it derives as `hostname-pid`, which is what keeps two
JVMs on one host from sharing a `_tmp` tree.

YAML overrides beyond these go in `<base>/config/application.yml`, which also survives deploys.

## 4. Start it

```bash
cd ~/mq-intake-rms                # and separately for ~/mq-intake-claims
./current/intake.sh preflight     # proves MQ + HDFS. Consumes nothing, starts nothing
./current/intake.sh start
./current/intake.sh status
```

**Preflight must pass first.** It connects with the deployment's own configuration, checks
one fact at a time, names the fix for each failure, and exits non-zero if anything fails —
so a pipeline can gate on it. Narrow it with `preflight mq`, `hdfs`, or `app`.

## 5. Day to day

```bash
./current/intake.sh status        # pid, release, health, key metrics
./current/intake.sh logs -f
./current/intake.sh stop          # graceful: drains and commits in flight
./current/intake.sh restart
./current/intake.sh config        # effective settings; secrets shown as set/unset only
```

Health and metrics: `http://localhost:8080/actuator/health` and `/actuator/metrics`.

`DOWN` (503) means every binding is unhealthy. `PARTIAL_OUTAGE` and `DEGRADED` return 200
deliberately — this service serves no traffic, so a 503 only restarts the pod and interrupts
the bindings that are still working. Alert on the status string and per-binding metrics.

Worth an alert: `mq_intake_balance_check_failures_total` (any increment = a message dropped
pre-commit, page it), `mq_intake_backout_queue_depth` (non-zero = poison sitting), and for rms
`mq_intake_tracker_sent_total` flatlining while `mq_intake_messages_consumed_total` climbs
(data landing unacknowledged).

## 6. Roll back

```bash
cd ~/mq-intake-rms                # the modules roll back independently
./current/intake.sh stop
ln -sfn releases/<previous-stamp> current
./current/intake.sh start
```

Always message-safe: unprocessed messages queue on MQ, landed files stay landed and audited.
`env.sh` and `config/` live outside the release directories and survive both directions.

## 7. Where things are

| Path | |
|---|---|
| `<base>/env.sh` | environment and credentials, chmod 600, survives deploys |
| `<base>/config/` | optional YAML overrides, survives deploys |
| `<base>/current/` | symlink to the active release |
| `<base>/releases/` | last five releases — the rollback targets |
| `<base>/logs/current.log` | log of the running instance |
| `<base>/run/intake.pid` | pid |

## 8. If startup fails

It is almost always a gate doing its job. The message names the cause; the common ones:

| Message mentions | Fix |
|---|---|
| dev defaults (`localhost`, `QM1`, `DEV.APP.SVRCONN`) | the env vars were not exported — check `env.sh` |
| nameservice mismatch | `HDFS_CONFIG_RESOURCES` points at the wrong cluster's conf |
| `claims.identity-field` | set `CLAIMS_IDENTITY_FIELD` |
| placeholder serializer | expected for claims; `CLAIMS_ACCEPT_PLACEHOLDER=true` is already the default |
| batch budget vs heap | raise `-Xmx` or lower `batch.bytes` / `listener-threads` |
| `MQRC 2035` in preflight | this module's MQ account lacks GET or PUT on one of its queues |
| the wrong queue manager in `config` output | the two modules are sharing an `env.sh` — see step 3 |
