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

## 2. Get from the MQ admin

### Ask this first — the answer can change the design

**Can one account hold every authority a binding needs?** JMS authenticates at the *connection*,
and a listener thread's consumer, tracker producer and backout producer all come from one session
on it. So per queue manager, a single account must hold:

| Queue | Authority | Used by |
|---|---|---|
| Source | GET | the consumer |
| Tracker (rms only) | PUT | tracker producer, same session |
| Backout | PUT | poison routing, same session |
| Backout | **BROWSE** | the depth monitor samples with a `QueueBrowser` |

BROWSE is the one that gets missed — a separate authority from PUT. Without it
`backout_queue_depth` never populates, and that gauge is the pager condition. Preflight probes
all four; `MQRC 2035` on any of them means this.

If their security model cannot give one account all of these, say so early: it changes the
design, not the configuration.

### Values to type into `env.sh` — once per queue manager

Host, listener port, queue manager name, SVRCONN channel name, the queue names (source, backout,
plus tracker for rms), and the account and password.

> **The channels are plain TCP/IP — settled, and worth keeping settled.** The client supports no
> other kind: `MqConnectionManager` sets host, port, queue manager, channel and transport type,
> and there is no cipher spec, no keystore and no CCDT support anywhere in the codebase. Moving
> either channel to TLS later is a code change, not a configuration value, so it needs notice
> rather than a change window.

### Settings only they can apply

The application cannot set these, and a mismatch fails under load rather than at startup.

| Setting | On | Value |
|---|---|---|
| `BOTHRESH` | each source queue | **5** rms, **14** claims (BISECT needs ≥ ceil(log2 8000) + 1) |
| `BOQNAME` | each source queue | the matching backout queue |
| `MAXUMSGS` | each queue manager | ≥ 2000 rms (TRACKED, unit of work is 2N), ≥ 8000 claims. The 10000 default suffices — confirm it was not lowered |
| `MAXMSGL` | claims source queue **and its channel** | IBM MQ defaults to 4 MB; claims has messages over 10 MB. Unraised, the largest messages cannot be put or got at all |
| `MAXMSGL` | rms tracker queue | the tracker body is a `FULL_COPY` of the source payload, so it needs the source queue's ceiling |
| `MAXDEPTH` | rms tracker queue | a full tracker queue now **stops rms ingestion** rather than silently dropping acks |

### Two judgement calls that need their sign-off

**`BOTHRESH` sizing against plausible outage length.** Poison detection is delivery-count only,
and infrastructure failures deliberately do not shrink the batch, so every message in every
rolled-back batch accrues delivery count at full rate. A long enough outage pushes undamaged
messages onto the backout queue. This is legacy-MDB parity, not a regression — but the mitigation
is sizing `BOTHRESH` above realistic outage windows, and that is their call.
See `READINESS_REVIEW.md` §D″ item 5.

**Validating the tracker message against its live consumers.** The rewritten
`MessageHeaderDetails` is reproduced from the legacy EJBHelper source and pinned by tests, but
nobody has confirmed the output against the consumers on the other side of the tracker queue.
An operational check at cutover, not a code gate (DESIGN item #24).

### What you do not need to ask for

MQ admin access. Preflight substitutes for it — it opens each queue as the configured account and
reports one actionable line per failure, consuming nothing and writing nothing outside
`_tmp/{instanceId}`. Run `./current/intake.sh preflight mq` from each module's directory and send
them the output; it is usually a faster conversation than requesting a `DISPLAY QLOCAL`.

## 3. Ship it

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

Deploy **never** starts, stops, or overwrites configuration. Starting is step 5.

## 4. Set the variables

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

The queue-manager-side settings that must match these are in step 2.

#### Setting `MQ_CREDENTIAL_REF`

It names **where the secret lives**, never the secret. Two forms work:

```bash
# Preferred — two variables
export MQ_CREDENTIAL_REF="env:MQ_USER,MQ_PASSWORD"
export MQ_USER=svc_dmih_rms
export MQ_PASSWORD='...'

# Or one variable holding username:password
export MQ_CREDENTIAL_REF="env:MQ_CREDS"
export MQ_CREDS='svc_dmih_rms:s3cr3t'
```

The single-variable form splits on the **first** colon, so a password may contain colons but
a username may not. The two-variable form avoids that rule and keeps the password out of a
value that also carries the username.

Three things to know before you set it:

- **Leaving it blank connects with no credentials**, and that is a legitimate configuration —
  it is what you use when the channel accepts the process identity. Blank is not an error.
- **Once set, it fails closed.** If the reference is present and does not resolve, startup
  refuses rather than connecting anonymously. Missing variables, only one of the two set, or
  a lookup that throws all fail with the reference named and the secret never logged. That
  is deliberate: a credential-store outage or a rotation that removed the entry would
  otherwise downgrade you to an anonymous connect, which on a queue manager permitting
  anonymous binds succeeds *with different authority than intended*.
- **The `env:` prefix is required.** `MQ_CREDENTIAL_REF="MQ_USER,MQ_PASSWORD"` resolves to
  nothing and startup refuses — it is not a partial success.

`MQ_USER`/`MQ_PASSWORD` are read from the process environment, so they must be `export`ed
in this module's own `env.sh`. A credential fault classifies as a *configuration* error, so
it fails on the first attempt instead of retrying to exhaustion and burying the cause.
Prove it with `./current/intake.sh preflight mq`, which resolves the credential and connects
without consuming anything.

### HDFS

| Variable | Notes |
|---|---|
| `HDFS_CONFIG_RESOURCES` | path to the **target** cluster's conf dir, or a comma-separated list of XML files. Without it Hadoop resolves `fs.defaultFS` to `file:///` and writes to local disk, silently and successfully |
| `HDFS_EXPECTED_NAMESERVICE` | `dfs.nameservices` from that `hdfs-site.xml`. The only check that catches a wrong conf dir — everything else passes either way |
| `HDFS_BASE_PATH` | where data lands |
| `HDFS_AUDIT_BASE_PATH` | audit records; must be writable or batches roll back |

> **A directory loads `core-site.xml` and `hdfs-site.xml`, and nothing else.** Anything
> else the cluster team put there — `ssl-client.xml`, `hive-site.xml`, `yarn-site.xml` — is
> ignored without comment. If the cluster needs one of them (`ssl-client.xml` for HDFS wire
> encryption or RPC privacy is the usual case), name the files instead of the directory:
>
> ```bash
> export HDFS_CONFIG_RESOURCES=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml,/etc/hadoop/conf/ssl-client.xml
> ```
>
> The directory form is the right default; it just cannot tell you about a file it was
> never going to read.

### Kerberos (if the cluster is secured)

`KERBEROS_ENABLED=true`, `KERBEROS_PRINCIPAL`, `KERBEROS_KEYTAB_PATH`.

### Behaviour

| Variable | Notes |
|---|---|
| `MQ_INTAKE_PRODUCTION=true` | arms every startup gate. Leave on anywhere that stands in for production |
| `JAVA_OPTS=-Xmx4g` | floor. Startup fails if the batch budget exceeds 50% of max heap |
| `JAVA_HOME` | only needed when the host's `PATH` resolves to a Java older than 11 — see below |
| `CLAIMS_IDENTITY_FIELD` | **claims only, required in production** — `CLM_XMITSN_ID` or `REC_CTL_NBR`, once the data owner confirms which. Startup is blocked until set |
| `STOP_TIMEOUT_SECONDS` | how long `stop` waits for the drain (default 90) |

Leave `INTAKE_INSTANCE_ID` unset — it derives as `hostname-pid`, which is what keeps two
JVMs on one host from sharing a `_tmp` tree.

YAML overrides beyond these go in `<base>/config/application.yml`, which also survives deploys.

### Running on a host whose Java is too old

The service needs Java 11 and runs as **its own process**, so it does not have to use
whatever JVM the host's other applications use. A WebSphere host pinned to Java 8 is the
case this exists for: give the intake its own runtime and nothing else on the host changes.

The launcher picks a Java in this order, and prints which one it chose in
`./current/intake.sh config`:

1. `<base>/jre/bin/java` — a runtime unpacked into the deployment
2. `$JAVA_HOME/bin/java` — set in `env.sh`
3. `java` from `PATH`

Prefer the first. It travels with the deployment, so a cron entry, a fresh login shell and
a colleague's session all get the same JVM without anyone remembering to prepend anything:

```bash
cd ~/mq-intake-rms
tar xzf /tmp/jdk-11-linux-x64.tar.gz
mv jdk-11* jre                 # <base>/jre, beside env.sh — survives deploys
./current/intake.sh config     # java : ... [bundled .../jre]
```

A `JAVA_HOME` that is set but does not hold `bin/java` is an error, not a reason to fall
through to `PATH` — naming the wrong runtime is a different mistake from naming none, and
silently using a third one would hide it.

### Landing on local disk instead of a cluster

Only useful on a box with no cluster to point at — a first smoke test, or a demo. There is
**no environment variable for it**; it is YAML only, deliberately, because it is not a thing
to turn on from a shell by accident:

```yaml
# <base>/config/application.yml
intake:
  hdfs:
    allow-local-filesystem: true
```

Without it, startup in production mode refuses to run on `file:///` and preflight's
`cluster-config.resources` check fails naming `HDFS_CONFIG_RESOURCES`. That refusal is the
feature: a run that lands on the server's own disk otherwise succeeds, reports healthy, and
writes a clean audit trail, with only the data in the wrong place.

## 5. Start it

```bash
cd ~/mq-intake-rms                # and separately for ~/mq-intake-claims
./current/intake.sh preflight     # proves MQ + HDFS. Consumes nothing, starts nothing
./current/intake.sh start
./current/intake.sh status
```

**Preflight must pass first.** It connects with the deployment's own configuration, checks
one fact at a time, names the fix for each failure, and exits non-zero if anything fails —
so a pipeline can gate on it. Narrow it with `preflight mq`, `hdfs`, or `app`.

---

## Day to day

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

## Roll back

```bash
cd ~/mq-intake-rms                # the modules roll back independently
./current/intake.sh stop
ln -sfn releases/<previous-stamp> current
./current/intake.sh start
```

Always message-safe: unprocessed messages queue on MQ, landed files stay landed and audited.
`env.sh` and `config/` live outside the release directories and survive both directions.

## Where things are

| Path | |
|---|---|
| `<base>/env.sh` | environment and credentials, chmod 600, survives deploys |
| `<base>/config/` | optional YAML overrides, survives deploys |
| `<base>/current/` | symlink to the active release |
| `<base>/releases/` | last five releases — the rollback targets |
| `<base>/logs/current.log` | log of the running instance |
| `<base>/run/intake.pid` | pid |

## If startup fails

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
