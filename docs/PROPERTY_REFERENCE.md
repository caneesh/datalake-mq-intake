# Property reference — test and production

Every value the service reads, where to put it, and how to prove it is right. This is the canonical list; the [test guide](TEST_DEPLOYMENT_GUIDE.md) and the [production checklist](DEPLOYMENT_CHECKLIST.md) both point here rather than repeating it.

The storage half of this list does not have to be invented. **The WebSphere application on that host already connects to the target cluster successfully**, which means the working values exist on disk and in a running JVM. Read them off the host rather than requesting them.

## Where values go

Three places, and the choice is not stylistic.

| Where | What belongs there | Why |
|---|---|---|
| `<base>/env.sh` | Everything environment-specific: MQ endpoint and queues, credentials, HDFS paths, Kerberos, cluster config | Deployed once, never overwritten by an upgrade. Holds secrets, so `chmod 600` |
| `<base>/config/application.yml` | Behaviour: batch size, thresholds, listener threads | Survives upgrades, applies to every release |
| `JAVA_OPTS` in `env.sh` | Hadoop property overrides and JVM/security system properties | These are `-D` system properties, not environment variables |

> **The collection trap.** Spring takes a whole list from one source. A `config/application.yml` mentioning `intake.bindings` at all must contain the **entire** `bindings:` block — a file with only `intake.bindings[0].batch.size` produces a binding with nothing else set, and the service fails with `Binding 'null' must specify mq-connection`. The same applies to `--intake.bindings[0].x=y` and `INTAKE_BINDINGS_0_X`. Scalars outside the list (`intake.hdfs.audit-base-path`, `intake.mq-connections.primary.host`) override individually and are safe — which is why every environment-specific leaf in the shipped YAML is a `${VAR}` placeholder instead.

## Reading the working values off the WebSphere host

The legacy application loads `odp.*` from its property map. Locate the running JVM and its configuration:

```bash
# The WebSphere JVM and its -D system properties
ps -ef | grep -i [w]ebsphere
tr '\0' '\n' < /proc/<pid>/cmdline | grep -E "krb5|odp|hadoop"

# The property file those odp.* values are read from
grep -rn "odp\." <was-app-config-dir>/*.properties

# What the keytab actually contains — confirms the principal spelling
klist -kt <keytab-path>

# Confirm the conf directory is the one you think it is
grep -n "fs.defaultFS"     <odp-conf-dir>/core-site.xml
grep -n "dfs.nameservices" <odp-conf-dir>/hdfs-site.xml
```

### Mapping the working values across

| WebSphere property | Set in intake as | Notes |
|---|---|---|
| `odp.hadoop.conf.dir` | `HDFS_CONFIG_RESOURCES` | Same directory. We load `core-site.xml` and `hdfs-site.xml` from it by absolute path |
| `odp.expected.nameservice.prefix` | `HDFS_EXPECTED_NAMESERVICE` | Same semantics: `fs.defaultFS` must contain it |
| `odp.principal` | `KERBEROS_PRINCIPAL` | Plus `KERBEROS_ENABLED=true`, which is off by default |
| `odp.keytab` | `KERBEROS_KEYTAB_PATH` | Must be readable by the account running *this* service, which may not be the WebSphere account |
| `<binding>RootDirectory` (e.g. the claims / membership root properties) | `HDFS_BASE_PATH` | The landing root, per binding. **The root transfers; the layout underneath it does not** — see [the layout comparison](WEBSPHERE_HOST_DEPLOYMENT.md#where-the-files-land-and-how-that-differs-from-the-legacy-feed) |
| `user` (HDFS user) | nothing to port | **Not the same as the principal.** With Kerberos the effective user is the principal's short name; there is no proxy-user support. Confirm that user can write both roots |
| `namenodes` (host:port list) | nothing to port | We resolve the NameNodes from the conf directory's HA configuration instead. Still worth having for the firewall request |
| `fileCloseInterval`, `fileNameBuildertype`, `timeFormat`, `fileName` | nothing to port | Legacy partitioning and naming. This service has its own, and they do not match |
| — | `HDFS_AUDIT_BASE_PATH` | **No WebSphere equivalent.** New path, must exist and be writable — the audit is fail-closed and stops ingestion if it is not |
| `conf.set("dfs.client.use.datanode.hostname","true")` in `createOdpConfiguration` | `-Dintake.hdfs.properties.dfs.client.use.datanode.hostname=true` | The legacy code sets it unconditionally, so assume this environment needs it |
| `conf.set("fs.hdfs.impl", …)`, `fs.AbstractFileSystem.hdfs.impl` | nothing to port | Only needed because that code uses `new Configuration(false)`. Applies here solely if you enable `intake.hdfs.isolate-configuration` |
| `odp.enabled` | nothing to port | That flag exists because the legacy application writes to two clusters. This service writes to one, and it is required — there is no disabled mode |
| `hadoop.security.authentication` | nothing to port | `KerberosManager` sets it before login |
| `UserGroupInformation.setLoginUser(ugi)` | nothing to port | Separate JVM; `loginUserFromKeytab` already establishes the login user |
| `java.security.krb5.conf` (if set on the WAS JVM) | `-Djava.security.krb5.conf=<path>` in `JAVA_OPTS` | Only if the host's default `/etc/krb5.conf` is not the one that works |

## The variables

All are set in `env.sh` unless the row says otherwise. "Differs" means the two environments will not share a value; "same" means they should not diverge.

### MQ

| Variable | Test | Prod | Notes |
|---|---|---|---|
| `MQ_HOST` | differs | differs | |
| `MQ_PORT` | differs | differs | Confirm with the MQ team; do not assume 1414 |
| `MQ_QUEUE_MANAGER` | differs | differs | All three queues must live on **this** queue manager — the tracker and backout producers come off the listener's own transacted session and cannot reach a sibling QM |
| `MQ_CHANNEL` | differs | differs | SVRCONN name. **If the channel requires TLS this is a blocker** — not wired into the connection factory |
| `MQ_SOURCE_QUEUE` | differs | differs | |
| `MQ_TRACKER_QUEUE` | differs | differs | RMS only; Claims ignores it |
| `MQ_BACKOUT_QUEUE` | differs | differs | Must exist on the same QM |
| `MQ_CREDENTIAL_REF` | same | same | `env:MQ_USER,MQ_PASSWORD` — a reference, not the secret. **One credential serves all three queues**, see below |
| `MQ_USER` | differs | differs | |
| `MQ_PASSWORD` | differs | differs | Never logged; `intake.sh config` prints `<set>` / `<unset>` only |

> **One credential per binding, not one per queue.** JMS authenticates at the *connection*, and a listener thread's consumer, tracker producer and backout producer are all created from one transacted session on that connection — which is what makes a batch's consume, tracker send and backout put commit or roll back together. So the account named by `MQ_CREDENTIAL_REF` must hold GET on the source queue and PUT on the tracker and backout queues.
>
> If the legacy application uses a separate account for one of those queues, that is a WebSphere resource-definition convention, not necessarily an authority restriction — ask the MQ team whether the consuming account *can* be granted the missing authority before assuming it cannot. Splitting them in this service would mean a second connection outside the transaction, which breaks the atomicity above and is a change to the delivery guarantee, not a configuration option.
>
> Preflight answers it directly: `<binding>.tracker-queue.output` and `.backout-queue.output` open those queues as the configured account, and `MQRC 2035` there means exactly this.

### Storage and identity

| Variable | Test | Prod | Notes |
|---|---|---|---|
| `HDFS_CONFIG_RESOURCES` | required | required | The target cluster's conf directory; differs whenever test and production target different clusters. **Without it Hadoop resolves `file:///` and writes to local disk** |
| `HDFS_EXPECTED_NAMESERVICE` | required | required | Must match the cluster `HDFS_CONFIG_RESOURCES` points at. The one check that distinguishes two clusters — everything else passes against either |
| `HDFS_BASE_PATH` | differs | differs | Landing root, must exist and be writable by the principal |
| `HDFS_AUDIT_BASE_PATH` | differs | differs | Created if absent; unwritable stops ingestion at the first batch |
| `KERBEROS_ENABLED` | `true` | `true` | Defaults to `false`. A secured cluster rejects you without it |
| `KERBEROS_PRINCIPAL` | may differ | may differ | Verify against `klist -kt` |
| `KERBEROS_KEYTAB_PATH` | differs | differs | Readable by the service account |

### Behaviour and runtime

| Variable | Test | Prod | Notes |
|---|---|---|---|
| `MQ_INTAKE_PRODUCTION` | **`true`** | `true` | Arms every startup gate. **Set it in test too** — otherwise you are testing a more permissive service than the one you will promote |
| `JAVA_OPTS` | `-Xmx4g` floor | same | Batch budget is threads × `batch.bytes` (RMS: 4 × 128 MB); startup fails if it exceeds 50% of max heap |
| `INTAKE_INSTANCE_ID` | leave unset | leave unset | Derives as `hostname-pid`. Two JVMs sharing an id share a `_tmp` tree and can collide on filenames |
| `STOP_TIMEOUT_SECONDS` | 90 | 90 | How long `stop` waits for the drain before reporting |

### Hadoop overrides — set via `JAVA_OPTS`, not as environment variables

```bash
export JAVA_OPTS="$JAVA_OPTS \
  -Dintake.hdfs.properties.dfs.client.use.datanode.hostname=true"
```

Any Hadoop key can be set this way; it is applied after the site files and overrides them. Add `-Djava.security.krb5.conf=<path>` here too if the default location is not the right one.

## Proving each value before consuming anything

`./current/intake.sh preflight` starts no listener and consumes no message. Each check maps to specific values:

| Check | Proves |
|---|---|
| `<binding>.connection` | `MQ_HOST`, `MQ_PORT`, `MQ_QUEUE_MANAGER`, `MQ_CHANNEL`, `MQ_USER`, `MQ_PASSWORD`, `MQ_CREDENTIAL_REF` |
| `<binding>.source-queue.input` | `MQ_SOURCE_QUEUE` resolves **on the connected QM** |
| `<binding>.tracker-queue.output` | `MQ_TRACKER_QUEUE` |
| `<binding>.backout-queue.output` / `.browse` | `MQ_BACKOUT_QUEUE` |
| `cluster-config.resources` | `HDFS_CONFIG_RESOURCES` — and prints which files it read |
| `filesystem.connect` | Kerberos login worked, and the filesystem is not the local disk |
| `filesystem.nameservice` | `HDFS_EXPECTED_NAMESERVICE` — **a `SKIP` here means it is unset and nothing is guarding the cluster identity** |
| `<binding>.landing-path` | `HDFS_BASE_PATH` exists and is writable |
| `<binding>.audit-path` | `HDFS_AUDIT_BASE_PATH` |
| `<binding>.durability-roundtrip` | write → hsync → close → rename → read-back, the exact sequence a batch performs |
| `production-mode` | `MQ_INTAKE_PRODUCTION` — must report **ARMED** |

Run `preflight mq` first: it needs none of the storage values, so it is useful before the cluster side is settled.

**A check that cannot reach its dependency does not fail quickly.** Hadoop retries a NameNode it cannot resolve, and a firewall that drops packets rather than refusing them leaves a TCP connect waiting on the socket. Each check is therefore bounded at 30s and reported as `did not answer within 30s`, which is itself diagnostic: it means the address resolved but nothing answered. Lower it with `--intake.preflight.check-timeout-ms=<ms>` when probing an environment you expect to be unreachable, since every storage check pays the timeout separately.

## Worked `env.sh`

Identical in shape for test and production; only the values differ. Copy from `env.sh.example`, never from another environment's live file.

```bash
# ---- MQ ----
export MQ_HOST=<from MQ team>
export MQ_PORT=<from MQ team>
export MQ_QUEUE_MANAGER=<from MQ team>
export MQ_CHANNEL=<SVRCONN name>
export MQ_SOURCE_QUEUE=<queue>
export MQ_TRACKER_QUEUE=<queue>          # RMS only
export MQ_BACKOUT_QUEUE=<queue>
export MQ_CREDENTIAL_REF="env:MQ_USER,MQ_PASSWORD"
export MQ_USER=<service account>
export MQ_PASSWORD=<secret>

# ---- Target cluster (values from the working WebSphere configuration) ----
export HDFS_CONFIG_RESOURCES=<odp.hadoop.conf.dir>
export HDFS_EXPECTED_NAMESERVICE=<odp.expected.nameservice.prefix>
export HDFS_BASE_PATH=<odp.root.directory or the agreed landing path>
export HDFS_AUDIT_BASE_PATH=<new audit path>

# ---- Identity ----
export KERBEROS_ENABLED=true
export KERBEROS_PRINCIPAL=<odp.principal>
export KERBEROS_KEYTAB_PATH=<odp.keytab>

# ---- Behaviour ----
export MQ_INTAKE_PRODUCTION=true
export JAVA_OPTS="-Xmx4g -Dintake.hdfs.properties.dfs.client.use.datanode.hostname=true"
export STOP_TIMEOUT_SECONDS=90
```

## What still cannot be read off the host

The WebSphere application does not consume from the queues this service will consume from, so **nothing on the MQ side can be lifted from it**. Channel name, port, credentials and queue names all still have to come from the MQ team, and whether the channel requires TLS remains the one answer that could block rather than delay.
