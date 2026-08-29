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
| `odp.conf.dir` | `HDFS_CONFIG_RESOURCES` | Same directory. We load `core-site.xml` and `hdfs-site.xml` from it by absolute path |
| `odp.expected.nameservice.prefix` | `HDFS_EXPECTED_NAMESERVICE` | Same semantics: `fs.defaultFS` must contain it |
| `odp.principal` | `KERBEROS_PRINCIPAL` | Plus `KERBEROS_ENABLED=true`, which is off by default |
| `odp.keytab` | `KERBEROS_KEYTAB_PATH` | Must be readable by the account running *this* service, which may not be the WebSphere account |
| `odp.root.directory` | `HDFS_BASE_PATH` | The landing root for the binding |
| — | `HDFS_AUDIT_BASE_PATH` | **No WebSphere equivalent.** New path, must exist and be writable — the audit is fail-closed and stops ingestion if it is not |
| `conf.set("dfs.client.use.datanode.hostname","true")` in `createOdpConfiguration` | `-Dintake.hdfs.properties.dfs.client.use.datanode.hostname=true` | The legacy code sets it unconditionally, so assume this environment needs it |
| `conf.set("fs.hdfs.impl", …)`, `fs.AbstractFileSystem.hdfs.impl` | nothing to port | Only needed because that code uses `new Configuration(false)`. Applies here solely if you enable `intake.hdfs.isolate-configuration` |
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
| `MQ_CREDENTIAL_REF` | same | same | `env:MQ_USER,MQ_PASSWORD` — a reference, not the secret |
| `MQ_USER` | differs | differs | |
| `MQ_PASSWORD` | differs | differs | Never logged; `intake.sh config` prints `<set>` / `<unset>` only |

### Storage and identity

| Variable | Test | Prod | Notes |
|---|---|---|---|
| `HDFS_CONFIG_RESOURCES` | differs if the clusters differ | | The target cluster's conf directory. **Without it Hadoop resolves `file:///` and writes to local disk** |
| `HDFS_EXPECTED_NAMESERVICE` | differs if the clusters differ | | The one check that distinguishes two clusters. Everything else passes against either |
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
export HDFS_CONFIG_RESOURCES=<odp.conf.dir>
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
