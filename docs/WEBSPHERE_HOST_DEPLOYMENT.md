# Deploying alongside another Hadoop client

The intake service is being deployed onto a host it does not own. That host already runs a WebSphere application with its own Hadoop client, its own cluster configuration on disk, and its own Kerberos identity. We are there for one reason: **the MQ port is open from this host and not from the intended one.**

Everything in this document follows from that single fact. The MQ side is unchanged and already proven. The storage side has one new failure mode, and it is the quiet kind.

## The failure mode worth understanding first

Point the service at the wrong Hadoop configuration directory and it will **connect, authenticate, create directories, write files, rename them into partitions, emit a balanced audit trail, and report healthy** — against the other cluster.

Nothing downstream can detect this. The paths exist. The permissions are correct. Reconciliation balances, because it reads the same wrong cluster it wrote to. The only symptom is that the data is not where anyone is looking for it, and by the time someone notices, it has been landing there for however long the service has been up.

Two properties close this, and they are the substance of the change:

- **`intake.hdfs.config-resources`** decides which cluster's configuration is loaded. Nothing is inherited from the host — see below.
- **`intake.hdfs.expected-nameservice`** decides which cluster is *acceptable*. `fs.defaultFS` is checked against it before anything connects, and startup fails if it does not match.

The first without the second is not enough. A wrong path is an ordinary mistake; the nameservice check is what makes an ordinary mistake loud.

## Why the host's configuration cannot leak in (VERIFIED)

Hadoop finds `core-site.xml` and `hdfs-site.xml` on the **classpath**. The service is launched by `scripts/server/intake.sh` as:

```bash
java $JAVA_OPTS -jar "$JAR" ...
```

`java -jar` ignores both `$CLASSPATH` and `-cp`. The classpath is therefore the application jar and nothing else, so the only Hadoop XML in scope is the `*-default.xml` packaged inside our own Hadoop 3.3.6 jars. **The host's cluster configuration is not reachable unless someone names it explicitly in `intake.hdfs.config-resources`.**

This is why `new Configuration(false)` is not the default here. Skipping Hadoop's packaged defaults is how the WebSphere application defends itself, and it must — inside a container the classpath is genuinely shared and hostile. It also costs: `Configuration(false)` discards the implementation bindings and every tuned client default, which is why that code then sets `fs.hdfs.impl` and `fs.AbstractFileSystem.hdfs.impl` back by hand. In a standalone process that trade buys nothing we do not already have.

It remains available. `intake.hdfs.isolate-configuration=true` builds with `new Configuration(false)` and restores the two implementation bindings exactly as the WebSphere code does. One property, no code change, if the Hadoop team prefers the faithful port.

## Separate process, not a WebSphere deployment

This runs as its own JVM on the shared host. It is worth stating because it removes a whole category of concern that applies to the existing application and not to this one:

| Concern | Status here |
|---|---|
| Two Hadoop clients sharing a `FileSystem` cache | Not possible — one JVM, one `FileSystem`, one UGI |
| `UserGroupInformation.setLoginUser` affecting another client | Not possible — nothing else is in this JVM |
| Needing `FileContext` to bypass the static cache | Not needed — the cache key is `(scheme, authority, ugi)` and only ours is in it |
| `fs.hdfs.impl.disable.cache` | **Do not set it.** It leaks a `FileSystem` per lookup with nothing closing them |

If this ever moves *inside* the WebSphere JVM, all four rows change and so does the Kerberos model. Treat that as a different deployment, not a variation on this one.

## Configuration

The cluster-side values do not have to be requested: the WebSphere application on this host already reaches the target cluster, so its working `odp.*` values can be read off the host and mapped across. [Property reference](PROPERTY_REFERENCE.md) has the commands for finding them and the property-by-property mapping.

| Property | Environment variable | Required | Purpose |
|---|---|---|---|
| `intake.hdfs.config-resources` | `HDFS_CONFIG_RESOURCES` | **Yes** | Target cluster's conf directory, or the two XML files directly. Missing entries fail startup |
| `intake.hdfs.expected-nameservice` | `HDFS_EXPECTED_NAMESERVICE` | **Yes** | `fs.defaultFS` must contain it. The wrong-cluster guard, and the whole reason this deployment is different |
| `intake.hdfs.properties` | `-D` via `JAVA_OPTS`, or `config/application.yml` | Conditional | Hadoop keys the cluster XML does not carry. See DataNodes below |
| `intake.hdfs.isolate-configuration` | — | Optional | `new Configuration(false)`. Default `false` |
| `intake.kerberos.enabled` | `KERBEROS_ENABLED` | **Yes** — must be `true` | Off by default; a secured cluster rejects you without it |
| `intake.kerberos.principal` | `KERBEROS_PRINCIPAL` | **Yes** | Service principal |
| `intake.kerberos.keytab-path` | `KERBEROS_KEYTAB_PATH` | **Yes** | Readable by the service account; existence and readability are checked at startup |
| `intake.kerberos.relogin-interval-ms` | — | Default 1h | TGT refresh for a long-running consumer |
| `intake.bindings[0].hdfs.base-path` | `HDFS_BASE_PATH` | **Yes** | Landing path on the target cluster |
| `intake.hdfs.audit-base-path` | `HDFS_AUDIT_BASE_PATH` | **Yes** | Audit path. Fail-closed: unwritable stops ingestion |

Setting a Hadoop property override from `env.sh`:

```bash
export JAVA_OPTS="$JAVA_OPTS -Dintake.hdfs.properties.dfs.client.use.datanode.hostname=true"
```

## DataNodes: why NameNode connectivity proves nothing

An HDFS write does not go to the NameNode. The client asks the NameNode where to write, receives a **pipeline of DataNode addresses**, and connects directly to those DataNodes. A host can reach the NameNode perfectly and fail every single write.

`dfs.client.use.datanode.hostname=true` makes the client use DataNode **hostnames** instead of the IP addresses the NameNode returns. This matters when the client sits outside the cluster's network segment — DNS resolves the names, but the raw addresses are not routable. The existing WebSphere application sets this property explicitly, which is direct evidence that this environment needs it.

Two consequences for the network request:

- Firewall rules must cover **every DataNode**, not just the NameNodes.
- **Forward and reverse DNS** must work for every DataNode from this host. Reverse lookups matter beyond routing: Kerberos service-principal matching uses them.

## Where the files land, and how that differs from the legacy feed

The legacy application writes to the same cluster, so the landing roots can be lifted from its properties. **The layout underneath them cannot.** These are different feeds sharing a tree, not a drop-in replacement, and the difference is invisible until a downstream consumer looks in the wrong directory.

| | Legacy | This service |
|---|---|---|
| Partition path | `<root>/YYYY/MM/dd/HH/mm` | `<root>/year=YYYY/month=MM/day=DD/hour=HH/quarter=Q` |
| Time zone | Not stated in its config — verify; a bare `timeFormat` is the JVM default, i.e. server local | **UTC**, always |
| Quarter encoding | Minute of the boundary (`00`/`15`/`30`/`45`) | `quarter=0..3` |
| File name | `messages…` / `HPSmessages…` (configured) | `{binding}_{instance}_{epochMs}_{batchSeq}.seq` |
| Close trigger | Fixed interval (`fileCloseInterval`) | Batch full, batch interval, or partition boundary |

Two consequences worth settling before cutover, both with the data owners rather than in code:

- **A consumer globbing `messages*` or walking `YYYY/MM/dd` will not see our output.** Nothing errors; the files are simply somewhere else under a different name.
- **If the time zones differ, the same wall-clock hour is a different directory.** For a cluster whose consumers assume local time, UTC partitions are shifted by the host's offset — which looks like missing data for part of every day, and duplicated data at the boundary.

Reconciliation is unaffected either way: it only enumerates `year=…/quarter=N` directories and only files ending `.seq`, so legacy files in the same tree are never read and never classified as orphans. Verified in `PartitionReconciliationService` and `OrphanFileClassifier`.

Landing in a **separate root** during parallel running keeps the two feeds legible and makes the comparison easy. Landing in the **same root** is safe mechanically but leaves two layouts interleaved in one tree.

## Which identity actually writes

The legacy configuration names an HDFS user (`user=…`) separately from the Kerberos principal. Those are not the same thing, and the difference decides whether this service can write at all.

With Kerberos enabled, this service's effective HDFS user is **the short name of the principal it logs in as** — there is no proxy-user support and no `HADOOP_USER_NAME` override. If the landing directories are owned by, or granted to, the legacy application's HDFS user and the principal's short name is different, every write fails with `AccessControlException` naming the principal.

Preflight answers this without consuming anything:

- `filesystem.connect` prints **the user it authenticated as** — compare it against the directory owner.
- `<binding>.landing-path` and `.audit-path` perform a real `access(WRITE)` as that user.

Establish before deployment how the legacy application uses its `user=` property — whether it proxies, logs in separately, or the value is vestigial — and confirm the principal has write access to both roots in its own right.

## Kerberos (VERIFIED behavior)

`KerberosManager` logs in with `UserGroupInformation.loginUserFromKeytab(principal, keytab)` after setting `hadoop.security.authentication=kerberos`, then `FileSystem.get()` runs inside `ugi.doAs(...)`. The `FileSystem` instance captures that identity, so every later operation — write, rename, audit, reconciliation — runs as it.

TGT renewal already exists and is correct for a long-running consumer: `checkTGTAndReloginFromKeytab()` runs on a single dedicated daemon thread, never from a listener thread, because UGI's TGT handling has a known thread-safety race. Nothing needs to be added here.

**External verification required:** whether this one principal is accepted by the target cluster's realm. The repository cannot answer that, and the existing application's code comments are not evidence.

## Startup sequence (VERIFIED — already correct)

```
Spring context
  1. hadoopConfiguration   load target cluster's XML, apply overrides,
                           validate fs.defaultFS and nameservice
  2. kerberosManager       login from keytab; failure fails the context
  3. fileSystem            FileSystem.get inside ugi.doAs; refuse local disk
IntakeRuntimeManager.start()
  4. validateBindingConfigurations / validateSerializers
  5. validateAllBindings   landing, temp and audit paths exist and are writable
  6. cleanupTempFiles
  7. createAndStartRuntimes   ← the first JMS session is created here
```

Steps 1–6 all precede step 7. **No message can be consumed before the target filesystem has been proven writable**, and any failure above throws before a consumer exists. This requirement was already met; nothing was changed to satisfy it.

## Failure behavior (VERIFIED — no message-loss path)

`TransactedReceiveLoop.processBatch()` orders every batch:

```
write to _tmp → hsync → close → rename into partition → emit audit → session.commit()
```

The MQ commit is last. Any HDFS failure throws before it, the transaction rolls back, and MQ redelivers. **There is no window in which a message is committed to MQ and missing from HDFS.** A crash between rename and commit yields a *duplicate*, which reconciliation detects — the correct direction to fail in.

During an outage of the target cluster, RMS rolls back repeatedly and redelivery increments `JMSXDeliveryCount`. Ordinarily that would push healthy messages past the backout threshold and divert them. It does not, because RMS sets `backout.route-only-on-data-failures: true` and `FailureClass.HDFS_INFRASTRUCTURE` does not permit routing. **A storage outage stalls the feed; it does not corrupt it.** Messages accumulate on the source queue, which is the real operational cost and the reason startup must fail fast rather than limp.

## Pre-deployment checks on the host

Mandatory — the deployment cannot succeed if any of these fail:

```bash
# MQ reachability (the reason for deploying here)
nc -vz <mq-host> <mq-port>

# Cluster configuration: confirm you are reading the intended cluster
grep -n "fs.defaultFS"    <target-conf-dir>/core-site.xml
grep -n "dfs.nameservices" <target-conf-dir>/hdfs-site.xml
grep -n "dfs.ha.namenodes" <target-conf-dir>/hdfs-site.xml

# Kerberos
kinit -kt <keytab> <principal> && klist

# DNS, forward and reverse, for NameNodes and DataNodes
getent hosts <namenode-host>
getent hosts <datanode-host>
```

Then the application's own probe, which starts no listener and consumes nothing:

```bash
./current/intake.sh preflight mq     # connectivity only
./current/intake.sh preflight hdfs   # cluster config, identity, durability
./current/intake.sh preflight        # everything
```

Optional, only if the Hadoop CLI happens to be installed — do not assume it is:

```bash
HADOOP_CONF_DIR=<target-conf-dir> hdfs getconf -confKey fs.defaultFS
HADOOP_CONF_DIR=<target-conf-dir> hdfs dfs -ls <landing-path>
```

## Deployment checklist

- [ ] `HDFS_CONFIG_RESOURCES` points at the **target** cluster's conf directory, not the host's default
- [ ] `HDFS_EXPECTED_NAMESERVICE` set to the target cluster's nameservice
- [ ] `KERBEROS_ENABLED=true`, principal and keytab path set, keytab readable by the service account
- [ ] `MQ_INTAKE_PRODUCTION=true`
- [ ] MQ channel, port, credentials and queue names set in `env.sh` (chmod 600)
- [ ] `dfs.client.use.datanode.hostname=true` set, or confirmed present in the cluster's `hdfs-site.xml`
- [ ] `./current/intake.sh preflight` → `PREFLIGHT PASSED`, exit 0
- [ ] `cluster-config.resources` names the expected directory in the report
- [ ] `filesystem.nameservice` passes — not skipped
- [ ] `./current/intake.sh start`, then `status` shows the binding HEALTHY
- [ ] A landed file confirmed **on the target cluster** at the configured path
- [ ] `./current/intake.sh stop` drains cleanly

## Open items — must come from other teams

Nothing below can be derived from the repository. None of it should be guessed.

| Item | Owner |
|---|---|
| Target cluster nameservice name and conf directory path | Hadoop |
| Landing and audit paths, with write permission for the service principal | Hadoop |
| Service principal and keytab, valid in the target cluster's realm | Security |
| Confirmation that the client's Hadoop 3.3.6 is compatible with the cluster version | Hadoop |
| `krb5.conf` location if not `/etc/krb5.conf` (then `-Djava.security.krb5.conf`) | Security |
| Firewall: host → NameNodes (RPC, both HA nodes) | Network |
| Firewall: host → **all** DataNodes (data transfer port) | Network |
| Firewall: host → KDC | Network |
| Forward **and reverse** DNS for all NameNodes and DataNodes | Network |
| MQ SVRCONN channel name, port, and whether the channel requires TLS | MQ |

TLS on the MQ channel is the one item that would block rather than delay: it is not wired into the connection factory, and no configuration works around it.
