# Test Deployment Guide — build locally, run on the server

How to get the intake service onto a test server and running against real IBM MQ and real HDFS.

**The shape of it:** the code is built on your machine, the jar is copied to the server, and the service runs there — close to MQ and HDFS, under the service account, with the environment's own Kerberos identity. Nothing about the build happens on the server.

```
  your machine                         test server
  ────────────                         ───────────
  mvn clean install                    releases/<stamp>/app.jar
        │                              current ──► releases/<stamp>
        │  scripts/deploy.sh           config/     your overrides
        ├────── scp/ssh ──────────►    env.sh      environment + secrets (chmod 600)
        │  scripts/bundle.sh
        └── tar.gz ─ any medium ──►    (install.sh, same installer)
                                       logs/  run/
                                             │
                                             ├── preflight  → proves MQ + HDFS
                                             ├── start      → consumes, lands, tracks
                                             └── stop       → drains in flight
```

Companions: `docs/TEST_ENVIRONMENT_PLAN.md` (what to test once it runs) and `docs/DEPLOYMENT_CHECKLIST.md` (production cutover).

---

## 1 — What the server needs

- **Java 11** on `PATH` (`java -version`). Newer is fine; older is not, and `intake.sh` refuses to start on it with a plain message rather than letting the JVM report `UnsupportedClassVersionError`.
- Network reach to the queue manager (default port 1414) and to HDFS
- The **service account**'s Kerberos keytab, if the cluster is kerberised
- A writable home or install directory
- `curl` (optional — `status` uses it for health and metrics)

Nothing else on the **server**: no Maven, no git, no repository clone, no MQ client install — the jar is self-contained. The **build machine** needs a JDK and Maven; git is optional (see §2).

**On build-JDK versions.** The project compiles with `maven.compiler.release=11`, which pins the API surface as well as the bytecode — so a newer build JDK cannot accidentally link against methods your Java 11 hosts do not have. (`source`/`target` alone would not: they produce Java 11 bytecode that still compiles against the build JDK's library, and the failure surfaces as a `NoSuchMethodError` in production, far from its cause.) `bundle.sh` additionally refuses to package anything whose class-file version exceeds Java 11, and records the build JDK in the release metadata.

## 2 — If the build machine has no git

**git is optional here.** The deploy script never requires it, and nothing in the workflow calls out to a remote. What changes is only how a release is *stamped*, and there is a better answer than a revision id anyway.

**Stamping.** Releases are identified in this order:

1. a `VERSION` file at the repository root, if present — one line, whatever your change process calls this build (`rms-2026.08.29-rc1`);
2. the git revision, if git happens to be available and this is a working copy;
3. otherwise `no-vcs`, and the jar's checksum stands alone.

Create the `VERSION` file when the build machine has no version control — it costs one line and it is what appears on the server:

```bash
echo "rms-2026.08.29-rc1" > VERSION
```

**The checksum is the real identity.** Every deploy records the jar's `sha256` in the server's `RELEASE` file, verifies it after the copy (a truncated `scp` is caught before the release is activated, not at 3am), and `intake.sh status` re-verifies the on-disk jar against it:

```
release : jar_sha256=cb5e0ff21d9b84f143b2ba21d263863977e4c76a6a91ea348684e00ed5f29b31
release : jar_verified=yes
```

That answers "is the server running exactly what I built?" — which a revision id cannot, since it says nothing about what was actually compiled or whether it arrived intact.

**If the machine is also offline**, Maven needs a populated local repository. Prime `~/.m2` once from a connected machine (copy the directory across), then:

```bash
./scripts/deploy.sh rms user@testhost --offline    # passes -o to maven
```

A build that fails on a missing artifact in offline mode is telling you the local repository is incomplete, not that anything is wrong with the code.

**Getting the source onto the build machine** is outside this toolchain — a zip, a shared drive, whatever your environment allows. The only thing the scripts assume is a directory containing the project, with `scripts/` in it.

## 3 — Deploy

From the repository root on your machine:

```bash
./scripts/deploy.sh rms user@testhost                  # default base: ~/mq-intake
./scripts/deploy.sh rms user@testhost /opt/mq-intake   # custom base
./scripts/deploy.sh claims user@testhost               # the other application
```

It builds (running the full test suite), uploads the jar and the control script into a timestamped release directory, repoints `current`, and keeps the last five releases. It prints what to do next.

**What deploy deliberately does not do:** start anything, stop anything, or overwrite `config/` and `env.sh`. A deploy must never restart a live consumer by surprise, and must never clobber the file holding credentials. If a service is already running from the previous release, deploy says so and leaves it alone.

Use `--fast` to skip the test suite while iterating (never for a deployment you intend to test against), and `--offline` if the build machine has no network — see §2.

### If this machine cannot reach the server

`deploy.sh` needs `ssh` to the target. When there is no route — a jump host you cannot script through, an air-gapped test environment, a change process that moves artifacts by ticket — build a bundle instead and carry it across by any means:

```bash
./scripts/bundle.sh rms                    # -> dist/mq-intake-rms-<stamp>.tar.gz (+ .sha256)
./scripts/bundle.sh rms -o /mnt/transfer   # write it straight to the transfer medium
```

Then on the server, with nothing installed but Java:

```bash
sha256sum -c mq-intake-rms-<stamp>.tar.gz.sha256   # did it survive the journey?
tar xzf mq-intake-rms-<stamp>.tar.gz
cd mq-intake-rms-<stamp>
./install.sh                                       # or: ./install.sh /opt/mq-intake
```

The bundle carries its own installer, control script, environment template and a `README.txt`, plus a `MANIFEST.sha256` that `install.sh` verifies before touching anything — a bundle that lost bytes in transit is refused with nothing installed, rather than producing a service that starts and fails obscurely later.

**`deploy.sh` builds and installs this same bundle over ssh.** One installer, two transports: the hand-carried path is not a lesser sibling that nobody tests, it is the identical code with a different delivery. Both give you the same layout, the same guarantees (never start, never stop, never overwrite `env.sh` or `config/`), and the same rollback.

Re-installing the same bundle is harmless — it replaces its own release directory and leaves your configuration alone.

## 4 — Configure the server (first deploy only)

The first deploy seeds `env.sh` from a template and chmods it 600. Edit it:

```bash
ssh user@testhost
cd ~/mq-intake
vi env.sh
```

Everything the environment needs is there: MQ host/port/queue-manager/channel, the three queue names, the credential reference, HDFS paths, Kerberos, heap, and `MQ_INTAKE_PRODUCTION`.

Two things to get right:

**Credentials are referenced, not embedded in configuration.** `MQ_CREDENTIAL_REF="env:MQ_USER,MQ_PASSWORD"` tells the service to read those two variables. The password is never logged and never printed by `intake.sh config`, which shows secrets only as `<set>` or `<unset>`.

**Arm production mode** (`MQ_INTAKE_PRODUCTION=true`) in any environment standing in for production. It is what makes the startup gates refuse dev-default connection values, placeholder serializers and an incomplete tracker contract. A test that runs without it is testing a more permissive service than the one you will promote.

### When you need more than environment variables

`env.sh` covers the environment-specific values. To change **behaviour** — batch size, thresholds, listener threads — drop a YAML file in `config/`, which the control script passes to Spring automatically:

```bash
vi ~/mq-intake/config/application.yml
```

> **Copy the whole `bindings:` block when you do this.** Spring does not merge collections across configuration sources: whichever source mentions `intake.bindings` supplies the entire list. A file containing only `intake.bindings[0].batch.size` produces a binding with *nothing else set*, and the service fails with `Binding 'null' must specify mq-connection`. The same is true of `--intake.bindings[0].x=y` on the command line and of `INTAKE_BINDINGS_0_X` in the environment. Scalar values outside the list (`intake.hdfs.audit-base-path`, `intake.mq-connections.primary.host`) override individually and are safe — which is exactly why the queue names and paths are `${VAR}` placeholders in the shipped YAML rather than something you have to override structurally.

## 5 — Prove the environment before consuming anything

```bash
./current/intake.sh preflight          # everything
./current/intake.sh preflight mq       # or: hdfs, app
```

Preflight connects to the real dependencies, checks one fact at a time, prints a report naming the fix for each failure, and exits non-zero if anything failed. **It starts no listener and consumes no message**, so it is safe against an environment carrying live data.

- [ ] `PREFLIGHT PASSED`, exit status 0
- [ ] `production-mode` reports **ARMED**

Fix everything it reports before starting. A failure found here is a failure found with no messages in flight.

## 6 — Start, watch, stop

```bash
./current/intake.sh start      # background; waits for the startup confirmation
./current/intake.sh status     # pid, release, health, key metrics
./current/intake.sh logs -f    # follow
./current/intake.sh stop       # graceful
```

`start` refuses if a service is already running, and reports failure with the last 30 log lines if the process dies during startup rather than claiming success for a process that a config gate killed three seconds later.

**`stop` sends SIGTERM and waits.** The receive loop drains and commits its in-flight batch on shutdown; that is why the script never escalates to `kill -9` on its own. If the drain has not finished within `STOP_TIMEOUT_SECONDS` (default 90) it tells you and stops, leaving the decision with you. A forced kill is safe for delivery — the batch rolls back and MQ redelivers — but it manufactures avoidable duplicates.

### What "working" looks like

```bash
./current/intake.sh status
```

```
process : running (pid 2954962, up 04:11)
release : module=rms
release : git_rev=e61bbf8
health  : {"status":"UP","components":{"bindings":{"status":"UP",
          "details":{"rms":{"status":"HEALTHY", ...
metric  : messages_consumed_total          12.0
metric  : batches_committed_total          4.0
metric  : batches_rolled_back_total        0.0
metric  : balance_check_failures_total     0.0
metric  : backout_queue_depth              0.0
```

Then confirm data actually landed:

```bash
hdfs dfs -ls -R $HDFS_BASE_PATH | grep '\.seq$'
hdfs dfs -cat $HDFS_AUDIT_BASE_PATH/rms/$(date -u +%Y%m%d)/audit_*.json | python3 -m json.tool
```

The audit record should read `"balance_status": "BALANCED"` with `consumed_count` equal to `record_count + backout_count`.

> **A handful of test messages will not appear immediately.** Production settings flush a batch on size (1000), on bytes, or at the quarter-hour partition boundary — so a dozen messages sit in the in-flight batch until the boundary passes. They are already consumed (the source queue shows depth 0) and they are not lost: a graceful `stop` drains them to disk immediately, which is a good way to see the whole path work in one minute. For sustained functional testing, use the test overlay in the test plan (`batch.size: 10`, `interval-ms: 5000`) via a `config/application.yml` with the complete binding block.

## 7 — Upgrade and roll back

Upgrading is deploy, stop, start:

```bash
./scripts/deploy.sh rms user@testhost      # from your machine; does not touch the running service
ssh user@testhost 'cd ~/mq-intake && ./current/intake.sh stop && ./current/intake.sh start'
```

Rolling back is repointing the symlink — the previous release still has its own jar and control script:

```bash
cd ~/mq-intake
./current/intake.sh stop
ln -sfn releases/<previous-stamp> current
./current/intake.sh start
```

`config/` and `env.sh` live outside the release directories, so they survive both.

**Rollback is always message-safe.** Anything unprocessed simply queues on MQ; landed files stay landed and audited. There is no data migration in either direction.

## 8 — Where things are

| | |
|---|---|
| `~/mq-intake/current/` | symlink to the active release (jar + control script + `RELEASE` metadata) |
| `~/mq-intake/releases/` | last five releases; rollback targets |
| `~/mq-intake/config/` | optional YAML overrides; survives deploys |
| `~/mq-intake/env.sh` | environment and credentials, chmod 600; survives deploys |
| `~/mq-intake/logs/current.log` | symlink to the log of the running instance |
| `~/mq-intake/run/intake.pid` | pid of the running instance |
| `dist/` (build machine) | bundles produced by `scripts/bundle.sh`, with `.sha256` sidecars |

## 9 — Troubleshooting

| Symptom | Cause |
|---|---|
| `refuses to start … dev-placeholder defaults` | production mode is armed and `MQ_HOST`/`MQ_QUEUE_MANAGER`/`MQ_CHANNEL` are unset — the gate doing its job |
| `Binding 'null' must specify mq-connection` | a partial binding override; copy the whole `bindings:` block (see §4) |
| Preflight `MQRC 2035` on the connection | credential reference empty or wrong; check `MQ_CREDENTIAL_REF` and that `MQ_USER`/`MQ_PASSWORD` are exported |
| Preflight `MQRC 2085` on a queue | the queue is not on the queue manager this connection reached — commonly a sibling QM in a pair |
| Preflight `MQRC 2035` on a queue but not the connection | connected fine, but the account lacks the open option; on MQ, an authority profile's `*` matches one qualifier, so `MQ.ABC.*` does **not** cover `MQ.ABC.DEF.IN` — use `**` |
| Started, but no `.seq` files | fewer than `batch.size` messages and the partition boundary has not passed (see §6) |
| `status` shows health not answering | the process is still starting, or `SERVER_PORT` differs from the default 8080 — set `HEALTH_URL`/`METRICS_URL` in `env.sh` |
| Service exits immediately | read `logs/current.log`; a startup gate names the exact cause on its first ERROR line |
| `status` shows `jar_verified=NO` | the jar on disk is not the one deployed — a partial copy or a hand edit; redeploy |
| Release shows `source=no-vcs` | expected on a build machine without git; add a `VERSION` file (§2) if you want a human-readable stamp |
| Offline build fails on a missing artifact | `~/.m2` is incomplete for `-o`; re-prime it from a connected machine |
| `CHECKSUM MISMATCH` from `install.sh` | the bundle lost bytes in transit; nothing was installed — copy it again |
| `UnsupportedClassVersionError` | a jar built for a newer Java than the host runs; `bundle.sh` should have refused it — check `maven.compiler.release` |
| `this service needs Java 11 or newer` | the host's `PATH` java is older; point `PATH` or `JAVA_HOME/bin` at the Java 11 runtime |
