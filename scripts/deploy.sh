#!/usr/bin/env bash
#
# Build locally, ship to a server, leave it ready to preflight and start.
#
#   ./scripts/deploy.sh rms user@testhost
#   ./scripts/deploy.sh rms user@testhost /opt/mq-intake     # custom base dir
#   ./scripts/deploy.sh rms user@testhost --fast             # skip the test suite
#
# What it does NOT do: start anything, or touch configuration and secrets
# already on the server. Deploying must never restart a running consumer by
# surprise, and must never overwrite the environment file that holds
# credentials. Starting is a separate, deliberate step on the server.
#
# Server layout it creates:
#
#   <base>/
#     releases/<timestamp>/     jar + control script (immutable, keeps 5)
#     current -> releases/...   symlink; rollback = repoint and restart
#     config/                   your application.yml overrides (never overwritten)
#     env.sh                    environment + secrets, chmod 600 (never overwritten)
#     logs/                     application logs
#     run/                      pid file

set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: deploy.sh <rms|claims> <user@host> [remote-base-dir] [--fast]

  remote-base-dir  default: ~/mq-intake
  --fast           skip the test suite (use only for iterating; never for a
                   deployment you intend to test against)
  --offline        build with maven -o, for a build machine with no network
                   (requires a populated ~/.m2)

git is optional: releases are stamped from a VERSION file if present, from git
if this is a working copy, and otherwise from the jar's sha256 alone.
USAGE
    exit 2
}

MODULE=""; TARGET=""; REMOTE_BASE='$HOME/mq-intake'; FAST=false; OFFLINE=false
for arg in "$@"; do
    case "$arg" in
        --fast) FAST=true ;;
        --offline) OFFLINE=true ;;
        -h|--help) usage ;;
        *)
            if   [[ -z "$MODULE" ]]; then MODULE="$arg"
            elif [[ -z "$TARGET" ]]; then TARGET="$arg"
            else REMOTE_BASE="$arg"
            fi ;;
    esac
done
[[ -n "$MODULE" && -n "$TARGET" ]] || usage
[[ "$MODULE" == "rms" || "$MODULE" == "claims" ]] || { echo "module must be rms or claims" >&2; exit 2; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

STAMP=$(date -u +%Y%m%dT%H%M%SZ)

# Source identity, best effort and honest about it. Build machines are not
# required to have git — or a working copy, or network access to a remote —
# so git is one source of an answer here, never a requirement. Order:
# an explicit VERSION file, then git if this really is a repository, then
# nothing. The jar checksum below is the identifier that always works.
describe_source() {
    if [[ -f "${REPO_ROOT}/VERSION" ]]; then
        head -1 "${REPO_ROOT}/VERSION" | tr -d '\r\n'
        return
    fi
    if command -v git > /dev/null 2>&1 && git -C "$REPO_ROOT" rev-parse --git-dir > /dev/null 2>&1; then
        local rev dirty=""
        rev=$(git -C "$REPO_ROOT" rev-parse --short HEAD)
        if ! git -C "$REPO_ROOT" diff --quiet || ! git -C "$REPO_ROOT" diff --cached --quiet; then
            dirty=" (uncommitted changes)"
        fi
        echo "git:${rev}${dirty}"
        return
    fi
    echo "no-vcs (add a VERSION file at the repo root to stamp releases)"
}
SOURCE_ID=$(describe_source)

echo "==> Building ${MODULE} — source: ${SOURCE_ID}"
MVN_FLAGS=()
$OFFLINE && { MVN_FLAGS+=(-o); echo "    maven OFFLINE (-o): resolving from the local ~/.m2 only"; }
if $FAST; then
    echo "    tests SKIPPED (--fast)"
    mvn -q "${MVN_FLAGS[@]}" clean install -pl "$MODULE" -am -DskipTests
else
    mvn -q "${MVN_FLAGS[@]}" clean install
fi

JAR=$(ls "${MODULE}"/target/datalake-mq-intake-"${MODULE}"-*.jar 2>/dev/null | head -1 || true)
[[ -n "$JAR" ]] || { echo "no jar produced for ${MODULE}" >&2; exit 1; }

# The checksum is the release's real identity: it names the exact bytes, which
# a revision id cannot, and it lets the server prove later that the jar it is
# running is the one that left this machine intact.
JAR_SHA=$(sha256sum "$JAR" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "$JAR" | cut -d' ' -f1)
echo "    $(basename "$JAR")  ($(du -h "$JAR" | cut -f1))  sha256:${JAR_SHA:0:16}…"

# Resolve the remote base once, so ~ and $HOME behave the same everywhere.
REMOTE_BASE_RESOLVED=$(ssh "$TARGET" "eval echo \"$REMOTE_BASE\"")
RELEASE="${REMOTE_BASE_RESOLVED}/releases/${STAMP}"

echo "==> Preparing ${TARGET}:${REMOTE_BASE_RESOLVED}"
ssh "$TARGET" "mkdir -p '${RELEASE}' '${REMOTE_BASE_RESOLVED}/config' '${REMOTE_BASE_RESOLVED}/logs' '${REMOTE_BASE_RESOLVED}/run'"

echo "==> Uploading"
scp -q "$JAR" "${TARGET}:${RELEASE}/app.jar"
scp -q "${REPO_ROOT}/scripts/server/intake.sh" "${TARGET}:${RELEASE}/intake.sh"
ssh "$TARGET" "chmod +x '${RELEASE}/intake.sh'"

# Release metadata, so 'what is actually running' is answerable on the server.
ssh "$TARGET" "cat > '${RELEASE}/RELEASE' <<EOF
module=${MODULE}
source=${SOURCE_ID}
jar=$(basename "$JAR")
jar_sha256=${JAR_SHA}
built_by=$(whoami)@$(hostname)
deployed_at=${STAMP}
tests=$($FAST && echo skipped || echo run)
EOF"

# Prove the bytes survived the copy. A truncated scp produces a jar that
# starts and then fails somewhere unhelpful; catching it here costs a second.
REMOTE_SHA=$(ssh "$TARGET" "sha256sum '${RELEASE}/app.jar' 2>/dev/null | cut -d' ' -f1 || shasum -a 256 '${RELEASE}/app.jar' | cut -d' ' -f1")
if [[ "$REMOTE_SHA" != "$JAR_SHA" ]]; then
    echo "TRANSFER CORRUPTED: local ${JAR_SHA} != remote ${REMOTE_SHA}" >&2
    echo "The release was NOT activated. Re-run the deploy." >&2
    exit 1
fi
echo "    checksum verified on the server"

# First deploy only: seed the environment file and leave it for the operator.
# Never overwritten — it holds credentials and environment specifics that must
# survive every subsequent deployment.
if ssh "$TARGET" "[ -f '${REMOTE_BASE_RESOLVED}/env.sh' ]"; then
    echo "    env.sh present — left untouched"
else
    scp -q "${REPO_ROOT}/scripts/server/env.sh.example" "${TARGET}:${REMOTE_BASE_RESOLVED}/env.sh"
    ssh "$TARGET" "chmod 600 '${REMOTE_BASE_RESOLVED}/env.sh'"
    echo "    env.sh seeded from template — EDIT IT BEFORE STARTING"
    SEEDED_ENV=true
fi

echo "==> Activating release"
ssh "$TARGET" "ln -sfn '${RELEASE}' '${REMOTE_BASE_RESOLVED}/current'"

# Keep the last 5 releases: enough to roll back through a bad week, few enough
# not to fill a modest /home.
ssh "$TARGET" "cd '${REMOTE_BASE_RESOLVED}/releases' && ls -1t | tail -n +6 | xargs -r rm -rf"

RUNNING=$(ssh "$TARGET" "'${REMOTE_BASE_RESOLVED}/current/intake.sh' is-running >/dev/null 2>&1 && echo yes || echo no")

cat <<EOF

==> Deployed ${MODULE} to ${TARGET}:${REMOTE_BASE_RESOLVED}
    source ${SOURCE_ID}, sha256 ${JAR_SHA}

Next, on the server:

  ssh ${TARGET}
  cd ${REMOTE_BASE_RESOLVED}
EOF

if [[ "${SEEDED_ENV:-false}" == "true" ]]; then
    cat <<EOF
  vi env.sh                 # REQUIRED: queues, hosts, paths, credential ref
EOF
fi

cat <<EOF
  ./current/intake.sh preflight     # prove MQ + HDFS before consuming anything
  ./current/intake.sh start
  ./current/intake.sh status
EOF

if [[ "$RUNNING" == "yes" ]]; then
    cat <<EOF

NOTE: a service is currently RUNNING from the previous release. This deploy did
      not touch it. To adopt the new release:
        ./current/intake.sh stop     # graceful: drains and commits in flight
        ./current/intake.sh start
EOF
fi
