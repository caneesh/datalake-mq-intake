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
USAGE
    exit 2
}

MODULE=""; TARGET=""; REMOTE_BASE='$HOME/mq-intake'; FAST=false
for arg in "$@"; do
    case "$arg" in
        --fast) FAST=true ;;
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
GIT_REV=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
GIT_DIRTY=""
if ! git diff --quiet 2>/dev/null || ! git diff --cached --quiet 2>/dev/null; then
    GIT_DIRTY=" (uncommitted changes)"
fi

echo "==> Building ${MODULE} at ${GIT_REV}${GIT_DIRTY}"
if $FAST; then
    echo "    tests SKIPPED (--fast)"
    mvn -q clean install -pl "$MODULE" -am -DskipTests
else
    mvn -q clean install
fi

JAR=$(ls "${MODULE}"/target/datalake-mq-intake-"${MODULE}"-*.jar 2>/dev/null | head -1 || true)
[[ -n "$JAR" ]] || { echo "no jar produced for ${MODULE}" >&2; exit 1; }
echo "    $(basename "$JAR")  ($(du -h "$JAR" | cut -f1))"

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
git_rev=${GIT_REV}${GIT_DIRTY}
jar=$(basename "$JAR")
built_by=$(whoami)@$(hostname)
deployed_at=${STAMP}
tests=$($FAST && echo skipped || echo run)
EOF"

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

==> Deployed ${MODULE} ${GIT_REV} to ${TARGET}:${REMOTE_BASE_RESOLVED}

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
