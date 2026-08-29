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

If this machine cannot reach the server at all, use scripts/bundle.sh instead
and carry the archive across; it installs with the same installer.
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

# One installer, two transports: deploy builds the same bundle that
# scripts/bundle.sh produces for hand-carried transfers, ships it, and runs the
# bundle's own install.sh on the far side. Anything else invites the two paths
# to drift until only one of them is actually tested.
BUNDLE_ARGS=("$MODULE")
$FAST && BUNDLE_ARGS+=(--fast)
$OFFLINE && BUNDLE_ARGS+=(--offline)

STAGING="$(mktemp -d)"
trap 'rm -rf "$STAGING"' EXIT

"${REPO_ROOT}/scripts/bundle.sh" "${BUNDLE_ARGS[@]}" -o "$STAGING"
ARCHIVE=$(ls "$STAGING"/*.tar.gz | head -1)
[[ -n "$ARCHIVE" ]] || { echo "bundle step produced no archive" >&2; exit 1; }

# Resolve the remote base once, so ~ and $HOME behave the same everywhere.
REMOTE_BASE_RESOLVED=$(ssh "$TARGET" "eval echo \"$REMOTE_BASE\"")

echo "==> Shipping $(basename "$ARCHIVE") to ${TARGET}"
REMOTE_TMP=$(ssh "$TARGET" "mktemp -d")
scp -q "$ARCHIVE" "${TARGET}:${REMOTE_TMP}/"

echo "==> Installing on ${TARGET}:${REMOTE_BASE_RESOLVED}"
# The installer verifies the bundle's own checksums, so a transfer that lost
# bytes fails there rather than at first start.
ssh "$TARGET" "set -e
    cd '${REMOTE_TMP}'
    tar xzf '$(basename "$ARCHIVE")'
    cd \"\$(basename '$(basename "$ARCHIVE")' .tar.gz)\"
    ./install.sh '${REMOTE_BASE_RESOLVED}'
    rm -rf '${REMOTE_TMP}'"

cat <<EOF

==> Deployed ${MODULE} to ${TARGET}:${REMOTE_BASE_RESOLVED}

  ssh ${TARGET}
  cd ${REMOTE_BASE_RESOLVED}

EOF
