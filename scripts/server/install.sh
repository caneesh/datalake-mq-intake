#!/usr/bin/env bash
#
# Install an unpacked bundle into a server layout. Runs on the server, from
# inside the unpacked bundle directory.
#
#   ./install.sh                 # into ~/mq-intake
#   ./install.sh /opt/mq-intake  # or a directory of your choosing
#
# Guarantees, identical whether the bundle arrived by scp or on a memory stick:
#   - never starts the service, and never stops a running one
#   - never overwrites env.sh or config/ — those hold credentials and the
#     environment's own decisions, and must survive every install
#   - activates by symlink, so rolling back is repointing it
#
# scripts/deploy.sh runs this same script over ssh. One installer, two
# transports.

set -euo pipefail

BUNDLE_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="${1:-$HOME/mq-intake}"

for required in app.jar intake.sh RELEASE MANIFEST.sha256; do
    [[ -f "${BUNDLE_DIR}/${required}" ]] || {
        echo "not an unpacked bundle: ${required} is missing from ${BUNDLE_DIR}" >&2
        exit 2
    }
done

echo "==> Verifying bundle contents"
if command -v sha256sum > /dev/null 2>&1; then
    ( cd "$BUNDLE_DIR" && sha256sum -c --quiet MANIFEST.sha256 ) || {
        echo "CHECKSUM MISMATCH — the bundle did not survive the transfer intact." >&2
        echo "Nothing has been installed. Copy it again." >&2
        exit 1
    }
elif command -v shasum > /dev/null 2>&1; then
    ( cd "$BUNDLE_DIR" && shasum -a 256 -c MANIFEST.sha256 > /dev/null ) || {
        echo "CHECKSUM MISMATCH — the bundle did not survive the transfer intact." >&2
        exit 1
    }
else
    echo "    no sha256 tool available; skipping verification" >&2
fi
echo "    ok"

RELEASE_ID=$(grep '^release=' "${BUNDLE_DIR}/RELEASE" | cut -d= -f2)
[[ -n "$RELEASE_ID" ]] || RELEASE_ID=$(date -u +%Y%m%dT%H%M%SZ)
RELEASE_DIR="${BASE_DIR}/releases/${RELEASE_ID}"

echo "==> Installing release ${RELEASE_ID} into ${BASE_DIR}"
mkdir -p "${BASE_DIR}/releases" "${BASE_DIR}/config" "${BASE_DIR}/logs" "${BASE_DIR}/run"

# Re-installing the same bundle replaces its own release directory rather than
# failing: installing twice should be harmless, not a puzzle.
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"
cp "${BUNDLE_DIR}/app.jar" "${BUNDLE_DIR}/intake.sh" "${BUNDLE_DIR}/RELEASE" "$RELEASE_DIR/"
chmod +x "${RELEASE_DIR}/intake.sh"
echo "installed_at=$(date -u +%Y%m%dT%H%M%SZ)" >> "${RELEASE_DIR}/RELEASE"
echo "installed_on=$(hostname)" >> "${RELEASE_DIR}/RELEASE"

SEEDED_ENV=false
if [[ -f "${BASE_DIR}/env.sh" ]]; then
    echo "    env.sh present — left untouched"
else
    cp "${BUNDLE_DIR}/env.sh.example" "${BASE_DIR}/env.sh"
    chmod 600 "${BASE_DIR}/env.sh"
    SEEDED_ENV=true
    echo "    env.sh seeded from template (chmod 600)"
fi

WAS_RUNNING=no
if [[ -f "${BASE_DIR}/current/intake.sh" ]] \
   && "${BASE_DIR}/current/intake.sh" is-running > /dev/null 2>&1; then
    WAS_RUNNING=yes
fi

ln -sfn "$RELEASE_DIR" "${BASE_DIR}/current"
echo "    activated ${BASE_DIR}/current -> releases/${RELEASE_ID}"

# Keep the last 5: enough to roll back through a bad week, few enough not to
# fill a modest /home.
( cd "${BASE_DIR}/releases" && ls -1t | tail -n +6 | xargs -r rm -rf )

cat <<EOF

==> Installed. Nothing has been started.

  cd ${BASE_DIR}
EOF
$SEEDED_ENV && echo "  vi env.sh                          # REQUIRED before starting"
cat <<EOF
  ./current/intake.sh preflight      # prove MQ + HDFS before consuming anything
  ./current/intake.sh start
  ./current/intake.sh status
EOF

if [[ "$WAS_RUNNING" == "yes" ]]; then
    cat <<EOF

NOTE: a service was RUNNING from the previous release and still is — this
      install did not touch it. To adopt the new release:
        ./current/intake.sh stop     # graceful: drains and commits in flight
        ./current/intake.sh start
EOF
fi
