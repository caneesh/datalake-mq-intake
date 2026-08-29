#!/usr/bin/env bash
#
# Package a build into one self-contained archive that can travel by any means
# — shared drive, USB, ticket attachment — and install on a server that this
# machine cannot reach.
#
#   ./scripts/bundle.sh rms
#   ./scripts/bundle.sh claims --offline -o /mnt/transfer
#
# Produces:
#   dist/mq-intake-<module>-<stamp>.tar.gz
#   dist/mq-intake-<module>-<stamp>.tar.gz.sha256
#
# On the server:
#   tar xzf mq-intake-<module>-<stamp>.tar.gz
#   cd mq-intake-<module>-<stamp> && ./install.sh
#
# The bundle carries its own installer, so the server needs nothing but Java.
# scripts/deploy.sh builds the same bundle and installs it over ssh — one
# installer, two transports, identical results.

set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: bundle.sh <rms|claims> [-o output-dir] [--fast] [--offline]

  -o DIR      where to write the archive (default: dist/)
  --fast      skip the test suite (never for a bundle you intend to deploy)
  --offline   build with maven -o, for a machine with no network

git is optional. The release is stamped from a VERSION file if present, from
git if this is a working copy, and otherwise from the jar's sha256 alone.
USAGE
    exit 2
}

MODULE=""; OUT_DIR=""; FAST=false; OFFLINE=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --fast) FAST=true; shift ;;
        --offline) OFFLINE=true; shift ;;
        -o) OUT_DIR="${2:-}"; shift 2 ;;
        -h|--help) usage ;;
        *) [[ -z "$MODULE" ]] && MODULE="$1" || usage; shift ;;
    esac
done
[[ -n "$MODULE" ]] || usage
[[ "$MODULE" == "rms" || "$MODULE" == "claims" ]] || { echo "module must be rms or claims" >&2; exit 2; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/dist}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

sha256_of() {
    sha256sum "$1" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "$1" | cut -d' ' -f1
}

# See scripts/deploy.sh: git is one possible answer here, never a requirement.
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
$OFFLINE && { MVN_FLAGS+=(-o); echo "    maven OFFLINE (-o)"; }
if $FAST; then
    echo "    tests SKIPPED (--fast)"
    mvn -q "${MVN_FLAGS[@]}" clean install -pl "$MODULE" -am -DskipTests
else
    mvn -q "${MVN_FLAGS[@]}" clean install
fi

JAR=$(ls "${MODULE}"/target/datalake-mq-intake-"${MODULE}"-*.jar 2>/dev/null | head -1 || true)
[[ -n "$JAR" ]] || { echo "no jar produced for ${MODULE}" >&2; exit 1; }
JAR_SHA=$(sha256_of "$JAR")

BUILD_JDK=$(java -version 2>&1 | head -1 | sed 's/.*version "\([^"]*\)".*/\1/')

# The servers run Java 11. Verify what we are about to ship actually targets
# it, rather than trusting the build machine's JDK to have been the right one:
# a class-file major above 55 loads nowhere on the host and the failure
# (UnsupportedClassVersionError at startup) is far from its cause.
if command -v unzip > /dev/null 2>&1; then
    CLASS_CHECK_DIR=$(mktemp -d)
    if unzip -o -q "$JAR" "BOOT-INF/classes/com/hcsc/*" -d "$CLASS_CHECK_DIR" 2>/dev/null; then
        MAX_MAJOR=$(find "$CLASS_CHECK_DIR" -name '*.class' -print0 \
            | xargs -0 -I{} sh -c 'od -An -t u1 -j 6 -N 2 "$1" | awk "{print \$1*256+\$2}"' _ {} \
            | sort -n | tail -1)
        rm -rf "$CLASS_CHECK_DIR"
        if [[ -n "$MAX_MAJOR" ]]; then
            if [[ "$MAX_MAJOR" -gt 55 ]]; then
                echo "REFUSING TO BUNDLE: class-file major ${MAX_MAJOR} needs Java $((MAX_MAJOR - 44))," >&2
                echo "but the target hosts run Java 11 (major 55). Build with a Java 11 JDK, or" >&2
                echo "check that maven.compiler.release is still 11 in pom.xml." >&2
                exit 1
            fi
            echo "    bytecode major ${MAX_MAJOR} (Java $((MAX_MAJOR - 44))) — runs on the Java 11 hosts"
        fi
    else
        rm -rf "$CLASS_CHECK_DIR"
    fi
fi

NAME="mq-intake-${MODULE}-${STAMP}"
STAGE="$(mktemp -d)/${NAME}"
mkdir -p "$STAGE"
trap 'rm -rf "$(dirname "$STAGE")"' EXIT

cp "$JAR" "${STAGE}/app.jar"
cp "${REPO_ROOT}/scripts/server/intake.sh" "${STAGE}/intake.sh"
cp "${REPO_ROOT}/scripts/server/install.sh" "${STAGE}/install.sh"
cp "${REPO_ROOT}/scripts/server/env.sh.example" "${STAGE}/env.sh.example"
chmod +x "${STAGE}/intake.sh" "${STAGE}/install.sh"

cat > "${STAGE}/RELEASE" <<EOF
module=${MODULE}
release=${STAMP}
source=${SOURCE_ID}
jar=$(basename "$JAR")
jar_sha256=${JAR_SHA}
built_by=$(whoami)@$(hostname)
built_at=${STAMP}
built_with_jdk=${BUILD_JDK}
tests=$($FAST && echo skipped || echo run)
EOF

cat > "${STAGE}/README.txt" <<EOF
${NAME}

Install (nothing needed but Java 11):

    ./install.sh                 # into ~/mq-intake
    ./install.sh /opt/mq-intake  # or a directory of your choosing

Then:

    cd <base>
    vi env.sh                          # first install only: queues, hosts, credentials
    ./current/intake.sh preflight      # prove MQ + HDFS before consuming anything
    ./current/intake.sh start
    ./current/intake.sh status

install.sh never starts the service, never stops a running one, and never
overwrites an existing env.sh or config/. Full guide: docs/TEST_DEPLOYMENT_GUIDE.md
in the source repository.

Contents are checksummed in MANIFEST.sha256 and verified by install.sh.
EOF

# Checksums for everything, so the installer can prove the bundle survived
# whatever medium carried it. Relative paths keep the manifest verifiable from
# inside the unpacked directory.
( cd "$STAGE" && for f in app.jar intake.sh install.sh env.sh.example RELEASE README.txt; do
      echo "$(sha256_of "$f")  ${f}"
  done > MANIFEST.sha256 )

mkdir -p "$OUT_DIR"
ARCHIVE="${OUT_DIR}/${NAME}.tar.gz"
tar -czf "$ARCHIVE" -C "$(dirname "$STAGE")" "$NAME"
ARCHIVE_SHA=$(sha256_of "$ARCHIVE")
echo "${ARCHIVE_SHA}  $(basename "$ARCHIVE")" > "${ARCHIVE}.sha256"

cat <<EOF

==> Bundle ready

  $(basename "$ARCHIVE")   ($(du -h "$ARCHIVE" | cut -f1))
  sha256  ${ARCHIVE_SHA}
  in      ${OUT_DIR}

Carry it across, then on the server:

  sha256sum -c $(basename "$ARCHIVE").sha256     # verify the transfer
  tar xzf $(basename "$ARCHIVE")
  cd ${NAME} && ./install.sh

EOF
