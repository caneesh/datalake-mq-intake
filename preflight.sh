#!/usr/bin/env bash
#
# Probe the real environment's components, one at a time, and exit.
#
#   ./preflight.sh rms                # every check for the rms module
#   ./preflight.sh rms mq             # MQ only
#   ./preflight.sh rms hdfs           # HDFS only
#   ./preflight.sh rms app            # serializer / tracker / gate summary
#   ./preflight.sh claims mq hdfs     # several groups
#
# Nothing is consumed, nothing is sent to a queue another system reads, and
# nothing is written outside _tmp/{instanceId} (probe files are removed).
# Safe to run against an environment carrying live data.
#
# Exit status is 0 when every check passes, 1 otherwise — so a deployment
# pipeline can gate on it.
#
# Environment variables are read exactly as the service reads them: point
# MQ_HOST / MQ_QUEUE_MANAGER / MQ_CHANNEL / MQ_CREDENTIAL_REF and the
# KERBEROS_* pair at the target environment before running.

set -euo pipefail

MODULE="${1:-}"
if [[ -z "$MODULE" ]]; then
    echo "usage: $0 <rms|claims> [mq] [hdfs] [app]" >&2
    exit 2
fi
shift || true

JAR=$(ls "${MODULE}"/target/datalake-mq-intake-"${MODULE}"-*.jar 2>/dev/null | head -1 || true)
if [[ -z "$JAR" ]]; then
    echo "No jar for module '${MODULE}'. Build it first:  mvn clean install -pl ${MODULE} -am" >&2
    exit 2
fi

ARGS=(--intake.preflight.enabled=true)
for group in "$@"; do
    ARGS+=("--preflight=${group}")
done

echo "Preflight: ${MODULE} (${*:-all groups}) using ${JAR}"
exec java -jar "$JAR" "${ARGS[@]}"
