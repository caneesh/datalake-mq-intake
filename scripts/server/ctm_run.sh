#!/bin/bash
#
# Control-M wrapper for the MQ intake service.
#
# Lives in the release directory next to intake.sh and finds everything else
# from there — no paths are hardcoded and no Control-M variables are needed.
#
# Usage:
#   ctm_run.sh <command>
#
# Commands:
#   start        start the service (delegates to intake.sh)
#   stop         graceful stop (delegates to intake.sh)
#   status       print status; exit 0 only if the process is running
#   is-running   exit 0 if running, 1 if not; prints nothing
#   preflight    probe MQ and HDFS without consuming anything
#   health       exit 0 if actuator reports UP, 1 otherwise
#   metrics      dump actuator metrics
#   log-cleanup  delete rotated logs older than LOG_RETENTION_DAYS
#   tmp-check    exit 1 if HDFS _tmp holds staging directories nobody owns
#
# Exit codes: 0 success/UP, 1 failure/DOWN, 2 usage.

set -euo pipefail

RELEASE_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Same detection as intake.sh: releases/<stamp>/ under the base, or
# current/ directly under it.
BASE_DIR="$(cd -P "${RELEASE_DIR}/../.." && pwd)"
if [[ ! -f "${BASE_DIR}/env.sh" ]]; then
    BASE_DIR="$(cd -P "${RELEASE_DIR}/.." && pwd)"
fi

# env.sh is the single source of truth for SERVER_PORT, HDFS_BASE_PATH and
# Kerberos settings; env_controlm.conf may add thresholds on top of it.
if [[ -f "${BASE_DIR}/env.sh" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${BASE_DIR}/env.sh"
    set +a
fi
if [[ -f "${RELEASE_DIR}/env_controlm.conf" ]]; then
    # shellcheck disable=SC1091
    source "${RELEASE_DIR}/env_controlm.conf"
fi

SERVER_PORT="${SERVER_PORT:-8080}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-30}"
HDFS_BASE="${HDFS_BASE:-${HDFS_BASE_PATH:-}}"
# How long a staging directory may sit without a lease refresh before the
# check reports it. Matches intake.hdfs.instance-lease-timeout-ms (1 hour).
STALE_LEASE_MINUTES="${STALE_LEASE_MINUTES:-60}"

hdfs_login() {
    command -v hdfs > /dev/null 2>&1 || {
        echo "ERROR: 'hdfs' is not on PATH; this check needs a Hadoop client" >&2
        return 1
    }
    if [[ "${KERBEROS_ENABLED:-false}" == "true" ]]; then
        [[ -n "${KERBEROS_KEYTAB_PATH:-}" && -n "${KERBEROS_PRINCIPAL:-}" ]] || {
            echo "ERROR: KERBEROS_ENABLED=true but KERBEROS_KEYTAB_PATH/KERBEROS_PRINCIPAL unset" >&2
            return 1
        }
        kinit -kt "$KERBEROS_KEYTAB_PATH" "$KERBEROS_PRINCIPAL" || {
            echo "ERROR: kinit failed for ${KERBEROS_PRINCIPAL}" >&2
            return 1
        }
    fi
}

COMMAND="${1:-help}"
shift || true

case "$COMMAND" in
    start|stop|preflight|is-running)
        exec "${RELEASE_DIR}/intake.sh" "$COMMAND" "$@"
        ;;

    status)
        # intake.sh status prints everything and exits 1 when not running.
        exec "${RELEASE_DIR}/intake.sh" status
        ;;

    health)
        response=$(curl -s --max-time 5 "http://localhost:${SERVER_PORT}/actuator/health" 2>/dev/null || true)
        status=$(echo "$response" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
        if [[ "$status" == "UP" ]]; then
            echo "UP"
            exit 0
        fi
        echo "DOWN: ${status:-no response on port ${SERVER_PORT}}"
        exit 1
        ;;

    metrics)
        curl -s --fail --max-time 5 "http://localhost:${SERVER_PORT}/actuator/metrics" || {
            echo "ERROR: cannot fetch metrics on port ${SERVER_PORT}" >&2
            exit 1
        }
        ;;

    log-cleanup)
        LOG_DIR="${BASE_DIR}/logs"
        [[ -d "$LOG_DIR" ]] || { echo "no log directory at ${LOG_DIR}"; exit 0; }
        echo "Deleting logs older than ${LOG_RETENTION_DAYS} days in ${LOG_DIR}"
        find "$LOG_DIR" -type f \( -name "*.log" -o -name "*.log.gz" -o -name "*.out" \) \
            -mtime +"${LOG_RETENTION_DAYS}" -print -delete
        echo "Done"
        ;;

    tmp-check)
        [[ -n "$HDFS_BASE" ]] || { echo "ERROR: HDFS_BASE_PATH not set in env.sh" >&2; exit 1; }
        hdfs_login || exit 1
        TMP_PATH="${HDFS_BASE}/_tmp"
        echo "Checking ${TMP_PATH} for abandoned staging directories"
        if ! hdfs dfs -test -d "$TMP_PATH" 2>/dev/null; then
            echo "OK: no _tmp directory"
            exit 0
        fi

        # A running instance refreshes .instance-lease continuously. A directory
        # whose lease is older than the timeout — or has no lease at all — is
        # debris; the service reclaims it on its next start, so this only warns.
        cutoff=$(date -d "-${STALE_LEASE_MINUTES} minutes" +%s 2>/dev/null \
                 || date -v-"${STALE_LEASE_MINUTES}"M +%s)
        stale=0
        while read -r dir; do
            [[ -n "$dir" ]] || continue
            lease_line=$(hdfs dfs -ls "${dir}/.instance-lease" 2>/dev/null | tail -1 || true)
            if [[ -z "$lease_line" ]]; then
                echo "STALE (no lease): ${dir}"
                stale=$((stale + 1))
                continue
            fi
            lease_ts=$(date -d "$(echo "$lease_line" | awk '{print $6" "$7}')" +%s 2>/dev/null || echo 0)
            if (( lease_ts < cutoff )); then
                echo "STALE (lease older than ${STALE_LEASE_MINUTES}m): ${dir}"
                stale=$((stale + 1))
            fi
        done < <(hdfs dfs -ls "$TMP_PATH" 2>/dev/null | awk 'NR>1 && $1 ~ /^d/ {print $8}')

        if (( stale > 0 )); then
            echo "WARNING: ${stale} abandoned staging director$([[ $stale -eq 1 ]] && echo y || echo ies)"
            exit 1
        fi
        echo "OK: every staging directory has a live lease"
        ;;

    help|*)
        sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        echo "Base: ${BASE_DIR}   Release: ${RELEASE_DIR}"
        [[ "$COMMAND" == "help" ]] && exit 0 || exit 2
        ;;
esac
