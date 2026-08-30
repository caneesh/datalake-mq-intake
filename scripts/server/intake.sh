#!/usr/bin/env bash
#
# Server-side control for the MQ -> HDFS intake service.
#
#   ./current/intake.sh preflight [mq|hdfs|app]   probe dependencies, change nothing
#   ./current/intake.sh start                     start in the background
#   ./current/intake.sh stop                      graceful stop (drains in-flight batches)
#   ./current/intake.sh status                    pid, health, key metrics
#   ./current/intake.sh logs [-f]                 tail the current log
#   ./current/intake.sh config                    show effective settings (no secrets)
#
# Reads <base>/env.sh for environment and credentials. Deployed by
# scripts/deploy.sh; lives inside the release directory so a rollback takes
# the matching control script with it.

set -euo pipefail

# -P resolves symlinks: this script is normally invoked through 'current',
# and without it '../..' walks up from the symlink's logical path rather than
# from releases/<stamp>/, landing outside the deployment entirely.
RELEASE_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd -P "${RELEASE_DIR}/../.." && pwd)"

JAR="${RELEASE_DIR}/app.jar"
ENV_FILE="${BASE_DIR}/env.sh"
CONFIG_DIR="${BASE_DIR}/config"
LOG_DIR="${BASE_DIR}/logs"
PID_FILE="${BASE_DIR}/run/intake.pid"

[[ -f "$JAR" ]] || { echo "missing ${JAR}" >&2; exit 1; }

if [[ -f "$ENV_FILE" ]]; then
    perms=$(stat -c '%a' "$ENV_FILE" 2>/dev/null || stat -f '%Lp' "$ENV_FILE")
    if [[ "$perms" != "600" && "$perms" != "400" ]]; then
        echo "WARNING: ${ENV_FILE} is mode ${perms}; it holds credentials. chmod 600 it." >&2
    fi
    # shellcheck disable=SC1090
    set -a; source "$ENV_FILE"; set +a
else
    echo "WARNING: no ${ENV_FILE}; running with built-in defaults, which production mode refuses" >&2
fi

JAVA_OPTS="${JAVA_OPTS:--Xmx4g}"

# The jar targets Java 11. An older JVM fails with UnsupportedClassVersionError
# — a message that names a class-file version rather than the actual problem —
# so say it plainly here instead. A newer JVM is fine and is not blocked.
require_java() {
    command -v java > /dev/null 2>&1 || {
        echo "java is not on PATH. This service needs a Java 11 runtime." >&2
        exit 1
    }
    local raw major
    raw=$(java -version 2>&1 | head -1 | sed 's/.*version "\([^"]*\)".*/\1/')
    major=${raw%%.*}
    [[ "$major" == "1" ]] && major=$(echo "$raw" | cut -d. -f2)   # 1.8.0_x style
    if [[ "$major" =~ ^[0-9]+$ ]] && (( major < 11 )); then
        echo "Java ${raw} found; this service needs Java 11 or newer." >&2
        echo "Point PATH (or JAVA_HOME/bin) at the Java 11 runtime and retry." >&2
        exit 1
    fi
}
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/actuator/health}"
METRICS_URL="${METRICS_URL:-http://localhost:8080/actuator/metrics}"

# A config directory is only passed to Spring when it actually holds something;
# an empty --spring.config.additional-location is a confusing no-op.
CONFIG_ARG=()
if compgen -G "${CONFIG_DIR}/*.yml" > /dev/null || compgen -G "${CONFIG_DIR}/*.properties" > /dev/null; then
    CONFIG_ARG=(--spring.config.additional-location="file:${CONFIG_DIR}/")
fi

running_pid() {
    [[ -f "$PID_FILE" ]] || return 1
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null) || return 1
    [[ -n "$pid" ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    echo "$pid"
}

cmd_is_running() {
    running_pid > /dev/null
}

cmd_preflight() {
    require_java
    local group="${1:-}"
    local args=(--intake.preflight.enabled=true)
    # Either form works: PreflightConfiguration reads --preflight=<group> from
    # the application arguments and unions it with intake.preflight.only. The
    # property form is used here because it is the one a config file or
    # manifest can also set.
    [[ -n "$group" ]] && args+=(--intake.preflight.only="$group")
    echo "Preflight — probing dependencies. Nothing is consumed and nothing is started."
    # Exit status propagates: 0 clean, 1 if any check failed.
    java $JAVA_OPTS -jar "$JAR" "${CONFIG_ARG[@]}" "${args[@]}" --logging.level.root=WARN
}

cmd_start() {
    require_java
    if pid=$(running_pid); then
        echo "Already running (pid ${pid}). Use 'stop' first." >&2
        exit 1
    fi
    mkdir -p "$LOG_DIR" "$(dirname "$PID_FILE")"
    local log="${LOG_DIR}/intake-$(date -u +%Y%m%dT%H%M%SZ).log"

    echo "Starting; log: ${log}"
    nohup java $JAVA_OPTS -jar "$JAR" "${CONFIG_ARG[@]}" > "$log" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"
    ln -sfn "$log" "${LOG_DIR}/current.log"

    # Confirm it survived startup rather than reporting success for a process
    # that died on a config gate three seconds later.
    for _ in $(seq 1 30); do
        sleep 1
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "FAILED to start — last lines:" >&2
            tail -30 "$log" >&2
            rm -f "$PID_FILE"
            exit 1
        fi
        if grep -q "IntakeRuntimeManager started" "$log" 2>/dev/null; then
            echo "Started (pid ${pid})."
            grep -m1 "IntakeRuntimeManager started" "$log"
            return 0
        fi
    done
    echo "Started (pid ${pid}) but no startup confirmation after 30s — check ${log}" >&2
}

cmd_stop() {
    local pid
    if ! pid=$(running_pid); then
        echo "Not running."
        rm -f "$PID_FILE"
        return 0
    fi
    # SIGTERM, never SIGKILL: the loop drains and commits its in-flight batch on
    # shutdown. Killing skips the drain — nothing is lost (the batch rolls back
    # and MQ redelivers) but it manufactures avoidable duplicates.
    local timeout="${STOP_TIMEOUT_SECONDS:-90}"
    echo "Stopping pid ${pid} (SIGTERM; draining in-flight batches, up to ${timeout}s)"
    kill -TERM "$pid"
    for _ in $(seq 1 "$timeout"); do
        sleep 1
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "Stopped cleanly."
            rm -f "$PID_FILE"
            return 0
        fi
    done
    echo "Still running after ${timeout}s. NOT killing: a forced kill during a commit" >&2
    echo "is safe for delivery but skips the drain. Inspect ${LOG_DIR}/current.log," >&2
    echo "then 'kill -9 ${pid}' deliberately if you accept that." >&2
    exit 1
}

cmd_status() {
    if pid=$(running_pid); then
        echo "process : running (pid ${pid}, up $(ps -o etime= -p "$pid" | tr -d ' '))"
    else
        echo "process : NOT running"
    fi
    if [[ -f "${RELEASE_DIR}/RELEASE" ]]; then
        sed 's/^/release : /' "${RELEASE_DIR}/RELEASE"
        # Integrity, not security: confirms the jar is byte-identical to the
        # one the build machine shipped. Useful where the build host has no
        # version control, because then the checksum IS the version.
        local recorded actual
        recorded=$(grep '^jar_sha256=' "${RELEASE_DIR}/RELEASE" 2>/dev/null | cut -d= -f2 || true)
        if [[ -n "$recorded" ]]; then
            actual=$(sha256sum "$JAR" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "$JAR" | cut -d' ' -f1)
            if [[ "$recorded" == "$actual" ]]; then
                echo "release : jar_verified=yes"
            else
                echo "release : jar_verified=NO — on-disk jar differs from the deployed one!" >&2
            fi
        fi
    fi

    if command -v curl > /dev/null; then
        local health
        health=$(curl -s --max-time 5 "$HEALTH_URL" 2>/dev/null || true)
        if [[ -n "$health" ]]; then
            echo "health  : $(echo "$health" | head -c 400)"
            for m in messages_consumed_total messages_written_total \
                     batches_committed_total batches_rolled_back_total \
                     balance_check_failures_total backout_queue_depth \
                     identity_misses_total suspect_count; do
                local v
                v=$(curl -s --max-time 5 "${METRICS_URL}/mq_intake_${m}" 2>/dev/null \
                    | grep -o '"value":[0-9.E]*' | head -1 | cut -d: -f2 || true)
                [[ -n "$v" ]] && printf 'metric  : %-32s %s\n' "$m" "$v"
            done
        else
            echo "health  : endpoint not answering at ${HEALTH_URL}"
        fi
    fi
}

cmd_logs() {
    local log="${LOG_DIR}/current.log"
    [[ -e "$log" ]] || { echo "no log yet at ${log}" >&2; exit 1; }
    if [[ "${1:-}" == "-f" ]]; then tail -f "$log"; else tail -100 "$log"; fi
}

cmd_config() {
    # Deliberately prints variable NAMES and non-secret values only. Anything
    # matching a secret-ish name is shown as set/unset, never echoed.
    echo "release   : ${RELEASE_DIR}"
    echo "env file  : ${ENV_FILE}"
    echo "config dir: ${CONFIG_DIR} $([[ ${#CONFIG_ARG[@]} -gt 0 ]] && echo '(in use)' || echo '(empty — using built-in defaults)')"
    echo "java opts : ${JAVA_OPTS}"
    echo "java      : $(command -v java > /dev/null 2>&1 && java -version 2>&1 | head -1 || echo '<not on PATH>')"
    echo
    for var in MQ_HOST MQ_PORT MQ_QUEUE_MANAGER MQ_CHANNEL MQ_SOURCE_QUEUE \
               MQ_TRACKER_QUEUE MQ_BACKOUT_QUEUE HDFS_BASE_PATH HDFS_AUDIT_BASE_PATH \
               KERBEROS_ENABLED KERBEROS_PRINCIPAL KERBEROS_KEYTAB_PATH \
               MQ_INTAKE_PRODUCTION SPRING_PROFILES_ACTIVE INTAKE_INSTANCE_ID; do
        printf '%-22s %s\n' "$var" "${!var:-<unset>}"
    done
    for var in MQ_CREDENTIAL_REF MQ_USER MQ_PASSWORD; do
        printf '%-22s %s\n' "$var" "$([[ -n "${!var:-}" ]] && echo '<set>' || echo '<unset>')"
    done
}

case "${1:-}" in
    preflight)  shift; cmd_preflight "${1:-}" ;;
    start)      cmd_start ;;
    stop)       cmd_stop ;;
    restart)    cmd_stop || true; cmd_start ;;
    status)     cmd_status ;;
    logs)       shift; cmd_logs "${1:-}" ;;
    config)     cmd_config ;;
    is-running) cmd_is_running ;;
    *)
        cat >&2 <<'USAGE'
usage: intake.sh <command>

  preflight [mq|hdfs|app]  probe dependencies; consumes nothing, starts nothing
  start                    start in the background
  stop                     graceful stop; drains in-flight batches
  restart                  stop then start
  status                   pid, release, health, key metrics
  logs [-f]                tail the current log
  config                   effective settings (secrets shown as set/unset only)
USAGE
        exit 2 ;;
esac
