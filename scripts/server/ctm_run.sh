#!/bin/bash
#
# Control-M wrapper script for MQ Intake
#
# Automatically detects paths based on script location.
# No hardcoded paths required.
#
# Usage:
#   ctm_run.sh <command> [args]
#
# Commands:
#   start       - Start the service
#   stop        - Stop the service
#   status      - Check service status
#   preflight   - Run preflight checks
#   health      - Check health endpoint (returns 0=UP, 1=DOWN)
#   metrics     - Output Prometheus metrics
#   log-cleanup - Delete logs older than LOG_RETENTION_DAYS
#   tmp-check   - Check for stale HDFS temp files
#
# Exit codes:
#   0 - Success / UP
#   1 - Failure / DOWN
#   2 - Warning / DEGRADED

set -euo pipefail

# Auto-detect paths from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# APP_BASE is parent of 'current' which contains this script
# Structure: APP_BASE/current/ctm_run.sh
if [[ "$SCRIPT_DIR" == */current ]]; then
    APP_BASE="$(dirname "$SCRIPT_DIR")"
else
    APP_BASE="$SCRIPT_DIR"
fi

# Load Control-M config if exists
CONFIG_FILE="$SCRIPT_DIR/env_controlm.conf"
if [[ -f "$CONFIG_FILE" ]]; then
    source "$CONFIG_FILE"
fi

# Defaults if not in config
APP_NAME="${APP_NAME:-unknown}"
SERVER_PORT="${SERVER_PORT:-8080}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-30}"
HDFS_BASE="${HDFS_BASE:-}"

# Commands
COMMAND="${1:-help}"
shift || true

case "$COMMAND" in
    start|stop|status|preflight)
        exec "$SCRIPT_DIR/intake.sh" "$COMMAND" "$@"
        ;;

    health)
        # Simple health check - returns 0 if UP, 1 otherwise
        HEALTH=$(curl -sf "http://localhost:${SERVER_PORT}/actuator/health" 2>/dev/null || echo '{"status":"DOWN"}')
        STATUS=$(echo "$HEALTH" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)

        if [[ "$STATUS" == "UP" ]]; then
            echo "UP"
            exit 0
        else
            echo "DOWN: $STATUS"
            exit 1
        fi
        ;;

    metrics)
        curl -sf "http://localhost:${SERVER_PORT}/actuator/prometheus" 2>/dev/null || {
            echo "ERROR: Cannot fetch metrics"
            exit 1
        }
        ;;

    log-cleanup)
        LOG_DIR="$APP_BASE/logs"
        if [[ -d "$LOG_DIR" ]]; then
            echo "Cleaning logs older than $LOG_RETENTION_DAYS days in $LOG_DIR"
            find "$LOG_DIR" -type f \( -name "*.log*" -o -name "*.gz" \) -mtime +${LOG_RETENTION_DAYS} -print -delete
            echo "Done"
        else
            echo "Log directory not found: $LOG_DIR"
        fi
        ;;

    tmp-check)
        if [[ -z "$HDFS_BASE" ]]; then
            echo "ERROR: HDFS_BASE not configured"
            exit 1
        fi
        TMP_PATH="$HDFS_BASE/_tmp"
        echo "Checking for stale files in $TMP_PATH"

        # List files, exit 1 if any found (excluding current instance)
        FILES=$(hdfs dfs -ls "$TMP_PATH" 2>/dev/null | grep -v "^Found" | wc -l)
        if [[ "$FILES" -gt 0 ]]; then
            echo "WARNING: Found $FILES staging directories"
            hdfs dfs -ls "$TMP_PATH"
            exit 1
        else
            echo "OK: No stale staging files"
            exit 0
        fi
        ;;

    help|*)
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  start       - Start the service"
        echo "  stop        - Stop the service"
        echo "  status      - Check service status"
        echo "  preflight   - Run preflight checks"
        echo "  health      - Check health endpoint"
        echo "  metrics     - Output Prometheus metrics"
        echo "  log-cleanup - Delete old log files"
        echo "  tmp-check   - Check for stale HDFS temp files"
        echo ""
        echo "Configuration: $CONFIG_FILE"
        echo "App Base: $APP_BASE"
        ;;
esac
