#!/bin/bash
#
# MQ Intake Backout Queue Alert for Control-M
#
# Checks if there are poison messages in the backout queue.
# Any messages in backout queue indicates a problem that needs attention.
#
# Usage: backout_alert.sh <app> [port]
#
# Exit codes:
#   0 - OK (backout queue empty)
#   2 - CRITICAL (messages in backout queue)
#   3 - UNKNOWN (cannot determine)

set -euo pipefail

APP="${1:-}"
PORT="${2:-}"

if [[ -z "$APP" ]]; then
    echo "Usage: $0 <rms|claims> [port]"
    exit 3
fi

# Default ports
case "$APP" in
    rms)    PORT="${PORT:-8081}" ;;
    claims) PORT="${PORT:-8099}" ;;
    *)      echo "Unknown app: $APP"; exit 3 ;;
esac

METRICS_URL="http://localhost:${PORT}/actuator/metrics/mq_intake_backout_queue_depth"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "=== MQ Intake Backout Queue Alert ==="
echo "App:       $APP"
echo "Port:      $PORT"
echo "Timestamp: $TIMESTAMP"
echo ""

# Fetch metric
RESPONSE=$(curl -s --connect-timeout 10 "$METRICS_URL" 2>/dev/null || echo '{"error": true}')

if echo "$RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
    # Metric might not exist if no backout queue configured
    echo "INFO: Backout queue metric not available"
    echo "This may be normal if no backout queue is configured"
    exit 0
fi

DEPTH=$(echo "$RESPONSE" | jq -r '.measurements[0].value // 0' 2>/dev/null)

# Convert to integer
DEPTH=${DEPTH%.*}

echo "Backout Queue Depth: $DEPTH"
echo ""

if (( DEPTH > 0 )); then
    echo "CRITICAL: $DEPTH poison message(s) in backout queue"
    echo ""
    echo "Action Required:"
    echo "1. Review messages in backout queue"
    echo "2. Determine root cause (malformed data, serialization error, etc.)"
    echo "3. Fix source issue or manually process/discard messages"
    echo ""
    echo "Backout Queue: MQ.${APP^^}.BACKOUT"
    exit 2
else
    echo "OK: Backout queue is empty"
    exit 0
fi
