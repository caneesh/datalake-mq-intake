#!/bin/bash
#
# MQ Intake Queue Depth Check for Control-M
#
# Usage: queue_depth_check.sh <app> [port] [warn_threshold] [crit_threshold]
#
# Exit codes:
#   0 - OK (queue depth below warning threshold)
#   1 - WARNING (queue depth above warning threshold)
#   2 - CRITICAL (queue depth above critical threshold)
#   3 - UNKNOWN (cannot determine)

set -euo pipefail

APP="${1:-}"
PORT="${2:-}"
WARN_THRESHOLD="${3:-10000}"
CRIT_THRESHOLD="${4:-50000}"

if [[ -z "$APP" ]]; then
    echo "Usage: $0 <rms|claims> [port] [warn_threshold] [crit_threshold]"
    exit 3
fi

# Default ports
case "$APP" in
    rms)    PORT="${PORT:-8081}" ;;
    claims) PORT="${PORT:-8099}" ;;
    *)      echo "Unknown app: $APP"; exit 3 ;;
esac

METRICS_URL="http://localhost:${PORT}/actuator/metrics/mq_intake_source_queue_depth"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "=== MQ Intake Queue Depth Check ==="
echo "App:            $APP"
echo "Port:           $PORT"
echo "Warn Threshold: $WARN_THRESHOLD"
echo "Crit Threshold: $CRIT_THRESHOLD"
echo "Timestamp:      $TIMESTAMP"
echo ""

# Fetch metric
RESPONSE=$(curl -s --connect-timeout 10 "$METRICS_URL" 2>/dev/null || echo '{"error": true}')

if echo "$RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
    echo "UNKNOWN: Cannot fetch queue depth metric"
    exit 3
fi

DEPTH=$(echo "$RESPONSE" | jq -r '.measurements[0].value // -1' 2>/dev/null)

if [[ "$DEPTH" == "-1" ]] || [[ -z "$DEPTH" ]]; then
    echo "UNKNOWN: Cannot parse queue depth"
    exit 3
fi

# Convert to integer
DEPTH=${DEPTH%.*}

echo "Queue Depth: $DEPTH"
echo ""

# Check thresholds
if (( DEPTH >= CRIT_THRESHOLD )); then
    echo "CRITICAL: Queue depth $DEPTH exceeds critical threshold $CRIT_THRESHOLD"
    echo "Action: Check if consumers are running, consider scaling"
    exit 2
elif (( DEPTH >= WARN_THRESHOLD )); then
    echo "WARNING: Queue depth $DEPTH exceeds warning threshold $WARN_THRESHOLD"
    echo "Action: Monitor closely, may need attention soon"
    exit 1
else
    echo "OK: Queue depth $DEPTH is within normal range"
    exit 0
fi
