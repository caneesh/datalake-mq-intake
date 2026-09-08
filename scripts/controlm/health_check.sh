#!/bin/bash
#
# MQ Intake Health Check Script for Control-M
#
# Usage: health_check.sh <app> [port]
#   app:  rms | claims
#   port: actuator port (optional, defaults based on app)
#
# Exit codes:
#   0 - Healthy (UP)
#   1 - Degraded (some threads down)
#   2 - Down (all threads down or unreachable)
#   3 - Unknown (cannot determine status)

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

HEALTH_URL="http://localhost:${PORT}/actuator/health"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "=== MQ Intake Health Check ==="
echo "App:       $APP"
echo "Port:      $PORT"
echo "Timestamp: $TIMESTAMP"
echo ""

# Fetch health endpoint
HTTP_CODE=$(curl -s -o /tmp/health_${APP}.json -w "%{http_code}" --connect-timeout 10 "$HEALTH_URL" 2>/dev/null || echo "000")

if [[ "$HTTP_CODE" == "000" ]]; then
    echo "CRITICAL: Cannot connect to health endpoint"
    echo "URL: $HEALTH_URL"
    exit 2
fi

# Parse response
STATUS=$(jq -r '.status // "UNKNOWN"' /tmp/health_${APP}.json 2>/dev/null || echo "PARSE_ERROR")

# Get binding details if available
BINDING_STATUS=$(jq -r ".components.bindings.details.${APP}.status // \"N/A\"" /tmp/health_${APP}.json 2>/dev/null || echo "N/A")
ACTIVE_THREADS=$(jq -r ".components.bindings.details.${APP}.activeThreads // \"N/A\"" /tmp/health_${APP}.json 2>/dev/null || echo "N/A")
TOTAL_THREADS=$(jq -r ".components.bindings.details.${APP}.totalThreads // \"N/A\"" /tmp/health_${APP}.json 2>/dev/null || echo "N/A")

echo "HTTP Code:      $HTTP_CODE"
echo "Overall Status: $STATUS"
echo "Binding Status: $BINDING_STATUS"
echo "Threads:        $ACTIVE_THREADS / $TOTAL_THREADS"
echo ""

# Determine exit code
case "$STATUS" in
    UP)
        echo "RESULT: OK - Service is healthy"
        exit 0
        ;;
    DEGRADED)
        echo "RESULT: WARNING - Service is degraded"
        echo "Some listener threads may be down"
        exit 1
        ;;
    PARTIAL_OUTAGE)
        echo "RESULT: WARNING - Partial outage detected"
        exit 1
        ;;
    DOWN)
        echo "RESULT: CRITICAL - Service is DOWN"
        echo "All listener threads may be dead"
        exit 2
        ;;
    *)
        echo "RESULT: UNKNOWN - Cannot determine status"
        exit 3
        ;;
esac
