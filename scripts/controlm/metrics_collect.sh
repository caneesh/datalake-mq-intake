#!/bin/bash
#
# MQ Intake Metrics Collection for Control-M
#
# Collects key metrics and outputs in a format suitable for trending/reporting.
#
# Usage: metrics_collect.sh <app> [port] [output_file]
#
# Exit codes:
#   0 - Metrics collected successfully
#   1 - Failed to collect metrics

set -euo pipefail

APP="${1:-}"
PORT="${2:-}"
OUTPUT_FILE="${3:-}"

if [[ -z "$APP" ]]; then
    echo "Usage: $0 <rms|claims> [port] [output_file]"
    exit 1
fi

# Default ports
case "$APP" in
    rms)    PORT="${PORT:-8081}" ;;
    claims) PORT="${PORT:-8099}" ;;
    *)      echo "Unknown app: $APP"; exit 1 ;;
esac

METRICS_BASE="http://localhost:${PORT}/actuator/metrics"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
TIMESTAMP_EPOCH=$(date +%s)

echo "=== MQ Intake Metrics Collection ==="
echo "App:       $APP"
echo "Port:      $PORT"
echo "Timestamp: $TIMESTAMP"
echo ""

# Function to fetch a metric value
get_metric() {
    local metric_name="$1"
    local default_value="${2:-0}"

    local response
    response=$(curl -s --connect-timeout 5 "${METRICS_BASE}/${metric_name}" 2>/dev/null || echo '{}')

    local value
    value=$(echo "$response" | jq -r '.measurements[0].value // empty' 2>/dev/null)

    if [[ -z "$value" ]]; then
        echo "$default_value"
    else
        echo "$value"
    fi
}

# Collect metrics
MESSAGES_CONSUMED=$(get_metric "mq_intake_messages_consumed_total")
BATCHES_COMMITTED=$(get_metric "mq_intake_batches_committed_total")
BYTES_WRITTEN=$(get_metric "mq_intake_bytes_written_total")
SOURCE_QUEUE_DEPTH=$(get_metric "mq_intake_source_queue_depth")
BACKOUT_QUEUE_DEPTH=$(get_metric "mq_intake_backout_queue_depth")
RECONNECT_COUNT=$(get_metric "mq_intake_reconnect_total")
RECONNECT_FAILURES=$(get_metric "mq_intake_reconnect_failures_total")

# JVM metrics
HEAP_USED=$(get_metric "jvm.memory.used" | awk '{printf "%.0f", $1/1048576}')  # Convert to MB
HEAP_MAX=$(get_metric "jvm.memory.max" | awk '{printf "%.0f", $1/1048576}')
GC_PAUSE=$(get_metric "jvm.gc.pause")

# Calculate derived metrics
if [[ "$BATCHES_COMMITTED" != "0" ]] && [[ -n "$BATCHES_COMMITTED" ]]; then
    AVG_BATCH_SIZE=$(echo "scale=2; $MESSAGES_CONSUMED / $BATCHES_COMMITTED" | bc 2>/dev/null || echo "0")
else
    AVG_BATCH_SIZE="0"
fi

HEAP_PERCENT=$(echo "scale=1; $HEAP_USED * 100 / $HEAP_MAX" | bc 2>/dev/null || echo "0")

# Output results
echo "=== Collected Metrics ==="
echo ""
printf "%-30s %s\n" "Messages Consumed:" "$MESSAGES_CONSUMED"
printf "%-30s %s\n" "Batches Committed:" "$BATCHES_COMMITTED"
printf "%-30s %s\n" "Avg Batch Size:" "$AVG_BATCH_SIZE"
printf "%-30s %s\n" "Bytes Written:" "$BYTES_WRITTEN"
printf "%-30s %s\n" "Source Queue Depth:" "$SOURCE_QUEUE_DEPTH"
printf "%-30s %s\n" "Backout Queue Depth:" "$BACKOUT_QUEUE_DEPTH"
printf "%-30s %s\n" "Reconnect Count:" "$RECONNECT_COUNT"
printf "%-30s %s\n" "Reconnect Failures:" "$RECONNECT_FAILURES"
printf "%-30s %s MB\n" "Heap Used:" "$HEAP_USED"
printf "%-30s %s MB\n" "Heap Max:" "$HEAP_MAX"
printf "%-30s %s%%\n" "Heap Usage:" "$HEAP_PERCENT"
echo ""

# Output to file if specified (CSV format for trending)
if [[ -n "$OUTPUT_FILE" ]]; then
    # Create header if file doesn't exist
    if [[ ! -f "$OUTPUT_FILE" ]]; then
        echo "timestamp,app,messages_consumed,batches_committed,avg_batch_size,bytes_written,source_queue_depth,backout_queue_depth,reconnects,reconnect_failures,heap_used_mb,heap_max_mb,heap_percent" > "$OUTPUT_FILE"
    fi

    # Append data row
    echo "${TIMESTAMP_EPOCH},${APP},${MESSAGES_CONSUMED},${BATCHES_COMMITTED},${AVG_BATCH_SIZE},${BYTES_WRITTEN},${SOURCE_QUEUE_DEPTH},${BACKOUT_QUEUE_DEPTH},${RECONNECT_COUNT},${RECONNECT_FAILURES},${HEAP_USED},${HEAP_MAX},${HEAP_PERCENT}" >> "$OUTPUT_FILE"

    echo "Metrics appended to: $OUTPUT_FILE"
fi

echo "Collection complete"
exit 0
