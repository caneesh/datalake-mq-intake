#!/bin/bash
#
# MQ Intake Log Cleanup for Control-M
#
# Removes log files older than retention period.
#
# Usage: log_cleanup.sh <app_base_dir> [retention_days]
#
# Exit codes:
#   0 - Cleanup successful
#   1 - Cleanup failed

set -euo pipefail

APP_BASE="${1:-}"
RETENTION_DAYS="${2:-30}"

if [[ -z "$APP_BASE" ]]; then
    echo "Usage: $0 <app_base_dir> [retention_days]"
    exit 1
fi

LOG_DIR="${APP_BASE}/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "=== MQ Intake Log Cleanup ==="
echo "Directory:      $LOG_DIR"
echo "Retention Days: $RETENTION_DAYS"
echo "Timestamp:      $TIMESTAMP"
echo ""

if [[ ! -d "$LOG_DIR" ]]; then
    echo "WARNING: Log directory does not exist: $LOG_DIR"
    exit 0
fi

# Count files before cleanup
BEFORE_COUNT=$(find "$LOG_DIR" -type f \( -name "*.log*" -o -name "*.gz" \) | wc -l)
BEFORE_SIZE=$(du -sh "$LOG_DIR" 2>/dev/null | cut -f1)

echo "Before cleanup:"
echo "  Files: $BEFORE_COUNT"
echo "  Size:  $BEFORE_SIZE"
echo ""

# Find and delete old log files
echo "Removing files older than $RETENTION_DAYS days..."

DELETED_FILES=$(find "$LOG_DIR" -type f \( -name "*.log*" -o -name "*.gz" \) -mtime +${RETENTION_DAYS} -print -delete 2>&1)

if [[ -n "$DELETED_FILES" ]]; then
    echo "Deleted files:"
    echo "$DELETED_FILES" | head -20
    DELETED_COUNT=$(echo "$DELETED_FILES" | wc -l)
    if (( DELETED_COUNT > 20 )); then
        echo "... and $((DELETED_COUNT - 20)) more"
    fi
else
    echo "No files to delete"
fi

echo ""

# Count files after cleanup
AFTER_COUNT=$(find "$LOG_DIR" -type f \( -name "*.log*" -o -name "*.gz" \) | wc -l)
AFTER_SIZE=$(du -sh "$LOG_DIR" 2>/dev/null | cut -f1)

echo "After cleanup:"
echo "  Files: $AFTER_COUNT"
echo "  Size:  $AFTER_SIZE"
echo ""

echo "Cleanup complete"
exit 0
