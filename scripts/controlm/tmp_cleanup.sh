#!/bin/bash
#
# MQ Intake HDFS Temp File Cleanup for Control-M
#
# Checks for orphaned temp files in HDFS _tmp directories.
# Only reports - does not auto-delete (safety first).
#
# Usage: tmp_cleanup.sh <hdfs_base_path> [max_age_hours]
#
# Exit codes:
#   0 - No stale files found
#   1 - Stale files found (needs attention)
#   2 - Error checking HDFS

set -euo pipefail

HDFS_BASE="${1:-}"
MAX_AGE_HOURS="${2:-2}"

if [[ -z "$HDFS_BASE" ]]; then
    echo "Usage: $0 <hdfs_base_path> [max_age_hours]"
    exit 2
fi

TMP_PATH="${HDFS_BASE}/_tmp"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
CURRENT_EPOCH=$(date +%s)
MAX_AGE_SECONDS=$((MAX_AGE_HOURS * 3600))

echo "=== MQ Intake HDFS Temp Cleanup Check ==="
echo "HDFS Path:     $TMP_PATH"
echo "Max Age Hours: $MAX_AGE_HOURS"
echo "Timestamp:     $TIMESTAMP"
echo ""

# Check if _tmp directory exists
if ! hdfs dfs -test -d "$TMP_PATH" 2>/dev/null; then
    echo "INFO: Temp directory does not exist: $TMP_PATH"
    echo "This is normal if the service has not run yet"
    exit 0
fi

# List instance directories
INSTANCE_DIRS=$(hdfs dfs -ls "$TMP_PATH" 2>/dev/null | awk 'NR>1 {print $8}' || true)

if [[ -z "$INSTANCE_DIRS" ]]; then
    echo "INFO: No instance directories found in $TMP_PATH"
    exit 0
fi

STALE_FOUND=0
STALE_FILES=""

echo "Checking instance directories..."
echo ""

for dir in $INSTANCE_DIRS; do
    INSTANCE=$(basename "$dir")
    echo "Instance: $INSTANCE"

    # List files in this instance directory
    FILES=$(hdfs dfs -ls "$dir" 2>/dev/null | awk 'NR>1 {print $6, $7, $8}' || true)

    if [[ -z "$FILES" ]]; then
        echo "  (empty directory)"
        continue
    fi

    while IFS= read -r line; do
        FILE_DATE=$(echo "$line" | awk '{print $1}')
        FILE_TIME=$(echo "$line" | awk '{print $2}')
        FILE_PATH=$(echo "$line" | awk '{print $3}')

        if [[ -z "$FILE_PATH" ]]; then
            continue
        fi

        # Calculate file age
        FILE_DATETIME="${FILE_DATE} ${FILE_TIME}"
        FILE_EPOCH=$(date -d "$FILE_DATETIME" +%s 2>/dev/null || echo "0")

        if [[ "$FILE_EPOCH" == "0" ]]; then
            echo "  WARNING: Cannot parse date for $FILE_PATH"
            continue
        fi

        AGE_SECONDS=$((CURRENT_EPOCH - FILE_EPOCH))
        AGE_HOURS=$((AGE_SECONDS / 3600))

        if (( AGE_SECONDS > MAX_AGE_SECONDS )); then
            echo "  STALE: $(basename "$FILE_PATH") (${AGE_HOURS}h old)"
            STALE_FOUND=1
            STALE_FILES="${STALE_FILES}${FILE_PATH}\n"
        else
            echo "  OK: $(basename "$FILE_PATH") (${AGE_HOURS}h old)"
        fi
    done <<< "$FILES"

    echo ""
done

if (( STALE_FOUND )); then
    echo "========================================"
    echo "WARNING: Stale temp files found!"
    echo "========================================"
    echo ""
    echo "These files may indicate:"
    echo "1. A crashed instance that didn't clean up"
    echo "2. HDFS lease issues preventing file closure"
    echo "3. A stuck batch that never committed"
    echo ""
    echo "Manual review required before deletion."
    echo "To remove an abandoned instance directory:"
    echo "  hdfs dfs -rm -r <instance_path>"
    echo ""
    echo "Stale files:"
    echo -e "$STALE_FILES"
    exit 1
else
    echo "OK: No stale temp files found"
    exit 0
fi
