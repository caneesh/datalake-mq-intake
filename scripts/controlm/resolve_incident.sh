#!/bin/bash
#
# Resolve ServiceNow Incident when Control-M Job Succeeds
#
# Usage: resolve_incident.sh <job_name>
#
# Environment variables required:
#   SNOW_INSTANCE  - ServiceNow instance
#   SNOW_API_USER  - ServiceNow API username
#   SNOW_API_PASS  - ServiceNow API password
#
# Exit codes:
#   0 - Incident resolved or no open incident found
#   1 - Failed to resolve incident

set -euo pipefail

JOB_NAME="${1:-}"

if [[ -z "$JOB_NAME" ]]; then
    echo "Usage: $0 <job_name>"
    exit 1
fi

# ServiceNow configuration
SNOW_INSTANCE="${SNOW_INSTANCE:-}"
SNOW_API_USER="${SNOW_API_USER:-}"
SNOW_API_PASS="${SNOW_API_PASS:-}"

INCIDENT_LOG="${CTM_LOG_DIR:-/var/log/ctm}/open_incidents.log"

# Check if there's an open incident for this job
if [[ ! -f "$INCIDENT_LOG" ]]; then
    echo "No incident log found, nothing to resolve"
    exit 0
fi

# Find the most recent open incident for this job
INCIDENT_LINE=$(grep "^${JOB_NAME}|" "$INCIDENT_LOG" | tail -1 || true)

if [[ -z "$INCIDENT_LINE" ]]; then
    echo "No open incident found for job: $JOB_NAME"
    exit 0
fi

INC_SYSID=$(echo "$INCIDENT_LINE" | cut -d'|' -f2)
INC_NUMBER=$(echo "$INCIDENT_LINE" | cut -d'|' -f3)
CREATED_AT=$(echo "$INCIDENT_LINE" | cut -d'|' -f4)

echo "Found open incident: $INC_NUMBER (created: $CREATED_AT)"

if [[ -z "$SNOW_INSTANCE" ]] || [[ -z "$SNOW_API_USER" ]] || [[ -z "$SNOW_API_PASS" ]]; then
    echo "WARNING: ServiceNow credentials not configured"
    echo "Cannot auto-resolve incident $INC_NUMBER"
    echo "Please resolve manually"
    exit 0
fi

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Update incident to resolved
RESOLVE_JSON=$(cat <<EOF
{
    "state": "6",
    "close_code": "Solved (Permanently)",
    "close_notes": "Job ${JOB_NAME} succeeded at ${TIMESTAMP}. Auto-resolved by Control-M."
}
EOF
)

echo "Resolving incident ${INC_NUMBER}..."

RESPONSE=$(curl -s -X PATCH \
    "https://${SNOW_INSTANCE}/api/now/table/incident/${INC_SYSID}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -u "${SNOW_API_USER}:${SNOW_API_PASS}" \
    --connect-timeout 30 \
    -d "${RESOLVE_JSON}")

# Check if resolved
NEW_STATE=$(echo "$RESPONSE" | jq -r '.result.state // empty')

if [[ "$NEW_STATE" == "6" ]]; then
    echo "SUCCESS: Resolved incident ${INC_NUMBER}"

    # Remove from open incidents log
    grep -v "^${JOB_NAME}|${INC_SYSID}" "$INCIDENT_LOG" > "${INCIDENT_LOG}.tmp" || true
    mv "${INCIDENT_LOG}.tmp" "$INCIDENT_LOG"

    exit 0
else
    echo "WARNING: Could not confirm incident resolution"
    echo "Response: $RESPONSE"
    echo "Please verify incident ${INC_NUMBER} status manually"
    exit 0
fi
