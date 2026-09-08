#!/bin/bash
#
# Create ServiceNow Incident for Control-M Job Failure
#
# Usage: create_incident.sh <job_name> <status> <order_id> [priority]
#
# Environment variables required:
#   SNOW_INSTANCE  - ServiceNow instance (e.g., company.service-now.com)
#   SNOW_API_USER  - ServiceNow API username
#   SNOW_API_PASS  - ServiceNow API password
#
# Exit codes:
#   0 - Incident created successfully
#   1 - Failed to create incident

set -euo pipefail

JOB_NAME="${1:-}"
STATUS="${2:-FAILED}"
ORDER_ID="${3:-UNKNOWN}"
PRIORITY="${4:-3}"

if [[ -z "$JOB_NAME" ]]; then
    echo "Usage: $0 <job_name> <status> <order_id> [priority]"
    exit 1
fi

# ServiceNow configuration
SNOW_INSTANCE="${SNOW_INSTANCE:-}"
SNOW_API_USER="${SNOW_API_USER:-}"
SNOW_API_PASS="${SNOW_API_PASS:-}"

if [[ -z "$SNOW_INSTANCE" ]] || [[ -z "$SNOW_API_USER" ]] || [[ -z "$SNOW_API_PASS" ]]; then
    echo "ERROR: ServiceNow credentials not configured"
    echo "Set SNOW_INSTANCE, SNOW_API_USER, SNOW_API_PASS environment variables"
    exit 1
fi

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
HOSTNAME=$(hostname)

# Map job to assignment group and category
case "$JOB_NAME" in
    RMS_*|CLAIMS_*)
        ASSIGNMENT_GROUP="Data Lake Operations"
        CATEGORY="Application"
        SUBCATEGORY="Batch Processing"
        CI_NAME="MQ-INTAKE-${JOB_NAME%%_*}"
        ;;
    *)
        ASSIGNMENT_GROUP="Data Lake Operations"
        CATEGORY="Application"
        SUBCATEGORY="Other"
        CI_NAME="MQ-INTAKE"
        ;;
esac

# Map priority
case "$PRIORITY" in
    1|critical) PRIORITY_NUM="1"; IMPACT="1"; URGENCY="1" ;;
    2|high)     PRIORITY_NUM="2"; IMPACT="2"; URGENCY="2" ;;
    3|medium)   PRIORITY_NUM="3"; IMPACT="2"; URGENCY="3" ;;
    4|low)      PRIORITY_NUM="4"; IMPACT="3"; URGENCY="3" ;;
    *)          PRIORITY_NUM="3"; IMPACT="2"; URGENCY="3" ;;
esac

# Build description with troubleshooting steps
read -r -d '' DESCRIPTION << EOF || true
Control-M Job Failure Notification

Job Name:    ${JOB_NAME}
Status:      ${STATUS}
Order ID:    ${ORDER_ID}
Host:        ${HOSTNAME}
Timestamp:   ${TIMESTAMP}

Troubleshooting Steps:
1. SSH to ${HOSTNAME}
2. Navigate to the application directory
3. Check logs: ./intake.sh logs | grep -i error | tail -50
4. Check status: ./intake.sh status
5. Run preflight: ./intake.sh preflight
6. If preflight passes, restart: ./intake.sh restart
7. Verify recovery: ./intake.sh status

Escalation:
- If issue persists after restart, check MQ and HDFS connectivity
- Review application logs for root cause
- Contact Data Lake on-call if unable to resolve
EOF

# Build incident payload
INCIDENT_JSON=$(cat <<EOF
{
    "short_description": "Control-M Job Failed: ${JOB_NAME}",
    "description": $(echo "$DESCRIPTION" | jq -Rs .),
    "category": "${CATEGORY}",
    "subcategory": "${SUBCATEGORY}",
    "assignment_group": "${ASSIGNMENT_GROUP}",
    "priority": "${PRIORITY_NUM}",
    "impact": "${IMPACT}",
    "urgency": "${URGENCY}",
    "caller_id": "control-m",
    "cmdb_ci": "${CI_NAME}",
    "u_source": "Control-M",
    "u_job_name": "${JOB_NAME}",
    "u_order_id": "${ORDER_ID}",
    "u_hostname": "${HOSTNAME}"
}
EOF
)

echo "Creating ServiceNow incident..."
echo "Job: $JOB_NAME"
echo "Priority: $PRIORITY_NUM"

# Create incident via ServiceNow API
RESPONSE=$(curl -s -X POST \
    "https://${SNOW_INSTANCE}/api/now/table/incident" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    -u "${SNOW_API_USER}:${SNOW_API_PASS}" \
    --connect-timeout 30 \
    -d "${INCIDENT_JSON}")

# Extract incident number
INC_NUMBER=$(echo "$RESPONSE" | jq -r '.result.number // empty')
INC_SYSID=$(echo "$RESPONSE" | jq -r '.result.sys_id // empty')

if [[ -n "$INC_NUMBER" ]] && [[ "$INC_NUMBER" =~ ^INC[0-9]+$ ]]; then
    echo "SUCCESS: Created incident ${INC_NUMBER}"

    # Store mapping for later resolution
    INCIDENT_LOG="${CTM_LOG_DIR:-/var/log/ctm}/open_incidents.log"
    mkdir -p "$(dirname "$INCIDENT_LOG")"
    echo "${JOB_NAME}|${INC_SYSID}|${INC_NUMBER}|${TIMESTAMP}" >> "$INCIDENT_LOG"

    echo "Incident URL: https://${SNOW_INSTANCE}/incident.do?sys_id=${INC_SYSID}"
    exit 0
else
    echo "ERROR: Failed to create incident"
    echo "Response: $RESPONSE"
    exit 1
fi
