#!/bin/bash
#
# Page On-Call via PagerDuty for Critical Failures
#
# Usage: page_oncall.sh <job_name> <message> [severity]
#   severity: critical | error | warning | info (default: critical)
#
# Environment variables required:
#   PAGERDUTY_ROUTING_KEY - PagerDuty Events API v2 routing key
#
# Exit codes:
#   0 - Page sent successfully
#   1 - Failed to send page

set -euo pipefail

JOB_NAME="${1:-}"
MESSAGE="${2:-}"
SEVERITY="${3:-critical}"

if [[ -z "$JOB_NAME" ]] || [[ -z "$MESSAGE" ]]; then
    echo "Usage: $0 <job_name> <message> [severity]"
    exit 1
fi

PD_ROUTING_KEY="${PAGERDUTY_ROUTING_KEY:-}"

if [[ -z "$PD_ROUTING_KEY" ]]; then
    echo "ERROR: PAGERDUTY_ROUTING_KEY not configured"
    echo "Cannot send page for: $MESSAGE"
    exit 1
fi

TIMESTAMP=$(date -Iseconds)
HOSTNAME=$(hostname)
DEDUP_KEY="ctm-${JOB_NAME}-$(date +%Y%m%d)"

# Map severity
case "$SEVERITY" in
    critical|crit) PD_SEVERITY="critical" ;;
    error|err)     PD_SEVERITY="error" ;;
    warning|warn)  PD_SEVERITY="warning" ;;
    info)          PD_SEVERITY="info" ;;
    *)             PD_SEVERITY="critical" ;;
esac

# Build PagerDuty payload
PAYLOAD=$(cat <<EOF
{
    "routing_key": "${PD_ROUTING_KEY}",
    "event_action": "trigger",
    "dedup_key": "${DEDUP_KEY}",
    "payload": {
        "summary": "${MESSAGE}",
        "source": "Control-M / ${HOSTNAME}",
        "severity": "${PD_SEVERITY}",
        "timestamp": "${TIMESTAMP}",
        "component": "MQ-Intake",
        "group": "Data Lake",
        "class": "batch-job-failure",
        "custom_details": {
            "job_name": "${JOB_NAME}",
            "hostname": "${HOSTNAME}",
            "timestamp": "${TIMESTAMP}",
            "runbook": "https://wiki.company.com/datalake/mq-intake/runbook"
        }
    },
    "links": [
        {
            "href": "https://wiki.company.com/datalake/mq-intake/runbook",
            "text": "Runbook"
        }
    ]
}
EOF
)

echo "Sending PagerDuty alert..."
echo "Job:      $JOB_NAME"
echo "Severity: $PD_SEVERITY"
echo "Message:  $MESSAGE"

RESPONSE=$(curl -s -X POST \
    "https://events.pagerduty.com/v2/enqueue" \
    -H "Content-Type: application/json" \
    --connect-timeout 30 \
    -d "${PAYLOAD}")

STATUS=$(echo "$RESPONSE" | jq -r '.status // empty')
DEDUP=$(echo "$RESPONSE" | jq -r '.dedup_key // empty')

if [[ "$STATUS" == "success" ]]; then
    echo "SUCCESS: PagerDuty alert sent"
    echo "Dedup Key: $DEDUP"
    exit 0
else
    echo "ERROR: Failed to send PagerDuty alert"
    echo "Response: $RESPONSE"
    exit 1
fi
