# Control-M Setup Guide for MQ Intake

This guide explains how to set up Control-M folders and jobs for monitoring and managing the MQ Intake applications (RMS and Claims).

## Table of Contents

- [Overview](#overview)
- [Folder Structure](#folder-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Scripts Reference](#scripts-reference)
- [Job Definitions](#job-definitions)
- [Configuration](#configuration)
- [Notification Setup](#notification-setup)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## Overview

The Control-M setup provides:

| Capability | Description |
|------------|-------------|
| **Operations** | Start, stop, restart, preflight checks |
| **Monitoring** | Health checks, queue depth, backout alerts |
| **Maintenance** | Log cleanup, temp file cleanup |
| **Alerting** | Email, PagerDuty, ServiceNow integration |
| **Auto-remediation** | Automatic restart on failure |

---

## Folder Structure

```
DATALAKE/
└── MQ_INTAKE/
    ├── RMS/
    │   ├── OPERATIONS/
    │   │   ├── RMS_PREFLIGHT
    │   │   ├── RMS_START
    │   │   ├── RMS_STOP
    │   │   └── RMS_RESTART
    │   ├── MONITORING/
    │   │   ├── RMS_HEALTH_CHECK      (cyclic: 5 min)
    │   │   ├── RMS_QUEUE_DEPTH       (cyclic: 5 min)
    │   │   ├── RMS_BACKOUT_ALERT     (cyclic: 15 min)
    │   │   └── RMS_METRICS_COLLECT   (cyclic: 1 hour)
    │   └── MAINTENANCE/
    │       ├── RMS_LOG_CLEANUP       (daily: 02:00)
    │       └── RMS_TMP_CLEANUP       (daily: 03:00)
    │
    └── CLAIMS/
        ├── OPERATIONS/
        │   ├── CLAIMS_PREFLIGHT
        │   ├── CLAIMS_START
        │   ├── CLAIMS_STOP
        │   └── CLAIMS_RESTART
        ├── MONITORING/
        │   ├── CLAIMS_HEALTH_CHECK   (cyclic: 5 min)
        │   ├── CLAIMS_QUEUE_DEPTH    (cyclic: 5 min)
        │   ├── CLAIMS_BACKOUT_ALERT  (cyclic: 15 min)
        │   └── CLAIMS_METRICS_COLLECT (cyclic: 1 hour)
        └── MAINTENANCE/
            ├── CLAIMS_LOG_CLEANUP    (daily: 02:00)
            └── CLAIMS_TMP_CLEANUP    (daily: 03:00)
```

---

## Prerequisites

### 1. Control-M Agent

Ensure the Control-M agent is installed and running on the application hosts.

### 2. Script Dependencies

The scripts require:
- `bash` 4.0+
- `curl`
- `jq`
- `hdfs` CLI (for HDFS operations)

Install jq if not present:
```bash
# RHEL/CentOS
yum install jq

# Ubuntu/Debian
apt-get install jq
```

### 3. Credentials

Set up the following in your secure vault or environment:

| Variable | Description |
|----------|-------------|
| `SNOW_API_USER` | ServiceNow API username |
| `SNOW_API_PASS` | ServiceNow API password |
| `PAGERDUTY_ROUTING_KEY` | PagerDuty Events API routing key |

---

## Installation

### Step 1: Copy Scripts to Control-M Server

```bash
# Create scripts directory
mkdir -p /opt/ctm/scripts/mq-intake

# Copy scripts from repository
cp scripts/controlm/*.sh /opt/ctm/scripts/mq-intake/

# Make executable
chmod +x /opt/ctm/scripts/mq-intake/*.sh

# Create log directory
mkdir -p /var/log/ctm
chmod 755 /var/log/ctm
```

### Step 2: Configure Parameters

Edit the configuration file:

```bash
vi /opt/ctm/scripts/mq-intake/config/job_parameters.conf
```

Update these values for your environment:

```bash
# Application paths
APP_BASE_RMS=/path/to/mq-intake-rms
APP_BASE_CLAIMS=/path/to/mq-intake-claims

# Ports
SERVER_PORT_RMS=8081
SERVER_PORT_CLAIMS=8099

# HDFS paths
HDFS_BASE_RMS=/data/raw/membership/hps
HDFS_BASE_CLAIMS=/data/raw/claims/dmih

# Notification recipients
EMAIL_OPS=your-ops-team@company.com
EMAIL_MGMT=your-mgmt-team@company.com

# ServiceNow instance
SNOW_INSTANCE=your-instance.service-now.com
```

See: [config/job_parameters.conf](../scripts/controlm/config/job_parameters.conf)

### Step 3: Import Job Definitions

Import the job XML files into Control-M:

```bash
# Using Control-M CLI
ctm deploy jobs/rms_jobs.xml
ctm deploy jobs/claims_jobs.xml
```

Or import via Control-M Web Interface:
1. Navigate to **Planning** → **Import**
2. Select the XML file
3. Review and confirm job definitions
4. Deploy to production

See:
- [jobs/rms_jobs.xml](../scripts/controlm/jobs/rms_jobs.xml)
- [jobs/claims_jobs.xml](../scripts/controlm/jobs/claims_jobs.xml)

### Step 4: Configure Variables in Control-M

Set these variables in Control-M Configuration Manager:

| Variable | Example Value |
|----------|---------------|
| `%%NODEID` | `twauslwsapp314` |
| `%%RUN_AS` | `hdwas01` |
| `%%APP_BASE_RMS` | `/home/hdwas01/.../mq-intake-rms` |
| `%%APP_BASE_CLAIMS` | `/home/hdwas01/.../mq-intake-claims` |
| `%%SERVER_PORT_RMS` | `8081` |
| `%%SERVER_PORT_CLAIMS` | `8099` |
| `%%CTM_SCRIPTS` | `/opt/ctm/scripts/mq-intake` |
| `%%CTM_LOG_DIR` | `/var/log/ctm` |
| `%%EMAIL_OPS` | `datalake-ops@company.com` |
| `%%EMAIL_MGMT` | `datalake-mgmt@company.com` |
| `%%QUEUE_DEPTH_WARN` | `10000` |
| `%%QUEUE_DEPTH_CRIT` | `50000` |
| `%%LOG_RETENTION_DAYS` | `30` |
| `%%TMP_MAX_AGE_HOURS` | `2` |
| `%%HDFS_BASE_RMS` | `/data/raw/membership/hps` |
| `%%HDFS_BASE_CLAIMS` | `/data/raw/claims/dmih` |

---

## Scripts Reference

All scripts are located in: [scripts/controlm/](../scripts/controlm/)

### Monitoring Scripts

| Script | Purpose | Exit Codes |
|--------|---------|------------|
| [health_check.sh](../scripts/controlm/health_check.sh) | Check service health via actuator | 0=UP, 1=DEGRADED, 2=DOWN, 3=UNKNOWN |
| [queue_depth_check.sh](../scripts/controlm/queue_depth_check.sh) | Monitor source queue backlog | 0=OK, 1=WARNING, 2=CRITICAL |
| [backout_alert.sh](../scripts/controlm/backout_alert.sh) | Alert on poison messages | 0=EMPTY, 2=MESSAGES_FOUND |
| [metrics_collect.sh](../scripts/controlm/metrics_collect.sh) | Collect metrics for trending | 0=SUCCESS, 1=FAILED |

### Incident Management Scripts

| Script | Purpose |
|--------|---------|
| [create_incident.sh](../scripts/controlm/create_incident.sh) | Create ServiceNow incident |
| [resolve_incident.sh](../scripts/controlm/resolve_incident.sh) | Auto-resolve incident on success |
| [page_oncall.sh](../scripts/controlm/page_oncall.sh) | Send PagerDuty alert |

### Maintenance Scripts

| Script | Purpose |
|--------|---------|
| [log_cleanup.sh](../scripts/controlm/log_cleanup.sh) | Remove old log files |
| [tmp_cleanup.sh](../scripts/controlm/tmp_cleanup.sh) | Check for stale HDFS temp files |

### Usage Examples

```bash
# Health check
./health_check.sh rms 8081

# Queue depth with custom thresholds
./queue_depth_check.sh claims 8099 5000 20000

# Create incident
./create_incident.sh "RMS_HEALTH_CHECK" "FAILED" "ORDER123" "high"

# Page on-call
./page_oncall.sh "RMS_DOWN" "RMS service is down" "critical"

# Log cleanup (30 day retention)
./log_cleanup.sh /home/hdwas01/mq-intake-rms 30

# Metrics collection to CSV
./metrics_collect.sh rms 8081 /var/log/ctm/rms_metrics.csv
```

---

## Job Definitions

### Operations Jobs

| Job | Trigger | Dependencies | Actions on Failure |
|-----|---------|--------------|-------------------|
| `*_PREFLIGHT` | On-demand | None | Email ops |
| `*_START` | On-demand | PREFLIGHT success | Email + Incident |
| `*_STOP` | On-demand | None | — |
| `*_RESTART` | On-demand or auto | None | Page on-call |

### Monitoring Jobs

| Job | Schedule | Alert Escalation |
|-----|----------|------------------|
| `*_HEALTH_CHECK` | Every 5 min | 1st fail: Email + Restart<br>2nd fail: Incident<br>3rd fail: Page |
| `*_QUEUE_DEPTH` | Every 5 min | Warning/Critical email |
| `*_BACKOUT_ALERT` | Every 15 min | Page + Incident |
| `*_METRICS_COLLECT` | Every 1 hour | — |

### Maintenance Jobs

| Job | Schedule | Alert |
|-----|----------|-------|
| `*_LOG_CLEANUP` | Daily 02:00 | — |
| `*_TMP_CLEANUP` | Daily 03:00 | Email if stale files found |

---

## Configuration

### Alert Thresholds

Edit thresholds in [config/job_parameters.conf](../scripts/controlm/config/job_parameters.conf):

```bash
# Queue depth
QUEUE_DEPTH_WARN=10000   # Warning threshold
QUEUE_DEPTH_CRIT=50000   # Critical threshold

# Retention
LOG_RETENTION_DAYS=30    # Log files older than this are deleted
TMP_MAX_AGE_HOURS=2      # Temp files older than this trigger alert
```

### Notification Recipients

```bash
EMAIL_OPS=datalake-ops@company.com      # Operations team
EMAIL_MGMT=datalake-mgmt@company.com    # Management (escalation)
```

### ServiceNow Integration

Set these environment variables (in vault or secure store):

```bash
export SNOW_INSTANCE=company.service-now.com
export SNOW_API_USER=ctm-integration
export SNOW_API_PASS=<secure-password>
```

### PagerDuty Integration

```bash
export PAGERDUTY_ROUTING_KEY=<your-routing-key>
```

---

## Notification Setup

### Email (Control-M Shout)

Configure in Control-M Configuration Manager → Shout Destinations:

| Name | Type | Address |
|------|------|---------|
| `EMAIL_OPS` | Email | datalake-ops@company.com |
| `EMAIL_MGMT` | Email | datalake-mgmt@company.com |

### ServiceNow Incident Creation

The [create_incident.sh](../scripts/controlm/create_incident.sh) script:
1. Maps job to assignment group and priority
2. Creates incident via ServiceNow REST API
3. Logs incident ID for later resolution
4. Includes troubleshooting steps in description

### PagerDuty Alerts

The [page_oncall.sh](../scripts/controlm/page_oncall.sh) script:
1. Sends event to PagerDuty Events API v2
2. Uses dedup key to prevent duplicate pages
3. Includes runbook link

### Auto-Resolution

The [resolve_incident.sh](../scripts/controlm/resolve_incident.sh) script:
1. Finds open incident for the job
2. Resolves it via ServiceNow API
3. Removes from tracking log

---

## Testing

### 1. Test Scripts Locally

```bash
# Test health check
./health_check.sh rms 8081
echo "Exit code: $?"

# Test with service stopped
./intake.sh stop
./health_check.sh rms 8081
echo "Exit code: $?"  # Should be 2 (DOWN)
```

### 2. Test Incident Creation (Dry Run)

```bash
# Verify ServiceNow connectivity
curl -s -u "${SNOW_API_USER}:${SNOW_API_PASS}" \
  "https://${SNOW_INSTANCE}/api/now/table/incident?sysparm_limit=1"
```

### 3. Test PagerDuty (Dry Run)

```bash
# Send test event
./page_oncall.sh "TEST_JOB" "Test alert - please ignore" "info"
```

### 4. Run Jobs in Control-M Test Mode

1. Order jobs with `ODAT=TEST`
2. Verify execution and notifications
3. Check incident creation/resolution

---

## Troubleshooting

### Job Fails to Execute

```bash
# Check script exists and is executable
ls -la /opt/ctm/scripts/mq-intake/

# Test script manually
su - hdwas01
/opt/ctm/scripts/mq-intake/health_check.sh rms 8081
```

### Health Check Returns UNKNOWN

```bash
# Verify service is running
./intake.sh status

# Check actuator endpoint
curl -s http://localhost:8081/actuator/health

# Check port
netstat -tlnp | grep 8081
```

### Incident Not Created

```bash
# Check ServiceNow credentials
echo "SNOW_INSTANCE=$SNOW_INSTANCE"
echo "SNOW_API_USER=$SNOW_API_USER"

# Test API connectivity
curl -s -u "${SNOW_API_USER}:${SNOW_API_PASS}" \
  "https://${SNOW_INSTANCE}/api/now/table/incident?sysparm_limit=1"

# Check incident log
cat /var/log/ctm/open_incidents.log
```

### PagerDuty Alert Not Received

```bash
# Verify routing key
echo "PAGERDUTY_ROUTING_KEY is set: ${PAGERDUTY_ROUTING_KEY:+yes}"

# Test with curl
curl -X POST "https://events.pagerduty.com/v2/enqueue" \
  -H "Content-Type: application/json" \
  -d '{"routing_key":"'$PAGERDUTY_ROUTING_KEY'","event_action":"trigger","payload":{"summary":"Test","source":"test","severity":"info"}}'
```

---

## Files Reference

| File | Description |
|------|-------------|
| [scripts/controlm/health_check.sh](../scripts/controlm/health_check.sh) | Health check script |
| [scripts/controlm/queue_depth_check.sh](../scripts/controlm/queue_depth_check.sh) | Queue depth monitoring |
| [scripts/controlm/backout_alert.sh](../scripts/controlm/backout_alert.sh) | Backout queue alert |
| [scripts/controlm/create_incident.sh](../scripts/controlm/create_incident.sh) | ServiceNow incident creation |
| [scripts/controlm/resolve_incident.sh](../scripts/controlm/resolve_incident.sh) | ServiceNow incident resolution |
| [scripts/controlm/page_oncall.sh](../scripts/controlm/page_oncall.sh) | PagerDuty alerting |
| [scripts/controlm/log_cleanup.sh](../scripts/controlm/log_cleanup.sh) | Log file cleanup |
| [scripts/controlm/tmp_cleanup.sh](../scripts/controlm/tmp_cleanup.sh) | HDFS temp file check |
| [scripts/controlm/metrics_collect.sh](../scripts/controlm/metrics_collect.sh) | Metrics collection |
| [scripts/controlm/config/job_parameters.conf](../scripts/controlm/config/job_parameters.conf) | Parameter configuration |
| [scripts/controlm/jobs/rms_jobs.xml](../scripts/controlm/jobs/rms_jobs.xml) | RMS job definitions |
| [scripts/controlm/jobs/claims_jobs.xml](../scripts/controlm/jobs/claims_jobs.xml) | Claims job definitions |

---

## Support

- **Operations Team**: datalake-ops@company.com
- **On-Call**: Via PagerDuty
- **Runbook**: https://wiki.company.com/datalake/mq-intake/runbook
