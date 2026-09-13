# Production Deployment Checklist

Pre-deployment checklist for MQ Intake service (RMS and Claims).

---

## 1. Code & Build

| Item | Status | Notes |
|------|--------|-------|
| All tests passing | ☐ | `mvn clean test` |
| Build successful with Java 11 | ☐ | Jenkins must use JDK 11 |
| Numeric partition paths | ☐ | Matches MDB format: `/2026/09/13/10/2` |
| No .seq extension | ☐ | Matches MDB format |
| Record index disabled | ☐ | No .index.jsonl files |
| Log rolling configured | ☐ | 30 days, 5GB cap |

---

## 2. Configuration

### RMS (`env.sh`)

| Variable | Value | Verified |
|----------|-------|----------|
| `JAVA_HOME` | Path to JRE 11 | ☐ |
| `SERVER_PORT` | 8081 | ☐ |
| `IBM_MQ_QUEUEMANAGER` | Production QM name | ☐ |
| `IBM_MQ_CHANNEL` | Production channel | ☐ |
| `IBM_MQ_CONNNAME` | Production MQ host(port) | ☐ |
| `IBM_MQ_USER` | Service account | ☐ |
| `HDFS_BASE_PATH` | `/raw/membership/hps` | ☐ |
| `MQ_TRACKER_QUEUE` | Tracker queue name | ☐ |
| `MQ_BACKOUT_QUEUE` | Backout queue name | ☐ |

### Claims (`env.sh`)

| Variable | Value | Verified |
|----------|-------|----------|
| `JAVA_HOME` | Path to JRE 11 | ☐ |
| `SERVER_PORT` | 8099 | ☐ |
| `IBM_MQ_QUEUEMANAGER` | Production QM name | ☐ |
| `IBM_MQ_CHANNEL` | Production channel | ☐ |
| `IBM_MQ_CONNNAME` | Production MQ host(port) | ☐ |
| `IBM_MQ_USER` | Service account | ☐ |
| `HDFS_BASE_PATH` | `/raw/claims/dmih` | ☐ |

---

## 3. Infrastructure

### MQ Channel Settings

| Setting | Required | Current | Verified |
|---------|----------|---------|----------|
| `SHARECNV` | > 1 | | ☐ |
| `MAXINST` | 20+ | | ☐ |
| `MAXINSTC` | 20+ | | ☐ |
| `HBINT` | 60 | | ☐ |

### Kerberos

| Item | Status |
|------|--------|
| Service principal created | ☐ |
| Keytab deployed to servers | ☐ |
| Keytab path in env.sh | ☐ |
| Test: `kinit` successful | ☐ |

### HDFS

| Item | Status |
|------|--------|
| Base paths exist and writable | ☐ |
| `_tmp/` directories writable | ☐ |
| `_audit/` directories writable | ☐ |
| Test: preflight passes | ☐ |

### Network

| Source | Destination | Port | Status |
|--------|-------------|------|--------|
| App server | MQ server | 1414 | ☐ |
| App server | HDFS NameNode | 8020 | ☐ |
| App server | HDFS DataNodes | 50010 | ☐ |

---

## 4. Directory Structure

```
/path/to/mq-intake-{rms|claims}/
├── releases/
│   └── 1.0.0/
│       ├── app.jar
│       └── intake.sh
├── current -> releases/1.0.0    # MUST be symlink
├── env.sh
├── logs/
└── run/
```

| Item | RMS | Claims |
|------|-----|--------|
| `releases/` directory exists | ☐ | ☐ |
| `current` is a symlink | ☐ | ☐ |
| `env.sh` configured | ☐ | ☐ |
| `logs/` directory exists | ☐ | ☐ |
| `run/` directory exists | ☐ | ☐ |

---

## 5. Pre-Flight Tests

Run on each server before going live:

```bash
cd /path/to/mq-intake-{rms|claims}/current
./intake.sh preflight
```

| Check | RMS | Claims |
|-------|-----|--------|
| MQ connectivity | ☐ | ☐ |
| HDFS connectivity | ☐ | ☐ |
| Kerberos authentication | ☐ | ☐ |
| Source queue readable | ☐ | ☐ |
| Backout queue accessible | ☐ | ☐ |
| Tracker queue writable (RMS) | ☐ | N/A |

---

## 6. Monitoring & Alerting

### Control-M Jobs

| Job | RMS | Claims |
|-----|-----|--------|
| Health check (5 min) | ☐ | ☐ |
| Queue depth monitor (5 min) | ☐ | ☐ |
| Backout alert (15 min) | ☐ | ☐ |
| Log cleanup (daily) | ☐ | ☐ |
| Metrics collection (hourly) | ☐ | ☐ |

### Notification Setup

| Item | Status |
|------|--------|
| Email distribution list configured | ☐ |
| PagerDuty routing key set | ☐ |
| ServiceNow integration tested | ☐ |

### Health Endpoints

| Endpoint | RMS | Claims |
|----------|-----|--------|
| `/actuator/health` | ☐ | ☐ |
| `/actuator/metrics` | ☐ | ☐ |

---

## 7. Migration Planning

### Cutover Strategy

| Phase | Description | Duration |
|-------|-------------|----------|
| 1. Parallel run | Both MDB and new service consuming | 1-2 weeks |
| 2. Traffic shift | Reduce MDB threads, increase new service | 1 week |
| 3. MDB stop | Stop MDB, new service only | Go-live |
| 4. MDB decommission | Remove MDB from infrastructure | Post go-live |

### Rollback Plan

| Trigger | Action |
|---------|--------|
| New service unhealthy > 10 min | Restart new service |
| New service unhealthy > 30 min | Stop new service, start MDB |
| Data loss detected | Stop new service, investigate, start MDB |

### File Identification

| Application | Filename Pattern |
|-------------|------------------|
| New Service | `{binding}_{hostname-pid}_{epoch}_{seq}` |
| Legacy MDB | `messages_{num}_{ip}_{uuid}_{seq}_{ts}` |

---

## 8. Documentation

| Document | Location | Status |
|----------|----------|--------|
| Deployment guide | docs/DEPLOYMENT.md | ☐ |
| Control-M setup | docs/CONTROLM_SETUP.md | ☐ |
| Runbook | Wiki | ☐ |
| Architecture diagram | Wiki | ☐ |

---

## 9. Approvals

| Approval | Owner | Date |
|----------|-------|------|
| Code review complete | | |
| QA sign-off | | |
| MQ team sign-off | | |
| HDFS/Hadoop team sign-off | | |
| Operations team sign-off | | |
| Change management approved | | |

---

## 10. Go-Live Checklist

### Day Before

| Item | Status |
|------|--------|
| Final build deployed to production servers | ☐ |
| env.sh configured with production values | ☐ |
| Preflight passes on all servers | ☐ |
| Control-M jobs imported and tested | ☐ |
| Rollback plan documented and communicated | ☐ |
| On-call schedule confirmed | ☐ |

### Go-Live Day

| Step | Time | Status |
|------|------|--------|
| 1. Notify stakeholders | T-30 min | ☐ |
| 2. Verify MDB running (baseline) | T-15 min | ☐ |
| 3. Start new service | T-0 | ☐ |
| 4. Verify health endpoint UP | T+2 min | ☐ |
| 5. Verify messages flowing | T+5 min | ☐ |
| 6. Verify HDFS files landing | T+15 min | ☐ |
| 7. Monitor for 1 hour | T+60 min | ☐ |
| 8. Send go-live confirmation | T+60 min | ☐ |

### Post Go-Live

| Item | Status |
|------|--------|
| Monitor for 24 hours | ☐ |
| Compare file counts: new service vs MDB | ☐ |
| Verify downstream consumers working | ☐ |
| Document any issues encountered | ☐ |
| Schedule MDB phase-out | ☐ |

---

## Open Items

| Item | Owner | Due Date | Status |
|------|-------|----------|--------|
| MQ channel capacity increase | MQ Team | | ☐ |
| Claims identity field decision | Dev Team | | ☐ |
| Backout queue messages investigation | Ops Team | | ☐ |

---

## Notes

- Both RMS and Claims can be deployed independently
- Start with RMS (lower volume) to validate, then Claims
- Keep MDB running during parallel period for safety
- Monitor backout queue depth — high numbers indicate poison messages

