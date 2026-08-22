# Docker Testing Setup

This directory contains configuration for testing with IBM MQ in Docker.

## Prerequisites

- Docker and Docker Compose installed
- At least 2GB RAM available for IBM MQ container

## Quick Start

1. **Start IBM MQ:**
   ```bash
   docker-compose up -d ibm-mq
   ```

2. **Wait for MQ to be ready** (takes ~30-60 seconds):
   ```bash
   docker-compose logs -f ibm-mq
   # Wait until you see "AMQ5975I: The queue manager 'QM1' is running"
   ```

3. **Verify queues are created:**
   ```bash
   docker exec -it mq-intake-ibmmq runmqsc QM1 <<< "DISPLAY QLOCAL(*)"
   ```

4. **Set credentials for testing:**
   ```bash
   export MQ_USER=app
   export MQ_PASSWORD=passw0rd
   ```

5. **Run integration tests:**
   ```bash
   # From project root
   mvn test -pl rms -Dspring.profiles.active=docker -Dit.test=*IntegrationTest
   ```

## MQ Console

Access the IBM MQ web console at: https://localhost:9443/ibmmq/console/

- Username: `admin`
- Password: `passw0rd`

## Sending Test Messages

Using the MQ console or command line:

```bash
# Send a test message to RMS queue
docker exec -it mq-intake-ibmmq /opt/mqm/samp/bin/amqsput MQ.HPS.MEMBERSHIP.IN QM1 <<< "<TestMessage><MessageID>test-123</MessageID></TestMessage>"

# Check messages on queue
docker exec -it mq-intake-ibmmq /opt/mqm/samp/bin/amqsbcg MQ.HPS.MEMBERSHIP.IN QM1
```

## Queue Configuration

The following queues are auto-created:

| Queue | Purpose |
|-------|---------|
| MQ.HPS.MEMBERSHIP.IN | RMS source queue |
| MQ.HPS.MEMBERSHIP.TRACKER | RMS tracker queue |
| MQ.HPS.MEMBERSHIP.BACKOUT | RMS backout queue |
| MQ.DMIH.CLAIMS.IN | Claims source queue |
| MQ.DMIH.CLAIMS.BACKOUT | Claims backout queue |

## Cleanup

```bash
docker-compose down -v
```

## Troubleshooting

**Container won't start:**
```bash
docker-compose logs ibm-mq
```

**Connection refused:**
- Ensure container is running: `docker-compose ps`
- Check port binding: `docker port mq-intake-ibmmq`
- Verify firewall allows port 1414

**Authentication errors:**
- Verify MQ_USER and MQ_PASSWORD env vars are set
- Check channel auth: `docker exec -it mq-intake-ibmmq runmqsc QM1 <<< "DISPLAY CHLAUTH(*) ALL"`
