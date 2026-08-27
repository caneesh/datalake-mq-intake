#!/bin/bash
set -e

echo "=== MQ Intake Docker Integration Test ==="
echo ""

# Check Docker is available
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed"
    exit 1
fi

# docker-compose v1 or the compose plugin, whichever this machine has
if command -v docker-compose &> /dev/null; then
    COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    COMPOSE="docker compose"
else
    echo "ERROR: neither docker-compose nor the docker compose plugin is installed"
    exit 1
fi

# Start MQ if not running
if ! docker ps | grep -q mq-intake-ibmmq; then
    echo "Starting IBM MQ container..."
    $COMPOSE up -d ibm-mq

    echo "Waiting for MQ to be ready..."
    for i in {1..60}; do
        if docker exec mq-intake-ibmmq dspmq -m QM1 2>/dev/null | grep -q "Running"; then
            echo "MQ is ready!"
            break
        fi
        if [ $i -eq 60 ]; then
            echo "ERROR: MQ did not start in time"
            $COMPOSE logs ibm-mq
            exit 1
        fi
        echo -n "."
        sleep 2
    done
    echo ""

    # Give queues time to be created
    sleep 5
else
    echo "IBM MQ container already running"
fi

# Set credentials
export MQ_USER=app
export MQ_PASSWORD=passw0rd

echo ""
echo "Running integration tests..."
echo ""

# The full real-MQ suite, not just basic connectivity: the failure tests
# (channel outage, queue-manager restart, poison isolation) are the ones this
# environment exists for. The demo shows the basic MQ -> HDFS path readably.
mvn test -pl rms \
    -Dtest='IbmMqIntegrationTest,IbmMqFailureIntegrationTest,BasicMqToHdfsDemoTest' \
    -DfailIfNoTests=false

echo ""
echo "=== Test Complete ==="
echo ""
echo "To send test messages:"
echo "  docker exec -it mq-intake-ibmmq /opt/mqm/samp/bin/amqsput MQ.HPS.MEMBERSHIP.IN QM1"
echo ""
echo "To browse queue:"
echo "  docker exec -it mq-intake-ibmmq /opt/mqm/samp/bin/amqsbcg MQ.HPS.MEMBERSHIP.IN QM1"
echo ""
echo "MQ Console: https://localhost:9443/ibmmq/console/ (admin/passw0rd)"
echo ""
echo "To stop: $COMPOSE down"
