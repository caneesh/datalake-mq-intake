#!/bin/bash
set -e

echo "=== MQ Intake Docker Integration Test ==="
echo ""

# Check Docker is available
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: docker-compose is not installed"
    exit 1
fi

# Start MQ if not running
if ! docker ps | grep -q mq-intake-ibmmq; then
    echo "Starting IBM MQ container..."
    docker-compose up -d ibm-mq

    echo "Waiting for MQ to be ready..."
    for i in {1..60}; do
        if docker exec mq-intake-ibmmq dspmq -m QM1 2>/dev/null | grep -q "Running"; then
            echo "MQ is ready!"
            break
        fi
        if [ $i -eq 60 ]; then
            echo "ERROR: MQ did not start in time"
            docker-compose logs ibm-mq
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

# Run the tests
mvn test -pl rms -Dtest=IbmMqIntegrationTest -DfailIfNoTests=false

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
echo "To stop: docker-compose down"
