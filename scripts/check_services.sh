#!/bin/bash
# Script to check status of all services

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Service Status Check"
echo "=========================================="
echo ""

cd "$PROJECT_ROOT"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    exit 1
fi

# Check Docker services
echo "Docker Services:"
echo "---------------"
docker-compose ps

echo ""
echo "Service URLs:"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Prometheus: http://localhost:9090"
echo "  - Grafana: http://localhost:3000"
echo ""

# Check if services are accessible
echo "Connectivity Check:"
echo "------------------"

# Check Kafka
if nc -z localhost 9092 2>/dev/null; then
    echo "✓ Kafka (localhost:9092) - accessible"
else
    echo "✗ Kafka (localhost:9092) - not accessible"
fi

# Check Spark Master
if nc -z localhost 8080 2>/dev/null; then
    echo "✓ Spark Master UI (localhost:8080) - accessible"
else
    echo "✗ Spark Master UI (localhost:8080) - not accessible"
fi

# Check Prometheus
if nc -z localhost 9090 2>/dev/null; then
    echo "✓ Prometheus (localhost:9090) - accessible"
else
    echo "✗ Prometheus (localhost:9090) - not accessible"
fi

# Check Grafana
if nc -z localhost 3000 2>/dev/null; then
    echo "✓ Grafana (localhost:3000) - accessible"
else
    echo "✗ Grafana (localhost:3000) - not accessible"
fi

echo ""

