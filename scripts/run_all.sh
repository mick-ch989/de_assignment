#!/bin/bash
# Script to run all services in the streaming pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Starting Streaming Pipeline Services"
echo "=========================================="
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    exit 1
fi

# Navigate to project root
cd "$PROJECT_ROOT"

# Start infrastructure services
echo "Starting infrastructure services (Zookeeper, Kafka)..."
docker-compose up -d zookeeper kafka

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Start Spark cluster
echo "Starting Spark cluster..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2

# Wait for Spark to be ready
echo "Waiting for Spark to be ready..."
sleep 15

# Start monitoring services
echo "Starting monitoring services..."
docker-compose up -d kafka-exporter spark-master-exporter spark-worker-1-exporter spark-worker-2-exporter prometheus grafana

# Wait for monitoring to be ready
echo "Waiting for monitoring services..."
sleep 5

# Start streaming job
echo "Starting Spark streaming job..."
docker-compose up -d spark-streaming

echo ""
echo "=========================================="
echo "All services started!"
echo "=========================================="
echo ""
echo "Service URLs:"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Prometheus: http://localhost:9090"
echo "  - Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f spark-streaming"
echo ""
echo "To stop all services:"
echo "  ./scripts/kill_all.sh"
echo ""
