#!/bin/bash
# Script to start the Spark streaming job

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Starting Spark Streaming Job"
echo "=========================================="
echo ""

# Configuration
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-input_events}"
CHECKPOINT_LOCATION="${CHECKPOINT_LOCATION:-/tmp/streaming_checkpoint}"
OUTPUT_PATH="${OUTPUT_PATH:-/tmp/streaming_output}"
S3_BUCKET="${S3_BUCKET:-}"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    exit 1
fi

# Navigate to project root
cd "$PROJECT_ROOT"

# Check if Spark cluster is running
if ! docker ps | grep -q spark-master; then
    echo "Warning: Spark cluster does not appear to be running"
    echo "Starting Spark cluster..."
    docker-compose up -d spark-master spark-worker-1 spark-worker-2
    echo "Waiting for Spark to be ready..."
    sleep 15
fi

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    echo "Warning: Kafka does not appear to be running"
    echo "Starting Kafka..."
    docker-compose up -d zookeeper kafka
    echo "Waiting for Kafka to be ready..."
    sleep 10
fi

# Build streaming job if needed
echo "Building/updating streaming job image..."
docker-compose build spark-streaming

# Set environment variables for the container
export KAFKA_BOOTSTRAP_SERVERS
export KAFKA_TOPIC
export CHECKPOINT_LOCATION
export OUTPUT_PATH
export S3_BUCKET

# Start streaming job
echo "Starting Spark streaming job..."
echo "Kafka servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "Kafka topic: $KAFKA_TOPIC"
if [ -n "$S3_BUCKET" ]; then
    echo "S3 bucket: $S3_BUCKET"
fi
echo ""

docker-compose up -d spark-streaming

echo "Streaming job started!"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f spark-streaming"
echo ""
echo "To check Spark UI:"
echo "  http://localhost:8080"
echo ""
