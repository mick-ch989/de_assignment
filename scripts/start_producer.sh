#!/bin/bash
# Script to start the Kafka producer

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Starting Kafka Producer"
echo "=========================================="
echo ""

# Configuration
# Try to detect if Kafka is in Docker and use appropriate address
if docker ps --format '{{.Names}}' | grep -q "^kafka$"; then
    # Check if port is accessible on localhost
    if nc -z localhost 9092 2>/dev/null; then
        KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
    else
        echo "Warning: Kafka is running in Docker but port 9092 is not accessible on localhost"
        echo "Make sure Kafka port is exposed in docker-compose.yml"
        KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
    fi
else
    KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
fi

PRODUCER_DIR="$PROJECT_ROOT/ingestion/producer"

# Check if Kafka is running
if ! nc -z localhost 9092 2>/dev/null; then
    echo "Warning: Kafka does not appear to be running on localhost:9092"
    echo "Make sure Kafka is started first:"
    echo "  docker-compose up -d kafka"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed"
    exit 1
fi

# Check if dependencies are installed
if [ ! -d "$PRODUCER_DIR" ]; then
    echo "Error: Producer directory not found: $PRODUCER_DIR"
    exit 1
fi

cd "$PRODUCER_DIR"

# Check if root venv exists and use it, otherwise create local venv
if [ -d "$PROJECT_ROOT/venv" ]; then
    echo "Using project virtual environment..."
    PYTHON_CMD="$PROJECT_ROOT/venv/bin/python"
    PIP_CMD="$PROJECT_ROOT/venv/bin/pip"
elif [ -d "venv" ]; then
    echo "Using local virtual environment..."
    PYTHON_CMD="./venv/bin/python"
    PIP_CMD="./venv/bin/pip"
else
    echo "Creating local virtual environment..."
    python3 -m venv venv
    PYTHON_CMD="./venv/bin/python"
    PIP_CMD="./venv/bin/pip"
fi

# Install dependencies if needed
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies..."
    $PIP_CMD install --progress-bar on -r requirements.txt
fi

# Set environment variable
export KAFKA_BOOTSTRAP_SERVERS

echo "Starting producer..."
echo "Kafka servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "Topic: input_events"
echo "Press Ctrl+C to stop"
echo ""

# Run producer
$PYTHON_CMD producer.py
