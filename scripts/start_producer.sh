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
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
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

# Install dependencies if needed
if [ ! -d "venv" ] && [ -f "requirements.txt" ]; then
    echo "Installing dependencies..."
    pip3 install -r requirements.txt
fi

# Set environment variable
export KAFKA_BOOTSTRAP_SERVERS

echo "Starting producer..."
echo "Kafka servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "Topic: input_events"
echo "Press Ctrl+C to stop"
echo ""

# Run producer
python3 producer.py
