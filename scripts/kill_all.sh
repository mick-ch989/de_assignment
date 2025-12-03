#!/bin/bash
# Script to stop all services in the streaming pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Stopping All Streaming Pipeline Services"
echo "=========================================="
echo ""

# Navigate to project root
cd "$PROJECT_ROOT"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    exit 1
fi

# Stop all services
echo "Stopping all services..."
docker-compose down

echo ""
echo "=========================================="
echo "All services stopped!"
echo "=========================================="
echo ""

# Optional: Remove volumes (uncomment if needed)
# echo "Removing volumes..."
# docker-compose down -v
