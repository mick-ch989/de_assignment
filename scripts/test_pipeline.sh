#!/bin/bash
# Script to test the complete pipeline: producer -> Kafka -> Spark -> MinIO

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Testing Complete Pipeline${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

cd "$PROJECT_ROOT"

# Check if services are running
echo -e "${YELLOW}1. Checking services...${NC}"
if ! docker ps | grep -q "kafka.*Up"; then
    echo -e "${RED}✗ Kafka is not running${NC}"
    echo "Run: make start"
    exit 1
fi

if ! docker ps | grep -q "spark-streaming.*Up"; then
    echo -e "${RED}✗ Spark streaming is not running${NC}"
    echo "Run: make start"
    exit 1
fi

if ! docker ps | grep -q "minio.*Up"; then
    echo -e "${RED}✗ MinIO is not running${NC}"
    echo "Run: make start"
    exit 1
fi

echo -e "${GREEN}✓ All services are running${NC}"
echo ""

# Check initial state
echo -e "${YELLOW}2. Checking initial state...${NC}"
INITIAL_COUNT=$(docker run --rm --network assignment_default \
  -e "MC_HOST_myMinIO=http://minioadmin:minioadmin@minio:9000" \
  minio/mc ls -r myMinIO/streaming-pipeline-output/streaming-output/ 2>/dev/null | grep -c "\.parquet" || echo "0")

echo "Initial Parquet files: $INITIAL_COUNT"
echo ""

# Start producer in background
echo -e "${YELLOW}3. Starting producer (will run for 2 minutes)...${NC}"
cd "$PROJECT_ROOT"
if [ -f "venv/bin/python" ]; then
    venv/bin/python ingestion/producer/producer.py &
    PRODUCER_PID=$!
else
    python3 ingestion/producer/producer.py &
    PRODUCER_PID=$!
fi

echo "Producer PID: $PRODUCER_PID"
echo ""

# Wait for data to be processed
echo -e "${YELLOW}4. Waiting for Spark to process data (2 minutes)...${NC}"
sleep 120

# Stop producer
echo -e "${YELLOW}5. Stopping producer...${NC}"
kill $PRODUCER_PID 2>/dev/null || true
wait $PRODUCER_PID 2>/dev/null || true
echo -e "${GREEN}✓ Producer stopped${NC}"
echo ""

# Wait a bit more for Spark to finish processing
echo -e "${YELLOW}6. Waiting for Spark to finish writing (30 seconds)...${NC}"
sleep 30

# Check final state
echo -e "${YELLOW}7. Checking results...${NC}"
FINAL_COUNT=$(docker run --rm --network assignment_default \
  -e "MC_HOST_myMinIO=http://minioadmin:minioadmin@minio:9000" \
  minio/mc ls -r myMinIO/streaming-pipeline-output/streaming-output/ 2>/dev/null | grep -c "\.parquet" || echo "0")

echo "Final Parquet files: $FINAL_COUNT"
echo ""

if [ "$FINAL_COUNT" -gt "$INITIAL_COUNT" ]; then
    echo -e "${GREEN}✓ SUCCESS: Pipeline is working!${NC}"
    echo "New files created: $((FINAL_COUNT - INITIAL_COUNT))"
else
    echo -e "${RED}✗ FAILED: No new files created${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  - Check Spark logs: make logs-streaming"
    echo "  - Check Kafka messages: docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input_events --from-beginning --max-messages 5"
    echo "  - Check MinIO: make check-minio"
    exit 1
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Test Complete${NC}"
echo -e "${BLUE}========================================${NC}"

