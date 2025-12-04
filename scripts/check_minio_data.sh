#!/bin/bash
# Script to check if Spark is writing data to MinIO

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
BUCKET_NAME="${S3_BUCKET_NAME:-streaming-pipeline-output}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
PREFIX="${S3_PREFIX:-streaming-output}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Checking MinIO Data${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Bucket: $BUCKET_NAME"
echo "Prefix: $PREFIX"
echo "Endpoint: $MINIO_ENDPOINT"
echo ""

cd "$PROJECT_ROOT"

# Get Docker network name
NETWORK_NAME=$(docker-compose ps -q minio 2>/dev/null | xargs -I {} docker inspect {} --format='{{range $net, $v := .NetworkSettings.Networks}}{{$net}}{{end}}' 2>/dev/null | head -1)

if [ -z "$NETWORK_NAME" ]; then
    NETWORK_NAME=$(basename "$PROJECT_ROOT" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')_default
    if ! docker network inspect "$NETWORK_NAME" &>/dev/null 2>&1; then
        NETWORK_NAME=""
    fi
fi

# Set up MC command
if [ -n "$NETWORK_NAME" ] && [ "$NETWORK_NAME" != "bridge" ]; then
    MC_HOST_ENV="MC_HOST_myMinIO=http://$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY@minio:9000"
    MC_CMD="docker run --rm --network $NETWORK_NAME -e $MC_HOST_ENV minio/mc"
else
    MC_HOST_ENV="MC_HOST_myMinIO=http://$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY@localhost:9000"
    MC_CMD="docker run --rm --network host -e $MC_HOST_ENV minio/mc"
fi

echo -e "${YELLOW}1. Checking if bucket exists...${NC}"
if $MC_CMD ls myMinIO/$BUCKET_NAME &>/dev/null 2>&1; then
    echo -e "${GREEN}✓ Bucket $BUCKET_NAME exists${NC}"
else
    echo -e "${RED}✗ Bucket $BUCKET_NAME does not exist${NC}"
    echo "Run: make setup-minio"
    exit 1
fi

echo ""
echo -e "${YELLOW}2. Listing objects in bucket...${NC}"
OBJECT_COUNT=$($MC_CMD ls -r myMinIO/$BUCKET_NAME/$PREFIX/ 2>/dev/null | wc -l | tr -d ' ')

if [ "$OBJECT_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Found $OBJECT_COUNT objects in $BUCKET_NAME/$PREFIX/${NC}"
    echo ""
    echo -e "${YELLOW}Sample files:${NC}"
    $MC_CMD ls -r myMinIO/$BUCKET_NAME/$PREFIX/ 2>/dev/null | head -10
else
    echo -e "${YELLOW}⚠ No objects found yet${NC}"
    echo "This might mean:"
    echo "  - Spark streaming job hasn't written data yet"
    echo "  - Check if streaming job is running: make logs-streaming"
    echo "  - Check if producer is sending data: make producer"
fi

echo ""
echo -e "${YELLOW}3. Checking data structure...${NC}"
if [ "$OBJECT_COUNT" -gt 0 ]; then
    echo -e "${YELLOW}Partition structure:${NC}"
    $MC_CMD ls myMinIO/$BUCKET_NAME/$PREFIX/ 2>/dev/null | grep -E "device_type=" | head -5 || true
    
    echo ""
    echo -e "${YELLOW}File types:${NC}"
    PARQUET_COUNT=$($MC_CMD ls -r myMinIO/$BUCKET_NAME/$PREFIX/ 2>/dev/null | grep -c "\.parquet" || echo "0")
    echo "Parquet files: $PARQUET_COUNT"
fi

echo ""
echo -e "${YELLOW}4. Quick stats:${NC}"
if [ "$OBJECT_COUNT" -gt 0 ]; then
    TOTAL_SIZE=$($MC_CMD du myMinIO/$BUCKET_NAME/$PREFIX/ 2>/dev/null | tail -1 | awk '{print $4}' || echo "0")
    echo "Total size: $TOTAL_SIZE"
    
    DEVICE_TYPES=$($MC_CMD ls myMinIO/$BUCKET_NAME/$PREFIX/ 2>/dev/null | grep "device_type=" | wc -l | tr -d ' ')
    echo "Device types: $DEVICE_TYPES"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Additional Commands${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "View in MinIO Console: http://localhost:9001"
echo "  Login: minioadmin / minioadmin"
echo ""
echo "Check Spark streaming logs:"
echo "  make logs-streaming"
echo ""
echo "Validate data with Spark:"
echo "  make validate-s3 S3_BUCKET=$BUCKET_NAME"
echo ""

