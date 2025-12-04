#!/bin/bash
# Script to set up MinIO bucket for streaming pipeline output

set -e

# Configuration
BUCKET_NAME="${S3_BUCKET_NAME:-streaming-pipeline-output}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
PREFIX="${S3_PREFIX:-streaming-output}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up MinIO bucket for streaming pipeline...${NC}"
echo "Bucket name: $BUCKET_NAME"
echo "MinIO endpoint: $MINIO_ENDPOINT"
echo "Prefix: $PREFIX"
echo ""

# Get project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Check if MinIO is running, start if not
echo -e "${YELLOW}Checking MinIO status...${NC}"
if docker ps --format '{{.Names}}' | grep -q "^minio$"; then
    echo -e "${GREEN}✓ MinIO container is running${NC}"
elif docker ps -a --format '{{.Names}}' | grep -q "^minio$"; then
    echo -e "${YELLOW}MinIO container exists but is not running. Starting it...${NC}"
    docker start minio
    echo "Waiting for MinIO to be ready..."
    sleep 5
else
    echo -e "${YELLOW}MinIO container not found. Starting MinIO via docker-compose...${NC}"
    docker-compose up -d minio
    echo "Waiting for MinIO to be ready..."
    sleep 10
fi

# Get Docker network name
NETWORK_NAME=$(docker-compose ps -q minio 2>/dev/null | xargs -I {} docker inspect {} --format='{{range $net, $v := .NetworkSettings.Networks}}{{$net}}{{end}}' 2>/dev/null | head -1)

# If that fails, try to get from project directory name
if [ -z "$NETWORK_NAME" ]; then
    NETWORK_NAME=$(basename "$PROJECT_ROOT" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')_default
    # Check if network exists
    if ! docker network inspect "$NETWORK_NAME" &>/dev/null 2>&1; then
        NETWORK_NAME=""
    fi
fi

# Check MinIO connectivity and set up MC command
echo -e "${YELLOW}Checking MinIO connectivity...${NC}"
MAX_RETRIES=10
RETRY_COUNT=0
MC_NETWORK_CMD=""

# First, try to connect via Docker network (if container is in a network)
if [ -n "$NETWORK_NAME" ] && [ "$NETWORK_NAME" != "bridge" ]; then
    MINIO_HOST="minio"
    MINIO_PORT="9000"
    echo -e "${YELLOW}Using Docker network: $NETWORK_NAME${NC}"
    
    # Use MC_HOST environment variable for persistent configuration
    MC_HOST_ENV="MC_HOST_myMinIO=http://$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY@$MINIO_HOST:$MINIO_PORT"
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        # Test connection using MC_HOST environment variable
        if docker run --rm --network "$NETWORK_NAME" -e "$MC_HOST_ENV" minio/mc admin info myMinIO &>/dev/null 2>&1; then
            echo -e "${GREEN}✓ MinIO is accessible via Docker network${NC}"
            MC_NETWORK_CMD="docker run --rm --network $NETWORK_NAME -e $MC_HOST_ENV minio/mc"
            break
        fi
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo -e "${YELLOW}Waiting for MinIO to be ready... (attempt $RETRY_COUNT/$MAX_RETRIES)${NC}"
            sleep 2
        fi
    done
fi

# If network connection failed, try localhost (for host network mode or port mapping)
if [ -z "$MC_NETWORK_CMD" ]; then
    echo -e "${YELLOW}Trying localhost connection...${NC}"
    RETRY_COUNT=0
    MC_HOST_ENV="MC_HOST_myMinIO=http://$MINIO_ACCESS_KEY:$MINIO_SECRET_KEY@localhost:9000"
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        # Test with curl first (faster)
        if curl -s -f "http://localhost:9000/minio/health/live" &>/dev/null 2>&1; then
            # Then test with mc using MC_HOST environment variable
            if docker run --rm --network host -e "$MC_HOST_ENV" minio/mc admin info myMinIO &>/dev/null 2>&1; then
                echo -e "${GREEN}✓ MinIO is accessible via localhost${NC}"
                MC_NETWORK_CMD="docker run --rm --network host -e $MC_HOST_ENV minio/mc"
                break
            fi
        fi
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo -e "${YELLOW}Waiting for MinIO to be ready... (attempt $RETRY_COUNT/$MAX_RETRIES)${NC}"
            sleep 2
        fi
    done
fi

# Final check
if [ -z "$MC_NETWORK_CMD" ]; then
    echo -e "${RED}Error: Cannot connect to MinIO at $MINIO_ENDPOINT${NC}"
    echo "Please ensure MinIO is running. You can start it with:"
    echo "  docker-compose up -d minio"
    echo "  or"
    echo "  make start"
    exit 1
fi

# Create bucket if it doesn't exist
echo -e "${YELLOW}Creating bucket: $BUCKET_NAME...${NC}"
if $MC_NETWORK_CMD mb "myMinIO/$BUCKET_NAME" 2>/dev/null; then
    echo -e "${GREEN}✓ Bucket $BUCKET_NAME created${NC}"
else
    echo -e "${YELLOW}Bucket $BUCKET_NAME may already exist${NC}"
fi

# Set bucket policy for public read (optional, adjust as needed)
echo -e "${YELLOW}Setting bucket policy...${NC}"
$MC_NETWORK_CMD anonymous set download "myMinIO/$BUCKET_NAME" 2>/dev/null || true

# Create prefix structure
echo -e "${YELLOW}Creating prefix structure: $PREFIX/...${NC}"
$MC_NETWORK_CMD cp /dev/null "myMinIO/$BUCKET_NAME/$PREFIX/.keep" 2>/dev/null || true

echo ""
echo -e "${GREEN}✓ MinIO bucket setup completed!${NC}"
echo ""
echo "Bucket details:"
echo "  Name: $BUCKET_NAME"
echo "  Endpoint: $MINIO_ENDPOINT"
echo "  Prefix: $PREFIX"
echo "  S3 Path: s3a://$BUCKET_NAME/$PREFIX/"
echo ""
echo "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "To use this bucket in Spark streaming job:"
echo "  export S3_BUCKET=$BUCKET_NAME"
echo "  export S3_PREFIX=$PREFIX"
echo "  export S3_ENDPOINT=$MINIO_ENDPOINT"
echo "  export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY"
echo "  export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY"
echo ""
