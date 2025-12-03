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

# Check if MinIO client (mc) is available
if ! command -v mc &> /dev/null; then
    echo -e "${YELLOW}MinIO client (mc) not found. Using Docker...${NC}"

    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed${NC}"
        echo "Please install Docker or MinIO client (mc)"
        exit 1
    fi

    # Use Docker to run mc command
    MC_CMD="docker run --rm --network host -e MC_HOST_myMinIO=$MINIO_ENDPOINT minio/mc"
else
    # Configure mc alias
    mc alias set myMinIO "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" 2>/dev/null || true
    MC_CMD="mc"
fi

# Check MinIO connectivity
echo -e "${YELLOW}Checking MinIO connectivity...${NC}"
if $MC_CMD admin info myMinIO &>/dev/null || docker run --rm --network host minio/mc admin info myMinIO --endpoint "$MINIO_ENDPOINT" --access-key "$MINIO_ACCESS_KEY" --secret-key "$MINIO_SECRET_KEY" &>/dev/null; then
    echo -e "${GREEN}✓ MinIO is accessible${NC}"
else
    echo -e "${RED}Error: Cannot connect to MinIO at $MINIO_ENDPOINT${NC}"
    echo "Please ensure MinIO is running and accessible"
    exit 1
fi

# Create bucket if it doesn't exist
echo -e "${YELLOW}Creating bucket: $BUCKET_NAME...${NC}"
if docker run --rm --network host minio/mc mb "myMinIO/$BUCKET_NAME" --endpoint "$MINIO_ENDPOINT" --access-key "$MINIO_ACCESS_KEY" --secret-key "$MINIO_SECRET_KEY" 2>/dev/null; then
    echo -e "${GREEN}✓ Bucket $BUCKET_NAME created${NC}"
else
    echo -e "${YELLOW}Bucket $BUCKET_NAME may already exist${NC}"
fi

# Set bucket policy for public read (optional, adjust as needed)
echo -e "${YELLOW}Setting bucket policy...${NC}"
docker run --rm --network host minio/mc anonymous set download "myMinIO/$BUCKET_NAME" --endpoint "$MINIO_ENDPOINT" --access-key "$MINIO_ACCESS_KEY" --secret-key "$MINIO_SECRET_KEY" 2>/dev/null || true

# Create prefix structure
echo -e "${YELLOW}Creating prefix structure: $PREFIX/...${NC}"
docker run --rm --network host minio/mc cp /dev/null "myMinIO/$BUCKET_NAME/$PREFIX/.keep" --endpoint "$MINIO_ENDPOINT" --access-key "$MINIO_ACCESS_KEY" --secret-key "$MINIO_SECRET_KEY" 2>/dev/null || true

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

