#!/bin/bash
# Script to set up S3 bucket for streaming pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Setting Up S3 Bucket"
echo "=========================================="
echo ""

BUCKET_SCRIPT="$PROJECT_ROOT/storage/create_s3_bucket.sh"

if [ ! -f "$BUCKET_SCRIPT" ]; then
    echo "Error: S3 bucket creation script not found: $BUCKET_SCRIPT"
    exit 1
fi

# Check if script is executable
if [ ! -x "$BUCKET_SCRIPT" ]; then
    chmod +x "$BUCKET_SCRIPT"
fi

# Run the bucket creation script
"$BUCKET_SCRIPT"

echo ""
echo "S3 bucket setup completed!"
echo ""

