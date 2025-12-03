#!/bin/bash
# Script to validate data in S3

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Validating S3 Data"
echo "=========================================="
echo ""

VALIDATION_SCRIPT="$PROJECT_ROOT/storage/validate_spark_read.py"

if [ ! -f "$VALIDATION_SCRIPT" ]; then
    echo "Error: Validation script not found: $VALIDATION_SCRIPT"
    exit 1
fi

# Configuration
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="${S3_PREFIX:-streaming-output}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"

if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET environment variable is not set"
    echo "Usage: S3_BUCKET=my-bucket ./scripts/validate_s3_data.sh"
    exit 1
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed"
    exit 1
fi

export S3_BUCKET
export S3_PREFIX
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

echo "S3 Bucket: $S3_BUCKET"
echo "S3 Prefix: $S3_PREFIX"
echo ""

python3 "$VALIDATION_SCRIPT"

