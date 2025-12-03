#!/bin/bash
# Script to run the percentile query

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Running Percentile Query"
echo "=========================================="
echo ""

# Configuration
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="${S3_PREFIX:-streaming-output}"
INPUT_PATH="${INPUT_PATH:-}"
OUTPUT_PATH="${OUTPUT_PATH:-/tmp/percentile_results}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"

# Check if Spark is available
if ! command -v spark-submit &> /dev/null && ! docker ps | grep -q spark-master; then
    echo "Error: Spark is not available"
    echo "Please ensure Spark is running or use Docker"
    exit 1
fi

QUERY_SCRIPT="$PROJECT_ROOT/spark_query/percentile_query.py"

if [ ! -f "$QUERY_SCRIPT" ]; then
    echo "Error: Query script not found: $QUERY_SCRIPT"
    exit 1
fi

# Set environment variables
export S3_BUCKET
export S3_PREFIX
export INPUT_PATH
export OUTPUT_PATH
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

echo "Configuration:"
if [ -n "$S3_BUCKET" ]; then
    echo "  S3 Bucket: $S3_BUCKET"
    echo "  S3 Prefix: $S3_PREFIX"
elif [ -n "$INPUT_PATH" ]; then
    echo "  Input Path: $INPUT_PATH"
else
    echo "Error: Either S3_BUCKET or INPUT_PATH must be set"
    exit 1
fi
echo "  Output Path: $OUTPUT_PATH"
echo ""

# Check if running in Docker or locally
if docker ps | grep -q spark-master; then
    echo "Running query in Docker container..."
    docker-compose exec -T spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /opt/spark_query/percentile_query.py
else
    echo "Running query locally..."
    spark-submit \
        --master local[*] \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        "$QUERY_SCRIPT"
fi

echo ""
echo "Query completed!"
echo "Results saved to: $OUTPUT_PATH"
echo ""
echo "To validate results:"
echo "  ./scripts/validate_output.sh"
echo ""

