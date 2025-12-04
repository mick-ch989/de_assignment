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
S3_ENDPOINT="${S3_ENDPOINT:-}"
INPUT_PATH="${INPUT_PATH:-}"
OUTPUT_PATH="${OUTPUT_PATH:-/tmp/percentile_results}"
OUTPUT_S3_PREFIX="${OUTPUT_S3_PREFIX:-query-results}"  # S3/MinIO prefix for output (empty = use local path)
# For Docker, use a path that can be mounted as volume
HOST_OUTPUT_PATH="${HOST_OUTPUT_PATH:-$PROJECT_ROOT/spark_query/results}"
CONTAINER_OUTPUT_PATH="/tmp/percentile_results"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

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
export S3_ENDPOINT
export INPUT_PATH
export OUTPUT_PATH
export OUTPUT_S3_PREFIX
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

echo "Configuration:"
if [ -n "$S3_BUCKET" ]; then
    echo "  S3 Bucket: $S3_BUCKET"
    echo "  S3 Prefix: $S3_PREFIX"
    [ -n "$S3_ENDPOINT" ] && echo "  S3 Endpoint: $S3_ENDPOINT"
elif [ -n "$INPUT_PATH" ]; then
    echo "  Input Path: $INPUT_PATH"
else
    echo "Error: Either S3_BUCKET or INPUT_PATH must be set"
    exit 1
fi
if docker ps | grep -q spark-master; then
    echo "  Container Output Path: $CONTAINER_OUTPUT_PATH"
    echo "  Host Output Path: $HOST_OUTPUT_PATH"
else
    echo "  Output Path: $OUTPUT_PATH"
fi
if [ -n "$S3_BUCKET" ] && [ -n "$OUTPUT_S3_PREFIX" ]; then
    echo "  S3 Output Path: s3a://$S3_BUCKET/$OUTPUT_S3_PREFIX"
fi
echo ""

# Get Docker network name
NETWORK_NAME=$(docker-compose ps -q spark-master 2>/dev/null | xargs -I {} docker inspect {} --format='{{range $net, $v := .NetworkSettings.Networks}}{{$net}}{{end}}' 2>/dev/null | head -1)

if [ -z "$NETWORK_NAME" ]; then
    NETWORK_NAME=$(basename "$PROJECT_ROOT" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')_default
    if ! docker network inspect "$NETWORK_NAME" &>/dev/null 2>&1; then
        NETWORK_NAME=""
    fi
fi

# Check if running in Docker or locally
if docker ps | grep -q spark-master; then
    echo "Running query in Docker container..."
    
    # Convert localhost to Docker service name for S3_ENDPOINT
    if [ -n "$S3_ENDPOINT" ] && echo "$S3_ENDPOINT" | grep -q "localhost"; then
        DOCKER_S3_ENDPOINT=$(echo "$S3_ENDPOINT" | sed 's/localhost/minio/g')
        echo "  Converting localhost to Docker service name: $S3_ENDPOINT -> $DOCKER_S3_ENDPOINT"
    else
        DOCKER_S3_ENDPOINT="$S3_ENDPOINT"
    fi
    
    # Use spark-streaming container if available, otherwise use a temporary container
    if docker ps | grep -q spark-streaming; then
        # Copy script to container if it doesn't exist
        docker exec spark-streaming mkdir -p /opt/spark_query || true
        docker cp "$QUERY_SCRIPT" spark-streaming:/opt/spark_query/percentile_query.py
        
        # Run in existing spark-streaming container with environment variables
        docker exec \
            -e "S3_BUCKET=$S3_BUCKET" \
            -e "S3_PREFIX=$S3_PREFIX" \
            -e "S3_ENDPOINT=$DOCKER_S3_ENDPOINT" \
            -e "INPUT_PATH=$INPUT_PATH" \
            -e "OUTPUT_PATH=$CONTAINER_OUTPUT_PATH" \
            -e "OUTPUT_S3_PREFIX=$OUTPUT_S3_PREFIX" \
            -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
            -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
            spark-streaming /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --driver-memory 512m \
            --executor-memory 512m \
            /opt/spark_query/percentile_query.py
        
        # Copy results from container to host
        echo ""
        echo "Copying results from container to host..."
        mkdir -p "$HOST_OUTPUT_PATH"
        docker cp "spark-streaming:$CONTAINER_OUTPUT_PATH/." "$HOST_OUTPUT_PATH/" 2>/dev/null || echo "Warning: Could not copy results from container"
    else
        # Use temporary container with Spark image
        if [ -n "$NETWORK_NAME" ] && [ "$NETWORK_NAME" != "bridge" ]; then
            NETWORK_ARG="--network $NETWORK_NAME"
        else
            NETWORK_ARG="--network host"
        fi
        
        # Create output directory on host
        mkdir -p "$HOST_OUTPUT_PATH"
        
        docker run --rm \
            $NETWORK_ARG \
            -v "$QUERY_SCRIPT:/opt/spark_query/percentile_query.py:ro" \
            -v "$HOST_OUTPUT_PATH:$CONTAINER_OUTPUT_PATH" \
            -e "S3_BUCKET=$S3_BUCKET" \
            -e "S3_PREFIX=$S3_PREFIX" \
            -e "S3_ENDPOINT=$DOCKER_S3_ENDPOINT" \
            -e "INPUT_PATH=$INPUT_PATH" \
            -e "OUTPUT_PATH=$CONTAINER_OUTPUT_PATH" \
            -e "OUTPUT_S3_PREFIX=$OUTPUT_S3_PREFIX" \
            -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
            -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
            apache/spark-py:latest \
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --driver-memory 512m \
            --executor-memory 512m \
            /opt/spark_query/percentile_query.py
    fi
else
    echo "Running query locally..."
    spark-submit \
        --master local[*] \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        "$QUERY_SCRIPT"
fi

echo ""
echo "Query completed!"
if [ -n "$S3_BUCKET" ] && [ -n "$OUTPUT_S3_PREFIX" ]; then
    echo "Results saved to MinIO/S3: s3://$S3_BUCKET/$OUTPUT_S3_PREFIX"
    echo ""
    echo "To view results in MinIO Console:"
    echo "  http://localhost:9001"
    echo "  Bucket: $S3_BUCKET"
    echo "  Path: $OUTPUT_S3_PREFIX"
elif docker ps | grep -q spark-master; then
    echo "Results saved to: $HOST_OUTPUT_PATH"
    echo "  (copied from container path: $CONTAINER_OUTPUT_PATH)"
else
    echo "Results saved to: $OUTPUT_PATH"
fi
echo ""
echo "To validate results:"
if [ -n "$S3_BUCKET" ] && [ -n "$OUTPUT_S3_PREFIX" ]; then
    echo "  (Results are in MinIO/S3, validation script needs S3 support)"
else
    echo "  OUTPUT_PATH=$HOST_OUTPUT_PATH ./scripts/validate_output.sh"
fi
echo ""

