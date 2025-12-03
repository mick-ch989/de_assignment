#!/bin/bash
# Script to validate percentile query output

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Validating Percentile Query Output"
echo "=========================================="
echo ""

OUTPUT_PATH="${OUTPUT_PATH:-/tmp/percentile_results}"
VALIDATION_SCRIPT="$PROJECT_ROOT/spark_query/validate_output.py"

if [ ! -f "$VALIDATION_SCRIPT" ]; then
    echo "Error: Validation script not found: $VALIDATION_SCRIPT"
    exit 1
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed"
    exit 1
fi

export OUTPUT_PATH

echo "Output path: $OUTPUT_PATH"
echo ""

python3 "$VALIDATION_SCRIPT"

