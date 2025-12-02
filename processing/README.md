# Processing Module

This module contains the Spark Structured Streaming job that performs ETL operations on data consumed from Kafka.

## Overview

The streaming job:
- Consumes events from Kafka topic `input_events`
- Parses JSON messages and validates schema
- Filters invalid records (nulls, out-of-range values)
- Deduplicates records based on event_id
- Performs windowed aggregations (1-minute windows per device)
- Writes aggregated results to S3 or console

## Features

### ETL Operations

1. **Parsing**
   - JSON schema validation
   - Type conversion (strings to timestamps, etc.)
   - Nested structure flattening

2. **Filtering**
   - Removes null/invalid records
   - Validates numeric ranges (battery level, signal strength, coordinates)
   - Filters invalid event durations

3. **Windowed Transformations**
   - 1-minute tumbling windows
   - 10-minute watermark for late data handling
   - Per-device aggregations

4. **Aggregations**
   - Event count per device per minute
   - Average, min, max event duration
   - Average, min battery level
   - Average, min signal strength
   - Location and firmware information

5. **Deduplication**
   - Removes duplicate events based on event_id and device_id
   - Uses watermark for state management

### Fault Tolerance

- **Checkpointing**: All state stored in checkpoint location for recovery
- **Graceful Shutdown**: Handles SIGTERM/SIGINT signals
- **Error Handling**: Comprehensive try-catch blocks with logging
- **Retry Logic**: Configurable retry mechanisms (can be extended)
- **Data Loss Prevention**: `failOnDataLoss=false` prevents job failure on Kafka issues
- **Watermarking**: Handles late-arriving data gracefully

### Error Handling

- Logging at multiple levels (INFO, WARN, ERROR)
- Exception handling for:
  - Spark session creation failures
  - Kafka connection issues
  - JSON parsing errors
  - Schema validation errors
  - Streaming query exceptions
- Graceful degradation and recovery

## Configuration

All configuration is done via environment variables. The job does not use configuration files.

### Environment Variables

#### Kafka Configuration
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `kafka:9092`)
- `KAFKA_TOPIC`: Kafka topic to consume from (default: `input_events`)
- `KAFKA_STARTING_OFFSETS`: Starting offset strategy (default: `latest`)
- `KAFKA_MAX_OFFSETS_PER_TRIGGER`: Maximum offsets per trigger (default: `1000`)
- `KAFKA_FAIL_ON_DATA_LOSS`: Fail on data loss (default: `false`)

#### Streaming Configuration
- `CHECKPOINT_LOCATION`: Spark checkpoint directory (default: `/tmp/streaming_checkpoint`)
- `OUTPUT_PATH`: Output path for results (default: `/tmp/streaming_output`)
- `WINDOW_DURATION`: Window duration for aggregations (default: `1 minute`)
- `WATERMARK_DELAY`: Watermark delay for late data (default: `10 minutes`)

#### Storage Configuration
- `S3_BUCKET`: S3/MinIO bucket name for output (optional, if not set writes to console)
- `S3_PREFIX`: S3/MinIO prefix for output files (default: `streaming-output`)
- `S3_ENDPOINT`: Storage endpoint (required for MinIO, e.g., `http://minio:9000`)

#### AWS/MinIO Credentials
Credentials are obtained in the following order:
1. Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (or `S3_ACCESS_KEY` and `S3_SECRET_KEY`)
2. boto3 credential chain (if boto3 is available and not using MinIO):
   - `~/.aws/credentials` file
   - IAM roles (when running on EC2/ECS)
   - Environment variables
   - Other boto3 credential providers

**For MinIO:**
- Default credentials: `minioadmin` / `minioadmin`
- Set via environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Endpoint must be set: `S3_ENDPOINT=http://minio:9000`

#### Spark Configuration
- `SPARK_SQL_ADAPTIVE_ENABLED`: Enable adaptive execution (default: `true`)
- `SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED`: Enable partition coalescing (default: `true`)
- `SPARK_SQL_STREAMING_SCHEMA_INFERENCE`: Enable schema inference (default: `true`)
- `SPARK_SQL_STREAMING_STOP_GRACEFULLY_ON_SHUTDOWN`: Graceful shutdown (default: `true`)
- `SPARK_SERIALIZER`: Spark serializer class (default: `org.apache.spark.serializer.KryoSerializer`)
- `SPARK_SQL_STREAMING_MIN_BATCHES_TO_RETAIN`: Minimum batches to retain (default: `10`)

## Usage

### Running with Docker Compose

The streaming job is automatically started when you run:

```bash
docker-compose up -d spark-streaming
```

### Running Manually

1. Ensure Spark cluster is running
2. Ensure Kafka is running and has data
3. Run the job:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_streaming_job.py
```

### With S3 Output

```bash
# Using environment variables for AWS credentials
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e S3_BUCKET=my-bucket \
  -e S3_PREFIX=streaming-output \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  --network assignment_default \
  spark-streaming

# Or using boto3 credential chain (IAM roles, ~/.aws/credentials, etc.)
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e S3_BUCKET=my-bucket \
  -e S3_PREFIX=streaming-output \
  -v ~/.aws:/root/.aws:ro \
  --network assignment_default \
  spark-streaming
```

## Output Schema

The aggregated output contains:

```json
{
  "window_start": "2024-01-15T10:30:00.000Z",
  "window_end": "2024-01-15T10:31:00.000Z",
  "device_id": "dev_42",
  "device_type": "sensor_A",
  "event_count": 120,
  "avg_duration": 2.345,
  "max_duration": 4.567,
  "min_duration": 0.123,
  "avg_battery_level": 85.5,
  "min_battery_level": 80,
  "avg_signal_strength": -75.2,
  "min_signal_strength": -85,
  "city": "New York",
  "country": "United States",
  "firmware_version": "3.2.15",
  "last_event_time": "2024-01-15T10:30:59.123Z"
}
```

## Monitoring

The streaming job can be monitored via:
- Spark UI: http://localhost:8080
- Prometheus metrics (via JMX exporters)
- Grafana dashboards

## Checkpointing and Recovery

The job uses Spark's checkpointing mechanism:
- State is stored in `CHECKPOINT_LOCATION`
- On restart, the job resumes from the last checkpoint
- Handles failures gracefully with automatic recovery

## Performance Tuning

Key configuration options for performance:
- `maxOffsetsPerTrigger`: Controls batch size (default: 1000)
- Window duration: Currently 1 minute (configurable)
- Watermark delay: 10 minutes (configurable)
- Adaptive execution: Enabled by default

## Troubleshooting

### Job Not Starting
- Check Spark master is accessible
- Verify Kafka connectivity
- Check checkpoint directory permissions

### No Data Processing
- Verify Kafka topic has messages
- Check starting offsets configuration
- Verify JSON schema matches input data

### High Latency
- Adjust `maxOffsetsPerTrigger`
- Tune window duration
- Check Spark resource allocation

### Checkpoint Issues
- Ensure checkpoint directory is writable
- Clear checkpoint if schema changes
- Monitor disk space

## Dependencies

- PySpark 3.5.0+
- Kafka connector: `spark-sql-kafka-0-10`
- S3 connector (if using S3): `hadoop-aws`, `aws-java-sdk`

## Testing

### Running Tests

The module includes comprehensive unit and integration tests.

#### Install Test Dependencies

```bash
pip install -r test_requirements.txt
```

#### Run Unit Tests

```bash
pytest test_spark_streaming_job.py -v
```

#### Run All Tests (excluding integration)

```bash
pytest -v -m "not integration"
```

#### Run Integration Tests

```bash
pytest test_integration.py -v -m integration
```

#### Run with Coverage

```bash
pytest --cov=spark_streaming_job --cov-report=html test_spark_streaming_job.py
```

### Test Structure

- **`test_spark_streaming_job.py`**: Unit tests for all functions
  - Tests for Spark session creation
  - Tests for Kafka reading
  - Tests for JSON parsing
  - Tests for filtering logic
  - Tests for deduplication
  - Tests for aggregations
  - Tests for sink writing
  - Tests for error handling
  - Tests for main function workflow

- **`test_integration.py`**: Integration tests
  - End-to-end pipeline tests
  - Schema validation tests
  - Performance tests

- **`conftest.py`**: Shared pytest fixtures
- **`pytest.ini`**: Pytest configuration

### Test Coverage

The test suite covers:
- ✅ All main functions
- ✅ Error handling scenarios
- ✅ Configuration management
- ✅ Schema validation
- ✅ Data filtering logic
- ✅ Aggregation functions
- ✅ Sink writing (console and S3)

## Next Steps

After processing, aggregated data can be:
- Queried using Spark SQL (see `spark_query` module)
- Exported to data warehouse
- Used for real-time analytics
- Stored in S3 for batch processing
