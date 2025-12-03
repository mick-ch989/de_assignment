# Scripts Directory

This directory contains utility scripts for managing and operating the streaming pipeline.

## Available Scripts

### Core Pipeline Scripts

#### `run_all.sh`
Starts all services in the streaming pipeline.

**Usage:**
```bash
./scripts/run_all.sh
```

**What it does:**
- Starts Zookeeper and Kafka
- Starts Spark cluster (master + 2 workers)
- Starts monitoring services (Prometheus, Grafana, exporters)
- Starts Spark streaming job
- Displays service URLs and status

#### `kill_all.sh`
Stops all services in the streaming pipeline.

**Usage:**
```bash
./scripts/kill_all.sh
```

**What it does:**
- Stops all Docker containers
- Cleans up services gracefully

#### `start_producer.sh`
Starts the Kafka producer to generate test data.

**Usage:**
```bash
# Using default Kafka (localhost:9092)
./scripts/start_producer.sh

# With custom Kafka server
KAFKA_BOOTSTRAP_SERVERS=kafka:9092 ./scripts/start_producer.sh
```

**What it does:**
- Checks Kafka connectivity
- Installs Python dependencies if needed
- Starts the producer to send events to `input_events` topic

#### `start_streaming.sh`
Starts the Spark streaming job.

**Usage:**
```bash
# Using default configuration
./scripts/start_streaming.sh

# With S3 output
S3_BUCKET=my-bucket ./scripts/start_streaming.sh
```

**What it does:**
- Checks Spark and Kafka are running
- Builds streaming job Docker image
- Starts the streaming job with proper configuration

### Query Scripts

#### `run_percentile_query.sh`
Runs the percentile analysis query.

**Usage:**
```bash
# Using S3
S3_BUCKET=my-bucket ./scripts/run_percentile_query.sh

# Using local path
INPUT_PATH=/path/to/data ./scripts/run_percentile_query.sh

# With AWS credentials
S3_BUCKET=my-bucket \
AWS_ACCESS_KEY_ID=your_key \
AWS_SECRET_ACCESS_KEY=your_secret \
./scripts/run_percentile_query.sh
```

**What it does:**
- Runs the 95th percentile query
- Computes statistics per device type per day
- Outputs results to CSV

**Environment Variables:**
- `S3_BUCKET`: S3 bucket name
- `S3_PREFIX`: S3 prefix (default: `streaming-output`)
- `INPUT_PATH`: Override input path (local or S3)
- `OUTPUT_PATH`: Output directory (default: `/tmp/percentile_results`)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key

#### `validate_output.sh`
Validates the percentile query output.

**Usage:**
```bash
# Using default output path
./scripts/validate_output.sh

# With custom output path
OUTPUT_PATH=/path/to/results ./scripts/validate_output.sh
```

**What it does:**
- Validates CSV file structure
- Checks data quality
- Verifies minimum events requirement
- Generates validation report

### Storage Scripts

#### `setup_s3_bucket.sh`
Sets up S3 bucket for streaming pipeline.

**Usage:**
```bash
# With default configuration
./scripts/setup_s3_bucket.sh

# With custom configuration
S3_BUCKET_NAME=my-bucket \
AWS_REGION=us-west-2 \
S3_PREFIX=streaming-output \
./scripts/setup_s3_bucket.sh
```

**What it does:**
- Creates S3 bucket
- Enables versioning
- Configures lifecycle policies
- Sets up prefix structure

**Prerequisites:**
- AWS CLI installed and configured
- AWS credentials with S3 permissions

#### `validate_s3_data.sh`
Validates data stored in S3.

**Usage:**
```bash
S3_BUCKET=my-bucket ./scripts/validate_s3_data.sh
```

**What it does:**
- Validates S3 path accessibility
- Reads and validates Parquet files
- Checks schema and data quality
- Generates validation report

**Environment Variables:**
- `S3_BUCKET`: S3 bucket name (required)
- `S3_PREFIX`: S3 prefix (default: `streaming-output`)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key

### Utility Scripts

#### `check_services.sh`
Checks the status of all services.

**Usage:**
```bash
./scripts/check_services.sh
```

**What it does:**
- Lists all Docker containers
- Checks service connectivity
- Displays service URLs
- Reports service status

## Quick Start

### 1. Start Everything

```bash
./scripts/run_all.sh
```

### 2. Start Producer (in separate terminal)

```bash
./scripts/start_producer.sh
```

### 3. Run Query

```bash
S3_BUCKET=my-bucket ./scripts/run_percentile_query.sh
```

### 4. Validate Results

```bash
./scripts/validate_output.sh
```

### 5. Stop Everything

```bash
./scripts/kill_all.sh
```

## Script Dependencies

### Required Tools

- **Docker & Docker Compose**: For containerized services
- **Python 3**: For producer and validation scripts
- **AWS CLI**: For S3 bucket setup (optional)
- **netcat (nc)**: For connectivity checks (optional)

### Python Dependencies

Producer and validation scripts require:
- `kafka-python`
- `faker` (for producer)
- Standard library modules

Install with:
```bash
pip install -r ingestion/producer/requirements.txt
```

## Environment Variables

Common environment variables used across scripts:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9092` |
| `KAFKA_TOPIC` | Kafka topic name | `input_events` |
| `S3_BUCKET` | S3 bucket name | - |
| `S3_PREFIX` | S3 prefix/path | `streaming-output` |
| `OUTPUT_PATH` | Output directory | `/tmp/percentile_results` |
| `AWS_ACCESS_KEY_ID` | AWS access key | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | - |

## Troubleshooting

### Scripts Not Executable

If you get "Permission denied" errors:

```bash
chmod +x scripts/*.sh
```

### Services Not Starting

Check Docker is running:
```bash
docker ps
```

Check service status:
```bash
./scripts/check_services.sh
```

### Kafka Connection Issues

Verify Kafka is accessible:
```bash
nc -z localhost 9092
```

If using Docker, ensure containers are on the same network.

### S3 Access Issues

Verify AWS credentials:
```bash
aws sts get-caller-identity
```

Check S3 bucket exists:
```bash
aws s3 ls s3://your-bucket-name
```

## Best Practices

1. **Use Environment Variables**: Set configuration via environment variables rather than editing scripts
2. **Check Service Status**: Use `check_services.sh` before running queries
3. **Monitor Logs**: Use `docker-compose logs -f` to monitor service logs
4. **Validate Outputs**: Always validate query outputs before using results
5. **Clean Up**: Use `kill_all.sh` to stop services when done

## Script Execution Order

Recommended execution order for a complete pipeline run:

1. `setup_s3_bucket.sh` - Set up storage (if using S3)
2. `run_all.sh` - Start all services
3. `start_producer.sh` - Generate test data
4. Wait for data to accumulate
5. `run_percentile_query.sh` - Run analysis
6. `validate_output.sh` - Validate results
7. `kill_all.sh` - Clean up

## Notes

- All scripts use relative paths and work from the project root
- Scripts check for prerequisites before execution
- Error messages provide guidance for common issues
- Scripts are idempotent (safe to run multiple times)

