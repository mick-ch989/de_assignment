# Streaming Pipeline Project
## Time spent: 12hrs
A comprehensive real-time streaming data pipeline using Kafka, Spark Structured Streaming, and S3/MinIO storage.

## 🚀 Quick Start

### One-Command Setup

```bash
make setup
```

This will:
- Install all Python dependencies
- Make scripts executable
- Check Docker installation
- Build Docker images

### Start Everything

```bash
make start
```

Or use the individual commands:
```bash
make setup    # One-time setup
make build    # Build Docker images
make start    # Start all services
```

## 📋 Prerequisites

- **Docker** and **Docker Compose**
- **Python 3.11+**
- **Make** (for convenience commands)
- **AWS CLI** (optional, only if using AWS S3 instead of MinIO)

## 🏗️ Project Structure

```
assignment/
├── ingestion/          # Kafka producer for generating test data
├── processing/         # Spark Structured Streaming ETL job
├── storage/            # S3/MinIO bucket setup and validation
├── spark_query/       # Analytics queries (percentile calculations)
├── monitoring/         # Prometheus and Grafana setup
├── scripts/            # Utility scripts for operations
├── .github/workflows/  # GitHub Actions CI/CD
├── Makefile           # One-command operations
└── docker-compose.yml # Service orchestration
```

## 🎯 Makefile Commands

The project includes a comprehensive Makefile for easy operations:

### Setup & Installation
- `make setup` - One-command setup (install deps, build images)
- `make install` - Install Python dependencies
- `make build` - Build Docker images
- `make dev-setup` - Development setup with all tools

### Service Management
- `make start` - Start all services
- `make stop` - Stop all services
- `make restart` - Restart all services
- `make status` - Check service status
- `make logs` - Show all service logs
- `make monitor` - Open monitoring dashboards

### Individual Services
- `make producer` - Start Kafka producer
- `make streaming` - Start Spark streaming job

### Queries & Validation
- `make query` - Run percentile query (requires S3_BUCKET)
- `make validate-output` - Validate query output
- `make validate-s3` - Validate S3/MinIO data
- `make setup-s3` - Set up S3 bucket (AWS)
- `make setup-minio` - Set up MinIO bucket (local)

### Testing
- `make test` - Run all tests (excluding integration)
- `make test-unit` - Run unit tests only
- `make test-integration` - Run integration tests
- `make test-coverage` - Run tests with coverage report

### Development
- `make lint` - Run linting checks
- `make format` - Format Python code
- `make clean` - Clean temporary files
- `make clean-docker` - Clean Docker resources
- `make clean-all` - Clean everything

### Quick Commands
- `make all` - Complete setup (install, build, start)
- `make pipeline` - Full pipeline (setup, start, producer)
- `make quick-start` - Quick start services
- `make quick-stop` - Quick stop services

**See all commands:**
```bash
make help
```

## 📊 Monitoring

Access monitoring dashboards:
- **Spark UI**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)

Or use:
```bash
make monitor
```

## 🧪 Testing

### Run Tests
```bash
make test              # All tests (excluding integration)
make test-unit         # Unit tests only
make test-integration  # Integration tests
make test-coverage     # With coverage report
```

### Manual Testing
```bash
cd processing
pytest test_spark_streaming_job.py -v
```

## 🔧 Configuration

### Environment Variables

Key environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker | `localhost:9092` |
| `S3_BUCKET` | S3/MinIO bucket name | - |
| `S3_PREFIX` | S3/MinIO prefix | `streaming-output` |
| `S3_ENDPOINT` | Storage endpoint (required for MinIO) | - |
| `AWS_ACCESS_KEY_ID` | Access key (MinIO or AWS) | - |
| `AWS_SECRET_ACCESS_KEY` | Secret key (MinIO or AWS) | - |

### Docker Compose

Services are configured in `docker-compose.yml`:
- Zookeeper & Kafka
- Spark cluster (1 master, 2 workers)
- Spark streaming job
- **MinIO** (S3-compatible storage, configured by default)
- Monitoring (Prometheus, Grafana)
- JMX exporters

**Storage Options:**
- **MinIO** (default): Local S3-compatible storage, no AWS account needed
- **AWS S3**: Cloud storage, requires AWS credentials

## 📦 Modules

### Ingestion
Kafka producer that generates realistic device event data.
- **Location**: `ingestion/`
- **Start**: `make producer` or `./scripts/start_producer.sh`

### Processing
Spark Structured Streaming ETL job with:
- JSON parsing and validation
- Outlier filtering
- Windowed aggregations
- Deduplication
- **Location**: `processing/`
- **Start**: `make streaming` or `./scripts/start_streaming.sh`

### Storage
S3/MinIO bucket management and data validation.
- **Location**: `storage/`
- **MinIO Setup**: `make setup-minio` or `./storage/setup_minio_bucket.sh` (default)
- **AWS S3 Setup**: `make setup-s3` or `./scripts/setup_s3_bucket.sh`

### Spark Query
Analytics queries including 95th percentile calculations.
- **Location**: `spark_query/`
- **Run**: `make query S3_BUCKET=my-bucket`

### Monitoring
Prometheus and Grafana for metrics and dashboards.
- **Location**: `monitoring/`
- **Access**: Automatically started with `make start`

## 🚀 GitHub Actions

The project includes comprehensive CI/CD workflows:

### Continuous Integration (`ci.yml`)
- Code linting (flake8, black)
- Unit tests
- Script validation
- Docker image builds
- Security scanning (Trivy)

### Integration Tests (`integration-tests.yml`)
- Full integration tests with Kafka
- End-to-end pipeline validation

### Release (`release.yml`)
- Automated Docker image builds
- Image publishing to Docker Hub

**Workflows run on:**
- Push to main/master/develop branches
- Pull requests
- Manual workflow dispatch

## 📝 Scripts

Utility scripts in `scripts/` directory:
- `run_all.sh` - Start all services
- `kill_all.sh` - Stop all services
- `start_producer.sh` - Start producer
- `start_streaming.sh` - Start streaming job
- `run_percentile_query.sh` - Run analytics query
- `validate_output.sh` - Validate query results
- `setup_s3_bucket.sh` - Set up AWS S3 bucket
- `setup_minio_bucket.sh` - Set up MinIO bucket (in storage/)
- `validate_s3_data.sh` - Validate S3/MinIO data
- `check_services.sh` - Check service status

See `scripts/README.md` for detailed documentation.

## 🛠️ Development

### Setup Development Environment
```bash
make dev-setup
```

### Code Quality
```bash
make lint      # Check code quality
make format    # Format code
```

### Cleanup
```bash
make clean         # Clean temporary files
make clean-docker  # Clean Docker resources
make clean-all     # Clean everything
```

## 📚 Documentation

- **Ingestion**: `ingestion/README.md`
- **Processing**: `processing/README.md`
- **Storage**: `storage/README.md`
- **Spark Query**: `spark_query/README.md`
- **Monitoring**: `monitoring/README.md`
- **Scripts**: `scripts/README.md`

## 🔍 Troubleshooting

### Services Not Starting
```bash
make status        # Check service status
make logs          # View logs
docker ps          # Check Docker containers
```

### Common Issues

**Docker not running:**
```bash
# Start Docker daemon
sudo systemctl start docker  # Linux
# Or start Docker Desktop (Mac/Windows)
```

**Port conflicts:**
- Check if ports 9092, 8080, 9090, 3000 are available
- Modify `docker-compose.yml` if needed

**Memory issues:**
- Increase Docker memory limit
- Adjust Spark executor memory in `docker-compose.yml`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Check code quality: `make lint`
6. Submit a pull request

## 🙏 Acknowledgments

Built with:
- Apache Kafka
- Apache Spark
- Docker & Docker Compose
- Prometheus & Grafana
- Python & PySpark
