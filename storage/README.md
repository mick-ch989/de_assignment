# Storage Module

This module provides utilities for managing storage (S3 or MinIO) for the streaming pipeline, including bucket creation and data validation.

## Overview

The storage module includes:
- **S3/MinIO Bucket Creation**: Scripts to create and configure buckets for streaming output
- **Data Validation**: Script to validate that data written by Spark can be read correctly from storage
- **MinIO Support**: Full support for MinIO as S3-compatible local storage

## Components

### 1. S3 Bucket Creation (`create_s3_bucket.sh`)

Creates and configures an S3 bucket for storing streaming pipeline output.

**Features:**
- Creates S3 bucket with specified name and region
- Enables versioning for data protection
- Configures lifecycle policies (auto-delete old versions)
- Sets up prefix structure for organized data storage
- Validates AWS credentials and CLI installation

**Usage:**

```bash
# Set environment variables
export S3_BUCKET_NAME=streaming-pipeline-output
export AWS_REGION=us-east-1
export S3_PREFIX=streaming-output

# Configure AWS credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Or use AWS CLI configuration
aws configure

# Run the script
./create_s3_bucket.sh
```

**Environment Variables:**
- `S3_BUCKET_NAME`: Name of the S3 bucket (default: `streaming-pipeline-output`)
- `AWS_REGION`: AWS region (default: `us-east-1`)
- `S3_PREFIX`: Prefix/directory path in bucket (default: `streaming-output`)

**Prerequisites:**
- AWS CLI installed and configured
- AWS credentials with S3 permissions
- Appropriate IAM permissions:
  - `s3:CreateBucket`
  - `s3:PutBucketVersioning`
  - `s3:PutBucketLifecycleConfiguration`
  - `s3:PutObject`

### 4. Data Validation (`validate_spark_read.py`)

Validates that data written by the Spark streaming job can be read correctly from S3 or MinIO.

**Features:**
- Validates S3 path accessibility
- Reads Parquet files from S3
- Validates schema against expected structure
- Checks data quality (nulls, statistics)
- Validates partition structure
- Provides detailed validation report

**Usage:**

```bash
# Set environment variables
export S3_BUCKET=streaming-pipeline-output
export S3_PREFIX=streaming-output
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Run validation
python validate_spark_read.py
```

**Environment Variables:**
- `S3_BUCKET`: S3/MinIO bucket name (required)
- `S3_PREFIX`: Prefix/directory path in bucket (default: `streaming-output`)
- `AWS_ACCESS_KEY_ID`: Access key (required for S3, or MinIO access key)
- `AWS_SECRET_ACCESS_KEY`: Secret key (required for S3, or MinIO secret key)
- `S3_ENDPOINT`: Storage endpoint (required for MinIO, e.g., `http://minio:9000`)

**Validation Checks:**
1. **S3 Path Exists**: Verifies the S3 path is accessible
2. **Read Successful**: Confirms Parquet files can be read
3. **Schema Valid**: Validates DataFrame has expected columns
4. **Data Quality**: Checks for nulls, validates statistics
5. **Partitions Valid**: Verifies partition structure

**Expected Schema:**

The validation expects the following columns in the Parquet files:
- `window_start`, `window_end`: Window timestamps
- `device_id`, `device_type`: Device identifiers
- `event_count`: Number of events in window
- `avg_duration`, `max_duration`, `min_duration`: Event duration statistics
- `avg_battery_level`, `min_battery_level`: Battery statistics
- `avg_signal_strength`, `min_signal_strength`: Signal strength statistics
- `city`, `country`: Location information
- `firmware_version`: Device firmware
- `last_event_time`: Timestamp of last event in window

## Integration with Streaming Pipeline

### Setting Up S3 for Streaming

1. **Create S3 Bucket:**
   ```bash
   cd storage
   ./create_s3_bucket.sh
   ```

2. **Configure Streaming Job:**
   ```bash
   export S3_BUCKET=streaming-pipeline-output
   export S3_PREFIX=streaming-output
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   ```

3. **Run Streaming Job:**
   The streaming job will automatically write to S3 if `S3_BUCKET` is set:
   ```bash
   docker-compose up -d spark-streaming
   ```

4. **Validate Data:**
   ```bash
   python validate_spark_read.py
   ```

### Storage Path Structure

Data is stored in S3/MinIO with the following structure:

```
s3://bucket-name/streaming-output/
├── device_type=sensor_A/
│   └── window_start=2024-01-15 10:30:00/
│       └── part-00000-*.parquet
├── device_type=sensor_B/
│   └── window_start=2024-01-15 10:30:00/
│       └── part-00000-*.parquet
└── ...
```

Partitioned by:
- `device_type`: Type of device
- `window_start`: Start time of aggregation window

## Configuration

### Spark S3/MinIO Configuration

The streaming job automatically configures Spark for S3 or MinIO based on environment variables.

**For MinIO:**
```python
spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
```

**For AWS S3:**
```python
spark.conf.set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "false")
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
```

### Required Spark Packages

For S3 access, Spark needs:
- `org.apache.hadoop:hadoop-aws:3.3.4`
- `com.amazonaws:aws-java-sdk-bundle:1.12.262`

These are automatically included when using the Docker image.

## Troubleshooting

### S3 Access Issues

**Error: "Access Denied"**
- Verify AWS credentials are correct
- Check IAM permissions for S3 access
- Ensure bucket policy allows access

**Error: "Bucket does not exist"**
- Run `create_s3_bucket.sh` to create the bucket
- Verify bucket name matches `S3_BUCKET` environment variable

**Error: "No files found"**
- Ensure streaming job has written data
- Check S3 prefix path is correct
- Verify data exists in the expected partition structure

### Validation Issues

**Schema Mismatch:**
- Ensure streaming job schema matches expected schema
- Check for schema evolution issues
- Verify Parquet files are not corrupted

**Read Failures:**
- Check Spark S3 configuration
- Verify AWS credentials in Spark session
- Ensure S3 endpoint is accessible

## Best Practices

1. **Bucket Naming**: Use descriptive, unique bucket names
2. **Versioning**: Enable versioning for data protection
3. **Lifecycle Policies**: Configure automatic cleanup of old data
4. **Partitioning**: Use appropriate partition columns (device_type, window_start)
5. **Monitoring**: Regularly validate data quality
6. **Backup**: Consider cross-region replication for critical data
7. **Access Control**: Use IAM roles and policies for secure access

## Cost Optimization

- **Lifecycle Policies**: Automatically delete old versions after retention period
- **Storage Classes**: Consider using S3 Intelligent-Tiering for cost savings
- **Compression**: Parquet format provides good compression
- **Partitioning**: Efficient partitioning reduces query costs

## Security

- **Credentials**: Never commit AWS credentials to version control
- **IAM Roles**: Use IAM roles instead of access keys when possible
- **Bucket Policies**: Restrict access to specific IPs or VPCs if needed
- **Encryption**: Enable S3 server-side encryption for data at rest

## Next Steps

After setting up S3 storage:
1. Configure streaming job to write to S3
2. Monitor data ingestion
3. Set up data validation pipeline
4. Configure data lake queries (see `spark_query` module)
5. Set up data archival and retention policies
