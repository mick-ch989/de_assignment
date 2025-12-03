# Spark Query Module

This module contains Spark queries for analyzing data stored in S3, including percentile calculations and statistical analysis.

## Overview

The module provides:
- **Percentile Query**: Computes 95th percentile of event_duration per device type per day with outlier filtering
- **Output Validation**: Validates the query results meet requirements
- **Resource Optimization**: Configuration for efficient query execution

## Query: 95th Percentile Analysis

### Requirements

The query computes:
- **95th percentile** of `event_duration` per device type per day
- **Outlier filtering**: Excludes events outside 3 standard deviations from daily mean
- **Minimum events**: Only includes device types with at least 500 distinct events per day
- **Output format**: CSV file with validation

### Query Logic

1. **Read Data**: Reads Parquet or JSON data from S3 or local filesystem
2. **Prepare Events**: Extracts event_duration, device_type, and date information
3. **Filter Outliers**: Removes events outside 3 standard deviations from daily mean per device type
4. **Filter by Event Count**: Keeps only device type-days with ≥500 distinct events
5. **Compute Percentile**: Calculates 95th percentile using `percentile_approx`
6. **Output Results**: Writes to CSV with statistics

### Output Schema

The output CSV contains:
- `device_type`: Type of device (sensor_A, sensor_B, camera, thermo)
- `event_date`: Date of the events (YYYY-MM-DD)
- `percentile_95`: 95th percentile of event_duration (seconds)
- `mean_duration`: Mean event duration for the day (seconds)
- `stddev_duration`: Standard deviation of event duration (seconds)
- `total_events`: Total number of events in the day
- `distinct_devices`: Number of distinct devices

## Usage

### Prerequisites

1. **Data Available**: Ensure data is written to S3 by the streaming job
2. **Spark Cluster**: Access to Spark cluster (local or distributed)
3. **S3 Access**: AWS credentials configured (if using S3)

### Running the Query

#### Option 1: Using MinIO (Recommended for Local Development)

```bash
# Set environment variables for MinIO
export S3_BUCKET=streaming-pipeline-output
export S3_PREFIX=streaming-output
export S3_ENDPOINT=http://localhost:9000
export OUTPUT_PATH=/tmp/percentile_results
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# Run query
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  percentile_query.py
```

#### Option 2: Using AWS S3

```bash
# Set environment variables
export S3_BUCKET=streaming-pipeline-output
export S3_PREFIX=streaming-output
export S3_RAW_PREFIX=raw-events  # If raw events are in separate location
export OUTPUT_PATH=/tmp/percentile_results
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Run query
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  percentile_query.py
```

#### Option 3: Using Local Path

```bash
export INPUT_PATH=/path/to/parquet/files
export OUTPUT_PATH=/tmp/percentile_results

spark-submit percentile_query.py
```

#### Option 4: Using Docker

```bash
docker run --rm \
  -e S3_BUCKET=my-bucket \
  -e S3_PREFIX=streaming-output \
  -e OUTPUT_PATH=/tmp/results \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  --network assignment_default \
  spark-query percentile_query.py
```

### Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `S3_BUCKET` | - | S3/MinIO bucket name |
| `S3_PREFIX` | `streaming-output` | S3/MinIO prefix/path |
| `S3_ENDPOINT` | - | Storage endpoint (required for MinIO, e.g., `http://minio:9000`) |
| `S3_RAW_PREFIX` | `raw-events` | Path to raw events (if separate) |
| `INPUT_PATH` | - | Override: full input path (S3/MinIO or local) |
| `OUTPUT_PATH` | `/tmp/percentile_results` | Output directory |
| `OUTPUT_FORMAT` | `csv` | Output format (csv or parquet) |
| `MIN_EVENTS_PER_DAY` | `500` | Minimum distinct events per day |
| `PERCENTILE` | `0.95` | Percentile to compute (0.95 = 95th) |
| `OUTLIER_STD_DEVIATIONS` | `3.0` | Standard deviations for outlier filtering |
| `AWS_ACCESS_KEY_ID` | - | Access key (MinIO or AWS) |
| `AWS_SECRET_ACCESS_KEY` | - | Secret key (MinIO or AWS) |

### Validating Output

After running the query, validate the output:

```bash
export OUTPUT_PATH=/tmp/percentile_results
python validate_output.py
```

The validation script checks:
- ✓ CSV file exists and is readable
- ✓ Header matches expected columns
- ✓ All rows have valid data
- ✓ Minimum events requirement (≥500) is met
- ✓ Data types are correct

## Resource Usage Considerations

### Memory Tuning

The query is optimized for large datasets with the following configurations:

#### Spark Configuration

```python
# Adaptive execution (automatically optimizes)
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true

# Shuffle partitions (adjust based on data size)
spark.sql.shuffle.partitions = 200  # Default: 200

# File reading optimization
spark.sql.files.maxPartitionBytes = 134217728  # 128MB
spark.sql.files.openCostInBytes = 4194304  # 4MB
```

#### Memory Recommendations

- **Driver Memory**: 2-4GB (for small datasets) or 8-16GB (for large datasets)
- **Executor Memory**: 4-8GB per executor
- **Executor Cores**: 2-4 cores per executor
- **Number of Executors**: Based on cluster size

**Example Spark Submit:**
```bash
spark-submit \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  percentile_query.py
```

### Shuffling Considerations

The query performs several shuffle operations:

1. **GroupBy for Daily Statistics**: Groups by `device_type` and `event_date`
   - **Shuffle Size**: Depends on number of unique device_type-date combinations
   - **Optimization**: Uses adaptive execution to coalesce partitions

2. **Join for Outlier Filtering**: Joins events with daily statistics
   - **Shuffle Size**: Full dataset size
   - **Optimization**: Broadcast join if daily stats are small (<10MB)

3. **GroupBy for Percentile**: Groups by `device_type` and `event_date` again
   - **Shuffle Size**: Filtered dataset size
   - **Optimization**: Skew join enabled for handling data skew

#### Shuffle Optimization Tips

1. **Adjust Shuffle Partitions**:
   ```python
   # For small datasets (< 10GB)
   spark.conf.set("spark.sql.shuffle.partitions", "100")
   
   # For large datasets (> 100GB)
   spark.conf.set("spark.sql.shuffle.partitions", "400")
   ```

2. **Monitor Shuffle Spill**:
   - If you see "spill to disk" warnings, increase executor memory
   - Check Spark UI for shuffle read/write metrics

3. **Handle Data Skew**:
   - The query uses `spark.sql.adaptive.skewJoin.enabled = true`
   - For extreme skew, consider salting or repartitioning

### Performance Optimization

#### For Small Datasets (< 10GB)

```bash
spark-submit \
  --master local[4] \
  --driver-memory 2g \
  --executor-memory 4g \
  percentile_query.py
```

#### For Medium Datasets (10-100GB)

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 5 \
  percentile_query.py
```

#### For Large Datasets (> 100GB)

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 20 \
  --conf spark.sql.shuffle.partitions=400 \
  percentile_query.py
```

### Percentile Calculation

The query uses `percentile_approx` with accuracy parameter:

```python
percentile_approx("event_duration", 0.95, 10000)
```

- **Accuracy**: 10000 provides good balance between accuracy and performance
- **Memory**: Uses approximate algorithm to avoid full sort
- **Trade-off**: Slight accuracy loss for significant performance gain

For exact percentiles (slower but more accurate):
```python
# Use collect_list and sort (requires more memory)
from pyspark.sql.functions import collect_list, expr
percentile_exact = expr("percentile_approx(event_duration, 0.95, 100000)")
```

### Monitoring Query Execution

1. **Spark UI**: Monitor at `http://spark-master:8080`
   - Check stage execution times
   - Monitor shuffle read/write
   - Watch for task failures

2. **Logs**: Check for warnings about:
   - Spill to disk
   - Skew detection
   - Long-running tasks

3. **Metrics to Watch**:
   - Shuffle read/write size
   - Task execution time
   - Memory usage
   - GC time

## Troubleshooting

### Out of Memory Errors

**Symptoms**: `java.lang.OutOfMemoryError`

**Solutions**:
- Increase executor memory: `--executor-memory 16g`
- Reduce shuffle partitions: `spark.sql.shuffle.partitions=100`
- Enable dynamic allocation: `spark.dynamicAllocation.enabled=true`

### Slow Query Execution

**Symptoms**: Query takes too long

**Solutions**:
- Check data skew (use Spark UI)
- Increase number of executors
- Adjust shuffle partitions
- Enable adaptive execution (already enabled)
- Consider caching intermediate results

### Missing Data

**Symptoms**: Results have fewer rows than expected

**Solutions**:
- Check outlier filtering is not too aggressive
- Verify minimum events threshold (500)
- Check input data path is correct
- Verify data has required columns

### S3 Access Issues

**Symptoms**: Cannot read from S3

**Solutions**:
- Verify AWS credentials
- Check S3 bucket permissions
- Verify S3 path is correct
- Check network connectivity

## Example Output

```csv
device_type,event_date,percentile_95,mean_duration,stddev_duration,total_events,distinct_devices
sensor_A,2024-01-15,4.523,2.345,1.234,1250,45
sensor_A,2024-01-16,4.678,2.412,1.267,1320,48
sensor_B,2024-01-15,3.891,2.123,1.089,890,32
camera,2024-01-15,5.234,2.789,1.456,2100,15
```

## Next Steps

After running the query:
1. Validate output using `validate_output.py`
2. Analyze results for insights
3. Set up scheduled execution (cron, Airflow, etc.)
4. Integrate with reporting tools
5. Set up alerts for anomalies
