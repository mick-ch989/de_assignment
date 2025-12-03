# Fault Tolerance and Error Handling

## Overview

The Spark Structured Streaming job implements comprehensive fault tolerance and error handling mechanisms to ensure reliable data processing.

## Fault Tolerance Mechanisms

### 1. Checkpointing

- **Location**: Configurable via `CHECKPOINT_LOCATION` environment variable
- **Purpose**: Stores streaming state, offsets, and metadata
- **Recovery**: On restart, job resumes from last checkpoint
- **Configuration**: 
  ```python
  .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
  ```

### 2. Kafka Offset Management

- **Starting Offsets**: `latest` (can be changed to `earliest` for reprocessing)
- **Fail on Data Loss**: Set to `false` to prevent job failure on Kafka issues
- **Max Offsets Per Trigger**: Limits batch size (default: 1000)
- **Configuration**:
  ```python
  .option("failOnDataLoss", "false")
  .option("maxOffsetsPerTrigger", 1000)
  ```

### 3. Watermarking

- **Window Duration**: 1 minute tumbling windows
- **Watermark Delay**: 10 minutes
- **Purpose**: Handles late-arriving data and manages state
- **Implementation**:
  ```python
  .withWatermark("event_timestamp", "10 minutes")
  ```

### 4. Graceful Shutdown

- **Configuration**: `spark.sql.streaming.stopGracefullyOnShutdown=true`
- **Behavior**: Waits for current batch to complete before stopping
- **Signal Handling**: Catches KeyboardInterrupt and SIGTERM

## Error Handling

### 1. Connection Errors

**Kafka Connection Failures**:
- Logged with ERROR level
- Job will fail but can be restarted
- Checkpoint allows recovery from last successful offset

**Retry Strategy**:
```python
try:
    kafka_df = read_from_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
except Exception as e:
    logger.error(f"Failed to read from Kafka: {str(e)}")
    raise  # In production, implement retry logic
```

### 2. Data Quality Errors

**JSON Parsing Errors**:
- Invalid JSON is filtered out (null in parsed_data)
- Schema validation catches type mismatches
- Logged for monitoring

**Filtering Invalid Records**:
- Null checks for all required fields
- Range validation (battery level, signal strength, coordinates)
- Invalid durations filtered out

### 3. Processing Errors

**Streaming Query Exceptions**:
- Caught with `StreamingQueryException`
- Logged with full error context
- Can implement retry logic in production

**Analysis Exceptions**:
- Schema or SQL errors caught separately
- Provides clear error messages for debugging

### 4. State Management Errors

**Checkpoint Corruption**:
- Clear checkpoint directory if schema changes
- Monitor checkpoint directory size
- Backup checkpoint location for critical jobs

## Retry Mechanisms

### Current Implementation

1. **Automatic Recovery**: Spark automatically retries failed tasks
2. **Checkpoint Recovery**: Job resumes from checkpoint on restart
3. **Graceful Degradation**: Invalid records are filtered, not causing job failure

### Recommended Enhancements

For production, consider implementing:

1. **Exponential Backoff Retry**:
   ```python
   import time
   
   def retry_with_backoff(func, max_retries=3, backoff_factor=2):
       for attempt in range(max_retries):
           try:
               return func()
           except Exception as e:
               if attempt == max_retries - 1:
                   raise
               wait_time = backoff_factor ** attempt
               logger.warning(f"Retry {attempt + 1}/{max_retries} after {wait_time}s")
               time.sleep(wait_time)
   ```

2. **Dead Letter Queue**: Send failed records to a separate topic for manual review

3. **Circuit Breaker**: Temporarily stop processing if error rate exceeds threshold

4. **Health Checks**: Periodic health check endpoints for monitoring

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Processing Lag**: Kafka consumer lag
2. **Error Rate**: Number of failed records per minute
3. **Checkpoint Size**: Monitor checkpoint directory growth
4. **Processing Time**: Time per batch
5. **Throughput**: Records processed per second

### Logging

- **INFO**: Normal operations, batch completion
- **WARN**: Recoverable errors, filtered records
- **ERROR**: Critical failures, connection issues

### Alerting Recommendations

- Alert on: Consumer lag > threshold
- Alert on: Error rate > threshold
- Alert on: Job not processing for > X minutes
- Alert on: Checkpoint size > threshold

## Best Practices

1. **Always use checkpoints** for stateful operations
2. **Set appropriate watermarks** based on data latency requirements
3. **Monitor checkpoint location** disk space
4. **Test failure scenarios** (Kafka down, network issues)
5. **Implement idempotent operations** where possible
6. **Use structured logging** for easier debugging
7. **Set resource limits** to prevent OOM errors
8. **Regular checkpoint cleanup** (archive old checkpoints)

## Recovery Procedures

### Job Restart After Failure

1. Check logs for error cause
2. Verify Kafka connectivity
3. Verify checkpoint directory exists and is accessible
4. Restart job - it will resume from checkpoint
5. Monitor first few batches for issues

### Schema Changes

1. Stop the streaming job
2. Clear checkpoint directory (or use new location)
3. Update schema in code
4. Restart job with new checkpoint location

### Data Reprocessing

1. Change `startingOffsets` to `earliest`
2. Use new checkpoint location
3. Let job process all historical data
4. Monitor processing time and resources

## Testing Fault Tolerance

### Test Scenarios

1. **Kafka Down**: Stop Kafka, verify job handles gracefully
2. **Network Partition**: Simulate network issues
3. **Invalid Data**: Send malformed JSON, verify filtering
4. **High Volume**: Test with high message rate
5. **Checkpoint Corruption**: Simulate checkpoint issues
6. **Resource Exhaustion**: Test with limited resources

### Load Testing

- Test with various message rates
- Test with different window sizes
- Test with late-arriving data
- Test checkpoint recovery time

