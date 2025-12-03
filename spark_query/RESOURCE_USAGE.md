# Resource Usage Considerations

This document provides detailed guidance on memory tuning, shuffling optimization, and performance considerations for the percentile query.

## Memory Tuning

### Driver Memory

The driver coordinates the Spark application and needs sufficient memory for:
- Storing application metadata
- Collecting small results (if using `.collect()`)
- Managing task scheduling

**Recommendations:**
- **Small datasets (< 10GB)**: 2-4GB
- **Medium datasets (10-100GB)**: 4-8GB
- **Large datasets (> 100GB)**: 8-16GB

**Configuration:**
```bash
--driver-memory 4g
```

### Executor Memory

Each executor processes data partitions. Memory is split between:
- **Execution memory**: For computations, shuffles, joins
- **Storage memory**: For caching (if used)
- **Reserved memory**: Spark overhead (~300MB)

**Recommendations:**
- **Small datasets**: 4-8GB per executor
- **Medium datasets**: 8-16GB per executor
- **Large datasets**: 16-32GB per executor

**Configuration:**
```bash
--executor-memory 8g
```

**Memory Fraction:**
```python
# Default: 60% for execution, 40% for storage
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.5")
```

### Executor Cores

Number of parallel tasks per executor.

**Recommendations:**
- **CPU-bound operations**: 2-4 cores per executor
- **I/O-bound operations**: 4-8 cores per executor

**Configuration:**
```bash
--executor-cores 4
```

### Number of Executors

Total parallelism = num_executors × executor_cores

**Recommendations:**
- **Small clusters**: 5-10 executors
- **Medium clusters**: 10-20 executors
- **Large clusters**: 20-50 executors

**Configuration:**
```bash
--num-executors 10
```

### Total Memory Calculation

**Example Configuration:**
- 10 executors × 8GB = 80GB executor memory
- 1 driver × 4GB = 4GB driver memory
- **Total**: ~84GB cluster memory

**Rule of Thumb:**
- Allocate 70-80% of cluster memory to Spark
- Leave 20-30% for OS and other services

## Shuffling Considerations

### What is Shuffling?

Shuffling occurs when data needs to be redistributed across partitions, typically during:
- `groupBy()` operations
- `join()` operations (unless broadcast join)
- `repartition()` operations
- `orderBy()` operations

### Shuffle Operations in Percentile Query

#### 1. Daily Statistics GroupBy

```python
daily_stats = df.groupBy("device_type", "event_date").agg(...)
```

**Shuffle Characteristics:**
- **Input size**: Full dataset
- **Output size**: Number of unique (device_type, event_date) combinations
- **Partitions**: `spark.sql.shuffle.partitions` (default: 200)

**Optimization:**
- If output is small (< 10MB), consider broadcasting
- Use adaptive execution to coalesce partitions
- Monitor for data skew

#### 2. Join for Outlier Filtering

```python
df_with_stats = df.join(daily_stats, on=["device_type", "event_date"])
```

**Shuffle Characteristics:**
- **Input size**: Full dataset + daily_stats
- **Output size**: Filtered dataset (smaller after outlier removal)
- **Join type**: Hash join (shuffle join)

**Optimization:**
- If `daily_stats` is small (< 10MB), use broadcast join:
  ```python
  from pyspark.sql.functions import broadcast
  df_with_stats = df.join(broadcast(daily_stats), ...)
  ```
- Enable skew join: `spark.sql.adaptive.skewJoin.enabled = true`

#### 3. Percentile GroupBy

```python
result = df_filtered.groupBy("device_type", "event_date").agg(...)
```

**Shuffle Characteristics:**
- **Input size**: Filtered dataset
- **Output size**: Number of qualifying (device_type, event_date) combinations
- **Partitions**: `spark.sql.shuffle.partitions`

**Optimization:**
- Adaptive execution automatically optimizes
- Skew join handles uneven data distribution

### Shuffle Partition Tuning

#### Default Configuration

```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

#### Tuning Guidelines

**Too Few Partitions:**
- **Symptoms**: Long-running tasks, memory pressure
- **Solution**: Increase partitions
- **Rule**: ~100-200MB per partition

**Too Many Partitions:**
- **Symptoms**: Many small tasks, overhead
- **Solution**: Decrease partitions
- **Rule**: Minimum 2x number of cores

**Formula:**
```
shuffle_partitions = (total_data_size / 200MB) × 2
```

**Examples:**
- 10GB data: 100 partitions
- 100GB data: 1000 partitions (but cap at 400-500)
- 1TB data: 400-500 partitions (capped)

### Shuffle Spill to Disk

**Symptoms:**
- Warnings: "Spilled data to disk"
- Slow query execution
- High I/O

**Causes:**
- Insufficient executor memory
- Too much data per partition
- Data skew

**Solutions:**
1. Increase executor memory
2. Increase shuffle partitions
3. Handle data skew (salting, repartitioning)
4. Enable disk-based shuffle: `spark.shuffle.spill.enabled = true`

### Data Skew Handling

**What is Skew?**
Uneven distribution of data across partitions, causing some tasks to run much longer.

**Detection:**
- Check Spark UI for task execution times
- Look for tasks taking 10x longer than others
- Monitor shuffle read/write imbalance

**Solutions:**

1. **Enable Skew Join** (already enabled):
   ```python
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
   ```

2. **Salting** (for extreme skew):
   ```python
   # Add random salt to join key
   df = df.withColumn("salt", (rand() * 10).cast("int"))
   ```

3. **Repartition**:
   ```python
   df = df.repartition("device_type", "event_date")
   ```

## Performance Optimization

### Adaptive Query Execution (AQE)

Already enabled in the query for automatic optimization:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Benefits:**
- Automatically coalesces small partitions
- Handles data skew in joins
- Optimizes shuffle partitions at runtime

### Caching Intermediate Results

If running the query multiple times, consider caching:

```python
# Cache daily_stats if reused
daily_stats.cache()
daily_stats.count()  # Trigger caching
```

**When to Cache:**
- Data reused multiple times
- Iterative algorithms
- Interactive queries

**When NOT to Cache:**
- One-time queries
- Limited memory
- Data changes frequently

### Broadcast Join Optimization

For small lookup tables:

```python
from pyspark.sql.functions import broadcast

# If daily_stats is small (< 10MB)
df_with_stats = df.join(
    broadcast(daily_stats),
    on=["device_type", "event_date"]
)
```

**Broadcast Threshold:**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
```

### File Reading Optimization

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")  # 4MB
```

**Tuning:**
- **maxPartitionBytes**: Larger = fewer partitions, more memory per partition
- **openCostInBytes**: Cost of opening a file (affects file splitting)

## Percentile Calculation

### Approximate vs Exact

**Current Implementation:**
```python
percentile_approx("event_duration", 0.95, 10000)
```

**Accuracy Parameter (10000):**
- Higher = more accurate but slower
- Lower = faster but less accurate
- 10000 provides good balance

**Memory Usage:**
- Approximate: O(n) where n = accuracy parameter
- Exact: O(n log n) where n = dataset size

**For Exact Percentile (if needed):**
```python
# More memory intensive
from pyspark.sql.functions import collect_list, expr
percentile_exact = expr("percentile_approx(event_duration, 0.95, 1000000)")
```

## Monitoring and Debugging

### Spark UI Metrics

Access at: `http://spark-master:8080`

**Key Metrics:**
1. **Stage Duration**: Time per stage
2. **Shuffle Read/Write**: Data movement size
3. **Task Execution Time**: Identify slow tasks
4. **Memory Usage**: Executor and driver memory
5. **GC Time**: Garbage collection overhead

### Log Analysis

**Warnings to Watch:**
- "Spilled data to disk" → Increase memory
- "Task serialization" → Check object size
- "Out of memory" → Increase executor memory
- "Skew detected" → Enable skew join

### Performance Profiling

```python
# Enable detailed logging
spark.sparkContext.setLogLevel("INFO")

# Check execution plan
df.explain(True)

# Check partition distribution
df.rdd.glom().map(len).collect()
```

## Recommended Configurations

### Small Dataset (< 10GB)

```bash
spark-submit \
  --master local[4] \
  --driver-memory 2g \
  --executor-memory 4g \
  --conf spark.sql.shuffle.partitions=100 \
  percentile_query.py
```

### Medium Dataset (10-100GB)

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=200 \
  percentile_query.py
```

### Large Dataset (> 100GB)

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 20 \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=400 \
  percentile_query.py
```

## Troubleshooting

### Out of Memory

**Symptoms:**
- `java.lang.OutOfMemoryError`
- Tasks failing
- Spill to disk warnings

**Solutions:**
1. Increase executor memory
2. Reduce shuffle partitions
3. Enable dynamic allocation
4. Check for memory leaks

### Slow Performance

**Symptoms:**
- Long execution time
- Many small tasks
- High shuffle overhead

**Solutions:**
1. Increase executors
2. Adjust shuffle partitions
3. Enable adaptive execution
4. Use broadcast joins where possible
5. Check for data skew

### Data Skew

**Symptoms:**
- Some tasks much slower
- Uneven partition sizes
- Shuffle imbalance

**Solutions:**
1. Enable skew join
2. Repartition by key
3. Use salting technique
4. Filter skewed data separately

## Best Practices

1. **Start Small**: Begin with default configs, tune based on metrics
2. **Monitor First**: Run query and observe Spark UI before optimizing
3. **Incremental Tuning**: Change one parameter at a time
4. **Document Changes**: Keep track of what works
5. **Test Regularly**: Validate performance after changes
6. **Use Adaptive Execution**: Let Spark optimize automatically when possible
7. **Profile Before Optimizing**: Use `explain()` to understand execution plan

