"""
Spark Query: 95th Percentile of event_duration per device type per day

Requirements:
- Compute 95th percentile of event_duration per device type per day
- Exclude outliers (outside 3 standard deviations from daily mean)
- Only include device types with at least 500 distinct events per day
- Output to CSV
"""

import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, stddev, 
    abs as abs_spark, percentile_approx, to_date, 
)
from pyspark.sql.types import DoubleType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "streaming-output")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw-events")  # Path to raw events if separate
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")  # For MinIO or S3-compatible services
INPUT_PATH = os.getenv("INPUT_PATH", "")  # Override: full S3 path or local path
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/percentile_results")  # Local output path
OUTPUT_S3_PREFIX = os.getenv("OUTPUT_S3_PREFIX", "")  # S3/MinIO prefix for output (e.g., "query-results")
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "csv")  # csv or parquet
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
MIN_EVENTS_PER_DAY = int(os.getenv("MIN_EVENTS_PER_DAY", "500"))
PERCENTILE = float(os.getenv("PERCENTILE", "0.95"))
OUTLIER_STD_DEVIATIONS = float(os.getenv("OUTLIER_STD_DEVIATIONS", "3.0"))


def create_spark_session():
    """Create Spark session with optimized configuration for percentile queries"""
    spark_builder = SparkSession.builder \
        .appName("PercentileQuery") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.files.openCostInBytes", "4194304")
    
    # Configure S3/MinIO access if credentials provided
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        spark_builder = spark_builder \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        # Configure endpoint for MinIO or S3-compatible services
        if S3_ENDPOINT:
            spark_builder = spark_builder \
                .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            logger.info(f"S3/MinIO configured with endpoint: {S3_ENDPOINT}")
        else:
            # AWS S3 default configuration
            spark_builder = spark_builder \
                .config("spark.hadoop.fs.s3a.path.style.access", "false") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            logger.info("S3 configured for AWS S3")
    
    # Add S3 dependencies
    spark_builder = spark_builder \
        .config("spark.jars.packages",
               "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created with optimized configuration")
    return spark


def read_data(spark, input_path):
    """Read data from S3 or local filesystem"""
    logger.info(f"Reading data from: {input_path}")
    
    try:
        # Try to read as Parquet first (aggregated data)
        df = spark.read.parquet(input_path)
        logger.info(f"✓ Successfully read {df.count()} records from Parquet")
        return df
    except Exception as e:
        logger.warning(f"Failed to read as Parquet: {str(e)}")
        logger.info("Attempting to read as JSON (raw events)...")
        
        try:
            # Try reading as JSON (raw events)
            df = spark.read.json(input_path)
            logger.info(f"✓ Successfully read {df.count()} records from JSON")
            return df
        except Exception as e2:
            logger.error(f"Failed to read data: {str(e2)}")
            raise


def prepare_event_data(df):
    """Prepare event data with event_duration and date columns"""
    logger.info("Preparing event data...")
    
    # Check if we have aggregated data or raw events
    has_event_duration = "event_duration" in df.columns
    has_avg_duration = "avg_duration" in df.columns
    
    if has_event_duration:
        # Raw events - use event_duration directly
        logger.info("Using raw event data with event_duration")
        prepared_df = df.select(
            col("event_duration"),
            col("device_type"),
            col("event_timestamp").alias("timestamp"),
            col("device_id")
        )
    elif has_avg_duration:
        # Aggregated data - we need to reconstruct or use avg_duration as approximation
        logger.warning("Using aggregated data - event_duration not available")
        logger.warning("Using avg_duration as approximation (note: this is not ideal for percentiles)")
        prepared_df = df.select(
            col("avg_duration").alias("event_duration"),
            col("device_type"),
            col("window_start").alias("timestamp"),
            col("device_id")
        )
    else:
        raise ValueError("Data must contain either 'event_duration' or 'avg_duration' column")
    
    # Extract date from timestamp
    prepared_df = prepared_df.withColumn(
        "event_date",
        to_date(col("timestamp"))
    )
    
    # Filter out null values
    prepared_df = prepared_df.filter(
        col("event_duration").isNotNull() &
        col("device_type").isNotNull() &
        col("event_date").isNotNull() &
        (col("event_duration") > 0)  # Only positive durations
    )
    
    logger.info(f"Prepared {prepared_df.count()} valid events")
    return prepared_df


def filter_outliers(df):
    """Filter outliers using 3 standard deviations from daily mean"""
    logger.info("Filtering outliers (outside 3 standard deviations)...")
    
    # Calculate daily statistics per device type
    daily_stats = df.groupBy("device_type", "event_date").agg(
        avg("event_duration").alias("daily_mean"),
        stddev("event_duration").alias("daily_stddev"),
        countDistinct("device_id").alias("distinct_events")
    )
    
    # Join with original data to filter outliers
    df_with_stats = df.join(
        daily_stats,
        on=["device_type", "event_date"],
        how="inner"
    )
    
    # Filter outliers: keep only events within 3 standard deviations
    filtered_df = df_with_stats.filter(
        abs_spark(col("event_duration") - col("daily_mean")) <= (OUTLIER_STD_DEVIATIONS * col("daily_stddev"))
    )
    
    # Also filter out records where stddev is null (only one event)
    filtered_df = filtered_df.filter(col("daily_stddev").isNotNull())
    
    logger.info(f"After outlier filtering: {filtered_df.count()} events")
    return filtered_df, daily_stats


def filter_by_event_count(daily_stats):
    """Filter device types with at least MIN_EVENTS_PER_DAY distinct events per day"""
    logger.info(f"Filtering device types with at least {MIN_EVENTS_PER_DAY} distinct events per day...")
    
    filtered_stats = daily_stats.filter(
        col("distinct_events") >= MIN_EVENTS_PER_DAY
    )
    
    logger.info(f"Device type-days meeting criteria: {filtered_stats.count()}")
    return filtered_stats


def compute_percentile(df, daily_stats_filtered):
    """Compute 95th percentile per device type per day"""
    logger.info(f"Computing {PERCENTILE*100}th percentile per device type per day...")
    
    # Join filtered stats to get only qualifying device type-days
    df_filtered = df.join(
        daily_stats_filtered.select("device_type", "event_date"),
        on=["device_type", "event_date"],
        how="inner"
    )
    
    # Compute percentile per device type per day
    result = df_filtered.groupBy("device_type", "event_date").agg(
        percentile_approx("event_duration", PERCENTILE, 10000).alias("percentile_95"),
        avg("event_duration").alias("mean_duration"),
        stddev("event_duration").alias("stddev_duration"),
        count("*").alias("total_events"),
        countDistinct("device_id").alias("distinct_devices")
    ).orderBy("device_type", "event_date")
    
    logger.info(f"Computed percentiles for {result.count()} device type-days")
    return result


def write_results(df, output_path, output_format="csv", use_s3=False, s3_bucket="", s3_prefix=""):
    """Write results to CSV or Parquet (local or S3/MinIO)"""
    logger.info(f"Writing results to {output_format} format...")
    
    # Determine if writing to S3 or local
    if use_s3 and s3_bucket and s3_prefix:
        full_output_path = f"s3a://{s3_bucket}/{s3_prefix}"
        logger.info(f"Writing to S3/MinIO: {full_output_path}")
    else:
        full_output_path = output_path
        logger.info(f"Writing to local path: {full_output_path}")
    
    if output_format.lower() == "csv":
        # Write as single CSV file (coalesce to 1 partition)
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(full_output_path)
        
        logger.info(f"✓ Results written to: {full_output_path}")
        if use_s3:
            logger.info("Note: CSV files are partitioned. Look for part-00000-*.csv in S3/MinIO")
        else:
            logger.info("Note: CSV files are partitioned. Look for part-00000-*.csv in the directory")
    else:
        # Write as Parquet
        df.write \
            .mode("overwrite") \
            .parquet(full_output_path)
        
        logger.info(f"✓ Results written to: {full_output_path}")


def main():
    """Main query execution"""
    logger.info("=" * 60)
    logger.info("95th Percentile Query - Event Duration Analysis")
    logger.info("=" * 60)
    logger.info(f"Percentile: {PERCENTILE*100}%")
    logger.info(f"Outlier threshold: {OUTLIER_STD_DEVIATIONS} standard deviations")
    logger.info(f"Minimum events per day: {MIN_EVENTS_PER_DAY}")
    logger.info("")
    
    spark = None
    
    try:
        # Determine input path
        if INPUT_PATH:
            input_path = INPUT_PATH
        elif S3_BUCKET:
            # Try raw events first, then aggregated
            raw_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}"
            agg_path = f"s3a://{S3_BUCKET}/{S3_PREFIX}"
            
            # For this query, we need raw events with individual event_duration
            # If raw events are in a different location, use S3_RAW_PREFIX
            input_path = raw_path if S3_RAW_PREFIX != "raw-events" else agg_path
            logger.info(f"Using S3 path: {input_path}")
        else:
            logger.error("Either INPUT_PATH or S3_BUCKET must be set")
            sys.exit(1)
        
        # Create Spark session
        spark = create_spark_session()
        
        # Read data
        df = read_data(spark, input_path)
        
        # Prepare event data
        event_df = prepare_event_data(df)
        
        # Filter outliers
        filtered_df, daily_stats = filter_outliers(event_df)
        
        # Filter by event count
        daily_stats_filtered = filter_by_event_count(daily_stats)
        
        # Compute percentile
        result_df = compute_percentile(filtered_df, daily_stats_filtered)
        
        # Show sample results
        logger.info("")
        logger.info("Sample results (first 10 rows):")
        result_df.show(10, truncate=False)
        
        # Determine output location (S3 or local)
        use_s3_output = bool(S3_BUCKET and OUTPUT_S3_PREFIX)
        
        if use_s3_output:
            logger.info("")
            logger.info(f"Output will be written to S3/MinIO: s3a://{S3_BUCKET}/{OUTPUT_S3_PREFIX}")
        else:
            logger.info("")
            logger.info(f"Output will be written to local path: {OUTPUT_PATH}")
        
        # Write results
        write_results(
            result_df, 
            OUTPUT_PATH, 
            OUTPUT_FORMAT,
            use_s3=use_s3_output,
            s3_bucket=S3_BUCKET,
            s3_prefix=OUTPUT_S3_PREFIX
        )
        
        # Summary statistics
        logger.info("")
        logger.info("Summary Statistics:")
        result_df.select(
            count("*").alias("total_device_type_days"),
            avg("percentile_95").alias("avg_percentile_95"),
            avg("mean_duration").alias("avg_mean_duration"),
            avg("total_events").alias("avg_events_per_day")
        ).show(truncate=False)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("Query completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Query failed: {str(e)}", exc_info=True)
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
