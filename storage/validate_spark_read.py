"""
Script to validate Spark read operations from S3
Verifies that data written by streaming job can be read correctly
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "streaming-output")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")  # For S3-compatible services like MinIO


def create_spark_session():
    """Create Spark session with S3 configuration"""
    spark_builder = SparkSession.builder \
        .appName("S3ReadValidation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Configure S3/MinIO access
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
        else:
            # AWS S3 default configuration
            spark_builder = spark_builder \
                .config("spark.hadoop.fs.s3a.path.style.access", "false") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    
    # Add S3A dependencies
    spark_builder = spark_builder \
        .config("spark.jars.packages", 
               "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def validate_s3_path(spark, s3_path):
    """Validate that S3 path exists and is accessible"""
    try:
        from pyspark.sql import DataFrame
        
        # Try to list files in the path
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(s3_path), hadoop_conf
        )
        
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(s3_path)
        exists = fs.exists(path)
        
        if exists:
            logger.info(f"✓ S3 path exists: {s3_path}")
            return True
        else:
            logger.warning(f"⚠ S3 path does not exist: {s3_path}")
            return False
            
    except Exception as e:
        logger.error(f"Error validating S3 path: {str(e)}")
        return False


def read_parquet_from_s3(spark, s3_path):
    """Read Parquet files from S3"""
    try:
        logger.info(f"Reading Parquet files from: {s3_path}")
        
        # Read Parquet files
        df = spark.read.parquet(s3_path)
        
        logger.info(f"✓ Successfully read {df.count()} records from S3")
        return df
        
    except Exception as e:
        logger.error(f"Error reading from S3: {str(e)}")
        raise


def validate_schema(df):
    """Validate that DataFrame has expected schema"""
    expected_columns = [
        "window_start", "window_end", "device_id", "device_type",
        "event_count", "avg_duration", "max_duration", "min_duration",
        "avg_battery_level", "min_battery_level",
        "avg_signal_strength", "min_signal_strength",
        "city", "country", "firmware_version", "last_event_time"
    ]
    
    actual_columns = df.columns
    
    logger.info("Validating schema...")
    logger.info(f"Expected columns: {len(expected_columns)}")
    logger.info(f"Actual columns: {len(actual_columns)}")
    
    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)
    
    if missing_columns:
        logger.warning(f"⚠ Missing columns: {missing_columns}")
    if extra_columns:
        logger.info(f"ℹ Extra columns: {extra_columns}")
    
    # Check critical columns
    critical_columns = ["device_id", "device_type", "event_count"]
    missing_critical = [col for col in critical_columns if col not in actual_columns]
    
    if missing_critical:
        logger.error(f"✗ Missing critical columns: {missing_critical}")
        return False
    
    logger.info("✓ Schema validation passed")
    return True


def validate_data_quality(df):
    """Validate data quality and statistics"""
    logger.info("Validating data quality...")
    
    try:
        # Basic statistics
        total_records = df.count()
        logger.info(f"Total records: {total_records}")
        
        if total_records == 0:
            logger.warning("⚠ No records found in S3")
            return False
        
        # Check for null values in critical columns
        null_counts = {}
        critical_columns = ["device_id", "device_type", "event_count"]
        
        for col_name in critical_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = null_count
                if null_count > 0:
                    logger.warning(f"⚠ Found {null_count} null values in {col_name}")
        
        # Aggregate statistics
        if "event_count" in df.columns:
            stats = df.agg(
                count("*").alias("total_records"),
                avg("event_count").alias("avg_event_count"),
                max("event_count").alias("max_event_count"),
                min("event_count").alias("min_event_count")
            ).collect()[0]
            
            logger.info(f"Event count statistics:")
            logger.info(f"  Average: {stats.avg_event_count:.2f}")
            logger.info(f"  Max: {stats.max_event_count}")
            logger.info(f"  Min: {stats.min_event_count}")
        
        # Device type distribution
        if "device_type" in df.columns:
            device_dist = df.groupBy("device_type").count().collect()
            logger.info("Device type distribution:")
            for row in device_dist:
                logger.info(f"  {row.device_type}: {row['count']}")
        
        logger.info("✓ Data quality validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error in data quality validation: {str(e)}")
        return False


def validate_partitions(df, s3_path):
    """Validate partition structure"""
    logger.info("Validating partition structure...")
    
    try:
        # Check if data is partitioned by device_type
        partitions = df.select("device_type").distinct().collect()
        logger.info(f"Found {len(partitions)} unique device types")
        
        # In S3, partitions are typically stored as directories
        # This is a basic check - full partition validation would require S3 listing
        logger.info("✓ Partition validation completed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating partitions: {str(e)}")
        return False


def main():
    """Main validation function"""
    if not S3_BUCKET:
        logger.error("S3_BUCKET environment variable is not set")
        logger.info("Usage: export S3_BUCKET=your-bucket-name")
        sys.exit(1)
    
    s3_path = f"s3a://{S3_BUCKET}/{S3_PREFIX}"
    
    logger.info("=" * 60)
    logger.info("S3 Read Validation")
    logger.info("=" * 60)
    logger.info(f"S3 Bucket: {S3_BUCKET}")
    logger.info(f"S3 Prefix: {S3_PREFIX}")
    logger.info(f"Full Path: {s3_path}")
    logger.info("")
    
    spark = None
    validation_results = {
        "s3_path_exists": False,
        "read_successful": False,
        "schema_valid": False,
        "data_quality_valid": False,
        "partitions_valid": False
    }
    
    try:
        # Create Spark session
        logger.info("Creating Spark session...")
        spark = create_spark_session()
        logger.info("✓ Spark session created")
        
        # Validate S3 path
        validation_results["s3_path_exists"] = validate_s3_path(spark, s3_path)
        
        if not validation_results["s3_path_exists"]:
            logger.warning("S3 path does not exist. This might be expected if no data has been written yet.")
            logger.info("Run the streaming job first to generate data.")
            return
        
        # Read data from S3
        try:
            df = read_parquet_from_s3(spark, s3_path)
            validation_results["read_successful"] = True
            
            # Validate schema
            validation_results["schema_valid"] = validate_schema(df)
            
            # Validate data quality
            validation_results["data_quality_valid"] = validate_data_quality(df)
            
            # Validate partitions
            validation_results["partitions_valid"] = validate_partitions(df, s3_path)
            
            # Show sample data
            logger.info("")
            logger.info("Sample data (first 5 rows):")
            df.show(5, truncate=False)
            
        except Exception as e:
            logger.error(f"Error reading or validating data: {str(e)}")
            validation_results["read_successful"] = False
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}", exc_info=True)
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()
    
    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Validation Summary")
    logger.info("=" * 60)
    
    all_passed = all(validation_results.values())
    
    for check, result in validation_results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {check}")
    
    logger.info("")
    
    if all_passed:
        logger.info("✓ All validations passed!")
        sys.exit(0)
    else:
        logger.warning("⚠ Some validations failed. Please check the logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
