"""
Spark Structured Streaming Job for ETL Processing
Consumes from Kafka, performs transformations, aggregations, and writes results
"""

import os
import json
import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession

# Try to import boto3 for AWS credentials
try:
    import boto3

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    avg,
    count,
    max as max_spark,
    min as min_spark,
    first,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
from pyspark.sql.utils import AnalysisException
from pyspark.errors import StreamingQueryException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_aws_credentials():
    """Get AWS credentials from boto3 or environment variables"""
    access_key = None
    secret_key = None

    # First try environment variables (explicit)
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("S3_SECRET_KEY")

    # If not in env vars, try boto3 (uses ~/.aws/credentials, IAM roles, etc.)
    if not access_key or not secret_key:
        if BOTO3_AVAILABLE:
            try:
                session = boto3.Session()
                credentials = session.get_credentials()
                if credentials:
                    access_key = credentials.access_key
                    secret_key = credentials.secret_key
                    logger.info("AWS credentials loaded from boto3")
            except Exception as e:
                logger.warning(f"Failed to get AWS credentials from boto3: {str(e)}")
        else:
            logger.debug("boto3 not available, using environment variables only")

    return access_key, secret_key


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "input_events")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "1000"))
KAFKA_FAIL_ON_DATA_LOSS = os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "false").lower() == "true"

CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/streaming_checkpoint")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/streaming_output")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")

S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "streaming-output")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")  # For MinIO or S3-compatible services

AWS_ACCESS_KEY, AWS_SECRET_KEY = get_aws_credentials()

SPARK_ADAPTIVE_ENABLED = os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "true").lower() == "true"
SPARK_ADAPTIVE_COALESCE = os.getenv("SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED", "true").lower() == "true"
SPARK_SCHEMA_INFERENCE = os.getenv("SPARK_SQL_STREAMING_SCHEMA_INFERENCE", "true").lower() == "true"
SPARK_GRACEFUL_SHUTDOWN = os.getenv("SPARK_SQL_STREAMING_STOP_GRACEFULLY_ON_SHUTDOWN", "true").lower() == "true"
SPARK_SERIALIZER = os.getenv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer")
SPARK_MIN_BATCHES_TO_RETAIN = int(os.getenv("SPARK_SQL_STREAMING_MIN_BATCHES_TO_RETAIN", "10"))

EVENT_SCHEMA = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("device_id", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("event_duration", DoubleType(), False),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("city", StringType(), False),
        StructField("country", StringType(), False)
    ]), False),
    StructField("metadata", StructType([
        StructField("firmware_version", StringType(), False),
        StructField("battery_level", IntegerType(), False),
        StructField("signal_strength", IntegerType(), False)
    ]), False)
])


def create_spark_session():
    """Create and configure Spark session with fault tolerance"""
    try:
        builder = SparkSession.builder.appName("StreamingETLJob")

        builder = builder.config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
        builder = builder.config("spark.sql.streaming.schemaInference", str(SPARK_SCHEMA_INFERENCE).lower())
        builder = builder.config("spark.sql.adaptive.enabled", str(SPARK_ADAPTIVE_ENABLED).lower())
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", str(SPARK_ADAPTIVE_COALESCE).lower())
        builder = builder.config("spark.serializer", SPARK_SERIALIZER)
        builder = builder.config("spark.sql.streaming.stopGracefullyOnShutdown", str(SPARK_GRACEFUL_SHUTDOWN).lower())
        builder = builder.config("spark.sql.streaming.minBatchesToRetain", str(SPARK_MIN_BATCHES_TO_RETAIN))

        # Configure S3/MinIO if credentials are available
        if AWS_ACCESS_KEY and AWS_SECRET_KEY:
            builder = builder.config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            builder = builder.config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
            builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            # Configure endpoint for MinIO or S3-compatible services
            if S3_ENDPOINT:
                builder = builder.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
                builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
                builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                logger.info(f"S3/MinIO configured with endpoint: {S3_ENDPOINT}")
            else:
                # AWS S3 default configuration
                builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "false")
                builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
                logger.info("S3 configured for AWS S3")
        elif S3_BUCKET:
            # If S3 bucket is set but no explicit credentials, try default AWS credential chain
            builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.DefaultAWSCredentialsProviderChain")
            if S3_ENDPOINT:
                builder = builder.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
                builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
            logger.info("S3 configured to use default AWS credential chain")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        logger.info(f"Using checkpoint location: {CHECKPOINT_LOCATION}")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise


def read_from_kafka(spark, kafka_servers, topic):
    """Read stream from Kafka with error handling"""
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
            .option("failOnDataLoss", str(KAFKA_FAIL_ON_DATA_LOSS).lower()) \
            .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER) \
            .load()

        logger.info(f"Successfully connected to Kafka topic: {topic}")
        logger.info(
            f"Kafka configuration: startingOffsets={KAFKA_STARTING_OFFSETS}, maxOffsetsPerTrigger={KAFKA_MAX_OFFSETS_PER_TRIGGER}")
        return df
    except Exception as e:
        logger.error(f"Failed to read from Kafka: {str(e)}")
        raise


def parse_json_data(df):
    """Parse JSON data from Kafka messages"""
    try:
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_value"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset")
        )

        parsed_df = parsed_df.withColumn(
            "parsed_data",
            from_json(col("json_value"), EVENT_SCHEMA)
        )

        flattened_df = parsed_df.select(
            col("kafka_timestamp"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("parsed_data.event_id").alias("event_id"),
            col("parsed_data.device_id").alias("device_id"),
            col("parsed_data.device_type").alias("device_type"),
            col("parsed_data.event_time").alias("event_time_str"),
            col("parsed_data.event_duration").alias("event_duration"),
            col("parsed_data.location.latitude").alias("latitude"),
            col("parsed_data.location.longitude").alias("longitude"),
            col("parsed_data.location.city").alias("city"),
            col("parsed_data.location.country").alias("country"),
            col("parsed_data.metadata.firmware_version").alias("firmware_version"),
            col("parsed_data.metadata.battery_level").alias("battery_level"),
            col("parsed_data.metadata.signal_strength").alias("signal_strength")
        )

        flattened_df = flattened_df.withColumn(
            "event_timestamp",
            to_timestamp(col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
        )

        logger.info("JSON parsing completed")
        return flattened_df

    except Exception as e:
        logger.error(f"Error parsing JSON: {str(e)}")
        raise


def filter_valid_records(df):
    """Filter out invalid records"""
    try:
        filtered_df = df.filter(
            col("event_id").isNotNull() &
            col("device_id").isNotNull() &
            col("device_type").isNotNull() &
            col("event_timestamp").isNotNull() &
            col("event_duration").isNotNull() &
            (col("event_duration") > 0) &
            (col("event_duration") < 100) &
            (col("battery_level") >= 0) &
            (col("battery_level") <= 100) &
            (col("signal_strength") >= -120) &
            (col("signal_strength") <= 0) &
            (col("latitude").isNotNull()) &
            (col("longitude").isNotNull()) &
            (col("latitude") >= -90) &
            (col("latitude") <= 90) &
            (col("longitude") >= -180) &
            (col("longitude") <= 180)
        )

        logger.info("Filtering completed")
        return filtered_df

    except Exception as e:
        logger.error(f"Error filtering records: {str(e)}")
        raise


def deduplicate_records(df):
    """Remove duplicate records based on event_id within watermark"""
    try:
        # Use watermark for deduplication
        deduplicated_df = df \
            .withWatermark("event_timestamp", WATERMARK_DELAY) \
            .dropDuplicates(["event_id", "device_id"])

        logger.info(f"Deduplication completed with watermark: {WATERMARK_DELAY}")
        return deduplicated_df

    except Exception as e:
        logger.error(f"Error in deduplication: {str(e)}")
        raise


def perform_aggregations(df):
    """Perform windowed aggregations per device per minute"""
    try:
        # Create tumbling windows based on configuration
        windowed_df = df \
            .withWatermark("event_timestamp", WATERMARK_DELAY) \
            .groupBy(
            window(col("event_timestamp"), WINDOW_DURATION),
            col("device_id"),
            col("device_type")
        ) \
            .agg(
            count("*").alias("event_count"),
            avg("event_duration").alias("avg_duration"),
            max_spark("event_duration").alias("max_duration"),
            min_spark("event_duration").alias("min_duration"),
            avg("battery_level").alias("avg_battery_level"),
            min_spark("battery_level").alias("min_battery_level"),
            avg("signal_strength").alias("avg_signal_strength"),
            min_spark("signal_strength").alias("min_signal_strength"),
            first("city").alias("city"),
            first("country").alias("country"),
            first("firmware_version").alias("firmware_version"),
            max_spark("event_timestamp").alias("last_event_time")
        ) \
            .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device_id"),
            col("device_type"),
            col("event_count"),
            col("avg_duration"),
            col("max_duration"),
            col("min_duration"),
            col("avg_battery_level"),
            col("min_battery_level"),
            col("avg_signal_strength"),
            col("min_signal_strength"),
            col("city"),
            col("country"),
            col("firmware_version"),
            col("last_event_time")
        )

        logger.info("Aggregations completed")
        return windowed_df

    except Exception as e:
        logger.error(f"Error in aggregations: {str(e)}")
        raise


def write_to_sink(df, output_path, s3_bucket=""):
    """Write aggregated results to sink (S3 or local filesystem)"""
    try:
        if s3_bucket:
            full_path = f"s3a://{s3_bucket}/{S3_PREFIX}"
            write_query = df.writeStream \
                .outputMode("update") \
                .format("parquet") \
                .option("path", full_path) \
                .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/s3") \
                .partitionBy("device_type", "window_start") \
                .trigger(processingTime="1 minute") \
                .start()
        else:
            write_query = df.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 100) \
                .trigger(processingTime="1 minute") \
                .start()

        logger.info("Write stream started")
        return write_query

    except Exception as e:
        logger.error(f"Error writing to sink: {str(e)}")
        raise


def main():
    """Main streaming job"""
    spark = None
    query = None

    try:
        logger.info("Starting Spark Streaming ETL Job")
        logger.info(f"Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Kafka topic: {KAFKA_TOPIC}")
        logger.info(f"Window duration: {WINDOW_DURATION}")
        logger.info(f"Watermark delay: {WATERMARK_DELAY}")
        if S3_BUCKET:
            logger.info(f"S3 bucket: {S3_BUCKET}, prefix: {S3_PREFIX}")


        spark = create_spark_session()
        kafka_df = read_from_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
        parsed_df = parse_json_data(kafka_df)
        filtered_df = filter_valid_records(parsed_df)
        deduplicated_df = deduplicate_records(filtered_df)
        aggregated_df = perform_aggregations(deduplicated_df)
        query = write_to_sink(aggregated_df, OUTPUT_PATH, S3_BUCKET)
        logger.info("Streaming query started. Waiting for termination...")
        query.awaitTermination()

    except StreamingQueryException as e:
        logger.error(f"Streaming query error: {str(e)}")
        logger.info("Attempting to restart query...")
        # In production, implement retry logic here
        raise

    except AnalysisException as e:
        logger.error(f"Analysis error: {str(e)}")
        raise

    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Stopping gracefully...")
        if query:
            query.stop()

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise

    finally:
        if spark:
            logger.info("Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
