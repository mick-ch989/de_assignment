"""
Integration tests for Spark Streaming Job
These tests require a running Spark cluster and may take longer to execute
AI Generated
"""

import pytest
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def integration_spark():
    """Spark session for integration tests"""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("integration_test") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/integration_checkpoint") \
        .getOrCreate()
    yield spark
    spark.stop()


class TestEndToEndProcessing:
    """End-to-end integration tests"""

    @pytest.mark.skip(reason="Requires Kafka and Spark cluster")
    def test_full_pipeline(self, integration_spark):
        """Test the complete ETL pipeline with real data"""
        # This test would require:
        # 1. Running Kafka with test data
        # 2. Running Spark cluster
        # 3. Executing the full pipeline
        # 4. Verifying output
        pass

    def test_schema_validation(self, integration_spark):
        """Test that schema correctly validates data"""
        import spark_streaming_job as stream_job

        # Valid data
        valid_data = {
            "event_id": 123456,
            "device_id": "dev_42",
            "device_type": "sensor_A",
            "event_time": "2024-01-15T10:30:45.123456+00:00",
            "event_duration": 2.345,
            "location": {
                "latitude": 40.712776,
                "longitude": -74.005974,
                "city": "New York",
                "country": "United States"
            },
            "metadata": {
                "firmware_version": "3.2.15",
                "battery_level": 85,
                "signal_strength": -75
            }
        }

        # Create DataFrame with JSON string
        data = [(json.dumps(valid_data),)]
        schema = StructType([StructField("value", StringType(), True)])
        df = integration_spark.createDataFrame(data, schema)

        # Parse JSON
        from pyspark.sql.functions import from_json, col
        parsed = df.select(from_json(col("value"), stream_job.EVENT_SCHEMA).alias("data"))

        # Verify parsing succeeded
        result = parsed.select("data.*").first()
        assert result is not None
        assert result.event_id == 123456
        assert result.device_id == "dev_42"

    def test_filtering_logic(self, integration_spark):
        """Test filtering logic with various data scenarios"""
        import spark_streaming_job as stream_job
        from pyspark.sql.functions import col, to_timestamp

        # Create test data with mix of valid and invalid records
        test_cases = [
            # (event_id, duration, battery, latitude, should_pass)
            (1, 2.5, 85, 40.0, True),  # Valid
            (2, -1.0, 85, 40.0, False),  # Invalid: negative duration
            (3, 2.5, 150, 40.0, False),  # Invalid: battery > 100
            (4, 2.5, 85, 200.0, False),  # Invalid: latitude out of range
            (5, 2.5, 85, 40.0, True),  # Valid
        ]

        data = []
        for event_id, duration, battery, lat, _ in test_cases:
            data.append((
                event_id,
                f"dev_{event_id}",
                "sensor_A",
                datetime.now().isoformat(),
                duration,
                lat,
                -74.0,
                "New York",
                "USA",
                "1.0.0",
                battery,
                -75,
                datetime.now()
            ))

        schema = StructType([
            StructField("event_id", IntegerType(), False),
            StructField("device_id", StringType(), False),
            StructField("device_type", StringType(), False),
            StructField("event_time_str", StringType(), False),
            StructField("event_duration", DoubleType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("city", StringType(), False),
            StructField("country", StringType(), False),
            StructField("firmware_version", StringType(), False),
            StructField("battery_level", IntegerType(), False),
            StructField("signal_strength", IntegerType(), False),
            StructField("event_timestamp", StringType(), True)
        ])

        df = integration_spark.createDataFrame(data, schema)
        df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))

        filtered_df = stream_job.filter_valid_records(df)

        # Should have 2 valid records
        assert filtered_df.count() == 2
        valid_ids = [row.event_id for row in filtered_df.select("event_id").collect()]
        assert 1 in valid_ids
        assert 5 in valid_ids


class TestPerformance:
    """Performance and load tests"""

    @pytest.mark.skip(reason="Performance test - run manually")
    def test_large_batch_processing(self, integration_spark):
        """Test processing of large batches"""
        # Generate large dataset
        # Measure processing time
        # Verify no memory issues
        pass

    @pytest.mark.skip(reason="Performance test - run manually")
    def test_window_aggregation_performance(self, integration_spark):
        """Test window aggregation performance"""
        # Test with various window sizes
        # Measure aggregation time
        pass

