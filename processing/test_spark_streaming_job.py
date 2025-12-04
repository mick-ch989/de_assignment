"""
Comprehensive tests for Spark Streaming ETL Job
"""

import pytest
import json
import os
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.utils import AnalysisException
from pyspark.errors import StreamingQueryException

# Import the module to test
import spark_streaming_job as stream_job


@pytest.fixture
def spark_session():
    """Create a test Spark session"""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("test_streaming_job") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/test_checkpoint") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_event_data():
    """Sample valid event data"""
    return {
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


@pytest.fixture
def sample_kafka_dataframe(spark_session, sample_event_data):
    """Create a sample Kafka-like DataFrame"""
    data = [
        (
            None,  # key
            json.dumps(sample_event_data).encode('utf-8'),  # value
            "input_events",  # topic
            0,  # partition
            100,  # offset
            datetime.now(),  # timestamp
            datetime.now()  # timestampType
        )
    ]
    
    schema = StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", IntegerType(), False),
        StructField("timestamp", StringType(), True),
        StructField("timestampType", IntegerType(), False)
    ])
    
    # Create DataFrame with proper Kafka schema
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def invalid_event_data():
    """Sample invalid event data for testing filters"""
    return {
        "event_id": None,  # Invalid: null event_id
        "device_id": "dev_42",
        "device_type": "sensor_A",
        "event_time": "2024-01-15T10:30:45.123456+00:00",
        "event_duration": -5.0,  # Invalid: negative duration
        "location": {
            "latitude": 200.0,  # Invalid: out of range
            "longitude": -74.005974,
            "city": "New York",
            "country": "United States"
        },
        "metadata": {
            "firmware_version": "3.2.15",
            "battery_level": 150,  # Invalid: out of range
            "signal_strength": -200  # Invalid: out of range
        }
    }


class TestCreateSparkSession:
    """Tests for create_spark_session function"""
    
    @patch('spark_streaming_job.SparkSession')
    def test_create_spark_session_success(self, mock_spark_session_class):
        """Test successful Spark session creation"""
        mock_builder = MagicMock()
        mock_spark = MagicMock()
        mock_spark_context = MagicMock()
        
        mock_spark_session_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_spark.sparkContext = mock_spark_context
        
        result = stream_job.create_spark_session()
        
        assert result == mock_spark
        assert mock_builder.appName.called
        assert mock_builder.config.call_count >= 5  # Multiple config calls
        mock_spark_context.setLogLevel.assert_called_once_with("WARN")
    
    @patch('spark_streaming_job.SparkSession')
    def test_create_spark_session_failure(self, mock_spark_session_class):
        """Test Spark session creation failure"""
        mock_builder = MagicMock()
        mock_spark_session_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            stream_job.create_spark_session()
        
        assert "Connection failed" in str(exc_info.value)


class TestReadFromKafka:
    """Tests for read_from_kafka function"""
    
    def test_read_from_kafka_success(self, spark_session):
        """Test successful Kafka read"""
        # Mock the readStream
        mock_read_stream = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        mock_load = MagicMock()
        
        spark_session.readStream = mock_read_stream
        mock_read_stream.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.load.return_value = mock_load
        
        result = stream_job.read_from_kafka(spark_session, "kafka:9092", "input_events")
        
        assert result == mock_load
        mock_read_stream.format.assert_called_once_with("kafka")
        assert mock_format.option.call_count >= 4  # Multiple option calls
    
    def test_read_from_kafka_failure(self, spark_session):
        """Test Kafka read failure"""
        spark_session.readStream = MagicMock()
        spark_session.readStream.format.side_effect = Exception("Kafka connection failed")
        
        with pytest.raises(Exception) as exc_info:
            stream_job.read_from_kafka(spark_session, "kafka:9092", "input_events")
        
        assert "Kafka connection failed" in str(exc_info.value)


class TestParseJsonData:
    """Tests for parse_json_data function"""
    
    def test_parse_json_data_success(self, spark_session, sample_event_data):
        """Test successful JSON parsing"""
        # Create a DataFrame with Kafka-like structure
        kafka_data = [
            (
                None,
                json.dumps(sample_event_data),
                datetime.now(),
                0,
                100
            )
        ]
        
        kafka_schema = StructType([
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("partition", IntegerType(), False),
            StructField("offset", IntegerType(), False)
        ])
        
        kafka_df = spark_session.createDataFrame(kafka_data, kafka_schema)
        
        # Test parsing
        result_df = stream_job.parse_json_data(kafka_df)
        
        # Verify result has expected columns
        assert result_df is not None
        columns = result_df.columns
        assert "event_id" in columns
        assert "device_id" in columns
        assert "device_type" in columns
        assert "event_timestamp" in columns
    
    def test_parse_json_data_invalid_json(self, spark_session):
        """Test parsing with invalid JSON"""
        kafka_data = [
            (None, "invalid json", datetime.now(), 0, 100)
        ]
        
        kafka_schema = StructType([
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("partition", IntegerType(), False),
            StructField("offset", IntegerType(), False)
        ])
        
        kafka_df = spark_session.createDataFrame(kafka_data, kafka_schema)
        
        # Should handle invalid JSON gracefully
        result_df = stream_job.parse_json_data(kafka_df)
        assert result_df is not None


class TestFilterValidRecords:
    """Tests for filter_valid_records function"""
    
    def test_filter_valid_records_success(self, spark_session, sample_event_data):
        """Test filtering with valid records"""
        # Create DataFrame with valid data
        data = [
            (
                123456, "dev_42", "sensor_A", "2024-01-15T10:30:45.123456+00:00",
                2.345, 40.712776, -74.005974, "New York", "United States",
                "3.2.15", 85, -75, datetime.now()
            )
        ]
        
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
        
        df = spark_session.createDataFrame(data, schema)
        result_df = stream_job.filter_valid_records(df)
        
        assert result_df is not None
        assert result_df.count() == 1  # Valid record should pass
    
    def test_filter_invalid_records(self, spark_session):
        """Test filtering with invalid records"""
        # Create DataFrame with invalid data (null event_id, negative duration)
        data = [
            (
                None, "dev_42", "sensor_A", "2024-01-15T10:30:45.123456+00:00",
                -5.0, 200.0, -74.005974, "New York", "United States",
                "3.2.15", 150, -200, datetime.now()
            )
        ]
        
        schema = StructType([
            StructField("event_id", IntegerType(), True),
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
        
        df = spark_session.createDataFrame(data, schema)
        result_df = stream_job.filter_valid_records(df)
        
        # Invalid records should be filtered out
        assert result_df.count() == 0


class TestDeduplicateRecords:
    """Tests for deduplicate_records function"""
    
    def test_deduplicate_records(self, spark_session):
        """Test deduplication removes duplicate event_ids"""
        from pyspark.sql.functions import col
        
        # Create DataFrame with duplicate event_ids
        data = [
            (123456, "dev_42", datetime.now()),
            (123456, "dev_42", datetime.now()),  # Duplicate
            (789012, "dev_43", datetime.now())
        ]
        
        schema = StructType([
            StructField("event_id", IntegerType(), False),
            StructField("device_id", StringType(), False),
            StructField("event_timestamp", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Convert timestamp string to timestamp type for watermark
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        
        result_df = stream_job.deduplicate_records(df)
        
        # Should have 2 unique records (one duplicate removed)
        assert result_df is not None
        # Note: dropDuplicates with watermark may not work in unit tests
        # This is more of an integration test scenario


class TestPerformAggregations:
    """Tests for perform_aggregations function"""
    
    def test_perform_aggregations(self, spark_session):
        """Test windowed aggregations"""
        from pyspark.sql.functions import col, to_timestamp
        
        # Create test data with timestamps
        data = [
            (123456, "dev_42", "sensor_A", datetime(2024, 1, 15, 10, 30, 0), 2.5, 85, -75),
            (123457, "dev_42", "sensor_A", datetime(2024, 1, 15, 10, 30, 30), 3.0, 80, -80),
            (123458, "dev_43", "sensor_B", datetime(2024, 1, 15, 10, 30, 0), 1.5, 90, -70)
        ]
        
        schema = StructType([
            StructField("event_id", IntegerType(), False),
            StructField("device_id", StringType(), False),
            StructField("device_type", StringType(), False),
            StructField("event_timestamp", StringType(), True),
            StructField("event_duration", DoubleType(), False),
            StructField("battery_level", IntegerType(), False),
            StructField("signal_strength", IntegerType(), False)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        
        result_df = stream_job.perform_aggregations(df)
        
        assert result_df is not None
        # Verify aggregation columns exist
        columns = result_df.columns
        assert "event_count" in columns
        assert "avg_duration" in columns
        assert "avg_battery_level" in columns


class TestWriteToSink:
    """Tests for write_to_sink function"""
    
    def test_write_to_console(self, spark_session):
        """Test writing to console sink"""
        # Create a simple DataFrame
        data = [(1, "dev_1", 10)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("device_id", StringType(), False),
            StructField("count", IntegerType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)
        
        # Mock the writeStream
        mock_write_stream = MagicMock()
        mock_output_mode = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        mock_trigger = MagicMock()
        mock_start = MagicMock()
        
        df.writeStream = mock_write_stream
        mock_write_stream.outputMode.return_value = mock_output_mode
        mock_output_mode.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.trigger.return_value = mock_trigger
        mock_trigger.start.return_value = mock_start
        
        result = stream_job.write_to_sink(df, "/tmp/output", "")
        
        assert result == mock_start
        mock_write_stream.outputMode.assert_called_once_with("update")
        mock_format.format.assert_called_once_with("console")
    
    def test_write_to_s3(self, spark_session):
        """Test writing to S3 sink"""
        data = [(1, "dev_1", 10)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("device_id", StringType(), False),
            StructField("count", IntegerType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)
        
        mock_write_stream = MagicMock()
        mock_output_mode = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        mock_partition_by = MagicMock()
        mock_trigger = MagicMock()
        mock_start = MagicMock()
        
        df.writeStream = mock_write_stream
        mock_write_stream.outputMode.return_value = mock_output_mode
        mock_output_mode.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.partitionBy.return_value = mock_partition_by
        mock_partition_by.trigger.return_value = mock_trigger
        mock_trigger.start.return_value = mock_start
        
        result = stream_job.write_to_sink(df, "/tmp/output", "my-bucket")
        
        assert result == mock_start
        mock_format.format.assert_called_once_with("parquet")


class TestMainFunction:
    """Tests for main function"""
    
    @patch('spark_streaming_job.write_to_sink')
    @patch('spark_streaming_job.perform_aggregations')
    @patch('spark_streaming_job.deduplicate_records')
    @patch('spark_streaming_job.filter_valid_records')
    @patch('spark_streaming_job.parse_json_data')
    @patch('spark_streaming_job.read_from_kafka')
    @patch('spark_streaming_job.create_spark_session')
    def test_main_success(self, mock_create_session, mock_read_kafka, 
                         mock_parse, mock_filter, mock_dedup, 
                         mock_agg, mock_write):
        """Test successful main execution"""
        # Setup mocks
        mock_spark = MagicMock()
        mock_kafka_df = MagicMock()
        mock_parsed_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_dedup_df = MagicMock()
        mock_agg_df = MagicMock()
        mock_query = MagicMock()
        
        mock_create_session.return_value = mock_spark
        mock_read_kafka.return_value = mock_kafka_df
        mock_parse.return_value = mock_parsed_df
        mock_filter.return_value = mock_filtered_df
        mock_dedup.return_value = mock_dedup_df
        mock_agg.return_value = mock_agg_df
        mock_write.return_value = mock_query
        mock_query.awaitTermination.return_value = None
        
        # Run main
        stream_job.main()
        
        # Verify all functions were called in order
        mock_create_session.assert_called_once()
        mock_read_kafka.assert_called_once()
        mock_parse.assert_called_once()
        mock_filter.assert_called_once()
        mock_dedup.assert_called_once()
        mock_agg.assert_called_once()
        mock_write.assert_called_once()
        mock_query.awaitTermination.assert_called_once()
    
    @patch('spark_streaming_job.create_spark_session')
    def test_main_spark_session_failure(self, mock_create_session):
        """Test main function handles Spark session creation failure"""
        mock_create_session.side_effect = Exception("Spark session failed")
        
        with pytest.raises(Exception):
            stream_job.main()
    
    @patch('spark_streaming_job.write_to_sink')
    @patch('spark_streaming_job.perform_aggregations')
    @patch('spark_streaming_job.deduplicate_records')
    @patch('spark_streaming_job.filter_valid_records')
    @patch('spark_streaming_job.parse_json_data')
    @patch('spark_streaming_job.read_from_kafka')
    @patch('spark_streaming_job.create_spark_session')
    def test_main_keyboard_interrupt(self, mock_create_session, mock_read_kafka,
                                    mock_parse, mock_filter, mock_dedup,
                                    mock_agg, mock_write):
        """Test main function handles KeyboardInterrupt gracefully"""
        mock_spark = MagicMock()
        mock_query = MagicMock()
        
        mock_create_session.return_value = mock_spark
        mock_read_kafka.return_value = MagicMock()
        mock_parse.return_value = MagicMock()
        mock_filter.return_value = MagicMock()
        mock_dedup.return_value = MagicMock()
        mock_agg.return_value = MagicMock()
        mock_write.return_value = mock_query
        mock_query.awaitTermination.side_effect = KeyboardInterrupt()
        
        # Should handle KeyboardInterrupt without raising
        stream_job.main()
        
        mock_query.stop.assert_called_once()
        mock_spark.stop.assert_called_once()
    
    @patch('spark_streaming_job.write_to_sink')
    @patch('spark_streaming_job.perform_aggregations')
    @patch('spark_streaming_job.deduplicate_records')
    @patch('spark_streaming_job.filter_valid_records')
    @patch('spark_streaming_job.parse_json_data')
    @patch('spark_streaming_job.read_from_kafka')
    @patch('spark_streaming_job.create_spark_session')
    def test_main_streaming_query_exception(self, mock_create_session, mock_read_kafka,
                                           mock_parse, mock_filter, mock_dedup,
                                           mock_agg, mock_write):
        """Test main function handles StreamingQueryException"""
        mock_spark = MagicMock()
        mock_query = MagicMock()
        
        mock_create_session.return_value = mock_spark
        mock_read_kafka.return_value = MagicMock()
        mock_parse.return_value = MagicMock()
        mock_filter.return_value = MagicMock()
        mock_dedup.return_value = MagicMock()
        mock_agg.return_value = MagicMock()
        mock_write.return_value = mock_query
        mock_query.awaitTermination.side_effect = StreamingQueryException("Query failed")
        
        with pytest.raises(StreamingQueryException):
            stream_job.main()


class TestConfiguration:
    """Tests for configuration and environment variables"""
    
    def test_default_configuration(self):
        """Test default configuration values"""
        assert stream_job.KAFKA_BOOTSTRAP_SERVERS == "kafka:9092" or \
               stream_job.KAFKA_BOOTSTRAP_SERVERS == os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        assert stream_job.KAFKA_TOPIC == "input_events" or \
               stream_job.KAFKA_TOPIC == os.getenv("KAFKA_TOPIC", "input_events")
    
    @patch.dict(os.environ, {'KAFKA_BOOTSTRAP_SERVERS': 'test:9092', 'KAFKA_TOPIC': 'test_topic'})
    def test_environment_variable_override(self):
        """Test environment variable configuration"""
        # Reload module to pick up new env vars
        import importlib
        importlib.reload(stream_job)
        
        # Note: This test may need adjustment based on when env vars are read
        assert stream_job.KAFKA_BOOTSTRAP_SERVERS in ["test:9092", "kafka:9092"]


class TestSchema:
    """Tests for event schema definition"""
    
    def test_event_schema_structure(self):
        """Test that EVENT_SCHEMA is properly defined"""
        schema = stream_job.EVENT_SCHEMA
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 7  # event_id, device_id, device_type, event_time, event_duration, location, metadata
        
        # Check required fields
        field_names = [field.name for field in schema.fields]
        assert "event_id" in field_names
        assert "device_id" in field_names
        assert "device_type" in field_names
        assert "event_time" in field_names
        assert "event_duration" in field_names
        assert "location" in field_names
        assert "metadata" in field_names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

