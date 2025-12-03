FROM bitnami/spark:latest

USER root

WORKDIR /opt

COPY spark_streaming_job.py /opt/spark_streaming_job.py

RUN chmod +x /opt/spark_streaming_job.py

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Default configuration (can be overridden via docker-compose or environment)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=input_events
ENV KAFKA_STARTING_OFFSETS=latest
ENV KAFKA_MAX_OFFSETS_PER_TRIGGER=1000
ENV KAFKA_FAIL_ON_DATA_LOSS=false

ENV CHECKPOINT_LOCATION=/tmp/streaming_checkpoint
ENV OUTPUT_PATH=/tmp/streaming_output
ENV WINDOW_DURATION=1 minute
ENV WATERMARK_DELAY=10 minutes

ENV S3_PREFIX=streaming-output
ENV S3_ENDPOINT=

# Spark configuration defaults
ENV SPARK_SQL_ADAPTIVE_ENABLED=true
ENV SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED=true
ENV SPARK_SQL_STREAMING_SCHEMA_INFERENCE=true
ENV SPARK_SQL_STREAMING_STOP_GRACEFULLY_ON_SHUTDOWN=true
ENV SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer
ENV SPARK_SQL_STREAMING_MIN_BATCHES_TO_RETAIN=10

# Note: S3_BUCKET and AWS credentials should be set via docker-compose or runtime
# Do not set them here as they may contain sensitive information

# Default command (can be overridden in docker-compose)
CMD ["spark-submit", "--master", "spark://spark-master:7077", "/opt/spark_streaming_job.py"]
