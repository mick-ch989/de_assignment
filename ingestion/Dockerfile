FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer code
COPY producer/ .

# Set environment variable for Kafka bootstrap servers (can be overridden)
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run the producer
CMD ["python", "producer.py"]

