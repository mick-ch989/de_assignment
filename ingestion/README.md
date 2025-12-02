# Ingestion Module

This module provides a Kafka producer that generates and sends realistic device event data to a Kafka topic for processing by the streaming pipeline.

## Overview

The ingestion module consists of a Python-based Kafka producer that:
- Generates realistic device event data using the Faker library
- Sends events to the `input_events` Kafka topic
- Produces messages at a configurable rate (default: every 0.5 seconds)
- Supports Docker containerization for easy deployment
<br>
### **Schema is AI generated**

## Structure

```
ingestion/
├── Dockerfile              # Docker image definition
├── producer/
│   ├── producer.py        # Main producer script
│   ├── schema.json        # JSON schema for event data
│   └── requirements.txt   # Python dependencies
└── README.md              # This file
```

## Features

- **Realistic Data Generation**: Uses Faker library to generate realistic device events with:
  - Unique event IDs
  - Device information (ID, type)
  - Timestamps (UTC, ISO 8601 format)
  - Geographic location data (latitude, longitude, city, country)
  - Device metadata (firmware version, battery level, signal strength)

- **Device Types**: Supports four device types:
  - `sensor_A`
  - `sensor_B`
  - `camera`
  - `thermo`

- **Configurable**: Kafka bootstrap servers can be configured via environment variable

## Data Schema

Each event message follows this JSON structure:

```json
{
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
```

See `producer/schema.json` for the complete JSON schema definition.

## Prerequisites

- Python 3.11+
- Kafka broker running and accessible
- Docker (optional, for containerized deployment)

## Installation

### Local Installation

1. Navigate to the producer directory:
   ```bash
   cd ingestion/producer
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Docker Installation

The module includes a Dockerfile for containerized deployment. Build the image:

```bash
cd ingestion
docker build -t kafka-producer .
```

## Usage

### Running Locally

1. Ensure Kafka is running and accessible (default: `localhost:9092`)

2. Run the producer:
   ```bash
   cd ingestion/producer
   python producer.py
   ```

3. The producer will:
   - Connect to Kafka
   - Start generating and sending events to the `input_events` topic
   - Print each sent message to the console
   - Send messages every 0.5 seconds

4. Press `Ctrl+C` to stop the producer

### Running with Docker

1. Build the Docker image (if not already built):
   ```bash
   cd ingestion
   docker build -t kafka-producer .
   ```

2. Run the container:
   ```bash
   docker run --rm \
     -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
     --network <your-docker-network> \
     kafka-producer
   ```

   Replace `<your-docker-network>` with your Docker network name, or use `--network host` if running Kafka on the host.

### Configuration

The producer can be configured using environment variables:

- **KAFKA_BOOTSTRAP_SERVERS**: Kafka broker address (default: `localhost:9092`)
  ```bash
  export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
  python producer.py
  ```

## Dependencies

- **kafka-python** (2.0.2): Kafka client library for Python
- **faker** (24.0.0): Library for generating fake data

## Integration with Docker Compose

When using with the main `docker-compose.yml`, ensure the producer connects to the Kafka service:

```bash
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  --network assignment_default \
  kafka-producer
```

Or add the producer as a service in your docker-compose.yml:

```yaml
producer:
  build: ./ingestion
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
  depends_on:
    - kafka
```

## Output

The producer sends messages to the Kafka topic `input_events`. Each message is a JSON object containing device event data. Example output:

```
Starting Kafka producer...
Kafka bootstrap servers: localhost:9092
Sending messages to topic: input_events
Press Ctrl+C to stop

Sent: {
  "event_id": 456789,
  "device_id": "dev_23",
  "device_type": "camera",
  "event_time": "2024-01-15T10:30:45.123456+00:00",
  "event_duration": 1.234,
  "location": {
    "latitude": 51.507351,
    "longitude": -0.127758,
    "city": "London",
    "country": "United Kingdom"
  },
  "metadata": {
    "firmware_version": "2.5.10",
    "battery_level": 92,
    "signal_strength": -68
  }
}
```

## Troubleshooting

### Connection Issues

If the producer cannot connect to Kafka:
- Verify Kafka is running: `docker ps | grep kafka`
- Check the bootstrap servers address matches your Kafka setup
- Ensure network connectivity between producer and Kafka

### Topic Not Found

The producer expects the `input_events` topic to exist or be auto-created. Ensure Kafka is configured with:
```
KAFKA_AUTO_CREATE_TOPICS_ENABLE=true
```

### Rate Limiting

The producer sends messages every 0.5 seconds. To change the rate, modify the `time.sleep(0.5)` value in `producer.py`.

## Next Steps

After starting the producer, events will be available in the `input_events` Kafka topic for consumption by the Spark streaming job in the processing module.
