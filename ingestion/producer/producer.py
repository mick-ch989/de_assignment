import json
import os
import time
import random
from kafka import KafkaProducer
from datetime import datetime, timezone
from faker import Faker

# Initialize Faker
fake = Faker()

# Get Kafka bootstrap servers from environment variable or use default
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

DEVICE_TYPES = ["sensor_A", "sensor_B", "camera", "thermo"]

def generate_record():
    return {
        "event_id": fake.random_int(min=100000, max=999999),
        "device_id": f"dev_{fake.random_int(min=1, max=50)}",
        "device_type": random.choice(DEVICE_TYPES),
        "event_time": datetime.now(timezone.utc).isoformat(),
                "event_duration": round(float(fake.pyfloat(left_digits=1, right_digits=3, positive=True, min_value=0.1, max_value=5.0)), 3),
        "location": {
            "latitude": round(float(fake.latitude()), 6),
            "longitude": round(float(fake.longitude()), 6),
            "city": fake.city(),
            "country": fake.country()
        },
        "metadata": {
            "firmware_version": f"{fake.random_int(min=1, max=5)}.{fake.random_int(min=0, max=9)}.{fake.random_int(min=0, max=99)}",
            "battery_level": fake.random_int(min=0, max=100),
            "signal_strength": fake.random_int(min=-120, max=0)
        }
    }

if __name__ == "__main__":
    print("Starting Kafka producer...")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print("Sending messages to topic: input_events")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            msg = generate_record()
            producer.send("input_events", msg)
            print("Sent:", json.dumps(msg, indent=2))
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"\nError occurred: {e}")
    finally:
        try:
            # Flush any pending messages before closing
            producer.flush(timeout=5)
        except Exception:
            pass
        try:
            producer.close(timeout=5)
        except Exception:
            # Ignore errors during close (file descriptor might already be closed)
            pass
        print("Producer stopped.")
