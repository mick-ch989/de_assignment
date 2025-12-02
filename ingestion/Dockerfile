FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ .

ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092

CMD ["python", "producer.py"]

