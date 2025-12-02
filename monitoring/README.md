# Monitoring Module

This module provides monitoring capabilities for the streaming pipeline using Prometheus and Grafana.

## Components

### Prometheus
- **Port**: 9090
- **Config**: `prometheus/prometheus.yml`
- Scrapes metrics from:
  - Kafka Exporter (port 9308)
  - Spark Master JMX Exporter (port 9101)
  - Spark Worker 1 JMX Exporter (port 9102)
  - Spark Worker 2 JMX Exporter (port 9103)

### Grafana
- **Port**: 3000
- **Default credentials**: admin/admin
- **Datasource**: Automatically configured to use Prometheus
- **Dashboards**: Pre-configured streaming pipeline dashboard

### JMX Exporters
- **Spark Master**: Exports JMX metrics from Spark Master (JMX port 7078)
- **Spark Workers**: Export JMX metrics from Spark Workers (JMX ports 7079, 7080)

### Kafka Exporter
- **Port**: 9308
- Exports Kafka broker and topic metrics

## Usage

The monitoring services are started automatically when you run:

```bash
docker-compose up -d
```

### Access Points

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

### Viewing Metrics

1. Open Grafana at http://localhost:3000
2. Login with admin/admin
3. Navigate to Dashboards → Streaming Pipeline Dashboard
4. View real-time metrics for Kafka and Spark

## Configuration Files

- `prometheus/prometheus.yml` - Prometheus scrape configuration
- `grafana/datasources/prometheus.yaml` - Grafana datasource provisioning
- `grafana/dashboards/streaming_pipeline.json` - Pre-configured dashboard
- `grafana/dashboards/dashboard.yml` - Dashboard provisioning config
- `jmx/spark-master.yml` - JMX exporter config for Spark Master
- `jmx/spark-worker.yml` - JMX exporter config for Spark Workers
