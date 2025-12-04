# Monitoring Module

This module provides comprehensive monitoring capabilities for the streaming pipeline using Prometheus and Grafana.

## Components

### Prometheus
- **Port**: 9090
- **Config**: `prometheus/prometheus.yml`
- Scrapes metrics from:
  - Kafka Exporter (port 9308)
  - Spark Master JMX Exporter (port 9101)
  - Spark Worker 1 JMX Exporter (port 9102)
  - Spark Worker 2 JMX Exporter (port 9103)
  - Spark Streaming Application JMX Exporter (port 9104)

### Grafana
- **Port**: 3000
- **Default credentials**: admin/admin
- **Datasource**: Automatically configured to use Prometheus
- **Dashboards**: 
  - **Streaming Pipeline Dashboard**: Kafka and pipeline metrics
  - **Spark Cluster Monitoring**: Comprehensive Spark cluster metrics

### JMX Exporters
- **Spark Master**: Exports JMX metrics from Spark Master (JMX port 7078)
  - Metrics include: alive workers, applications, cores usage, memory usage
- **Spark Workers**: Export JMX metrics from Spark Workers (JMX ports 7079, 7080)
  - Metrics include: cores usage, memory usage, executors, applications
- **Spark Streaming Application**: Exports JMX metrics from Spark application (JMX port 9999)
  - Metrics include: streaming metrics, application metrics, JVM metrics

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
- **Spark UI**: http://localhost:4040 (when Spark application is running)

### Viewing Metrics

1. Open Grafana at http://localhost:3000
2. Login with admin/admin
3. Navigate to Dashboards:
   - **Streaming Pipeline Dashboard**: View Kafka and pipeline metrics
   - **Spark Cluster Monitoring**: View comprehensive Spark cluster metrics including:
     - Spark Master metrics (workers, applications, resources)
     - Spark Worker metrics (cores, memory, executors)
     - JVM metrics (heap memory, GC, threads)
     - Spark application metrics

### Spark Monitoring Dashboard Features

The Spark Cluster Monitoring dashboard includes:

- **Cluster Overview**: Alive workers, active/waiting applications, cores and memory usage
- **Worker Metrics**: Per-worker cores and memory usage, executors, applications
- **JVM Metrics**: Heap memory usage, garbage collection, threads, uptime
- **Real-time Updates**: Auto-refresh every 10 seconds

## Configuration Files

- `prometheus/prometheus.yml` - Prometheus scrape configuration
- `grafana/datasources/prometheus.yaml` - Grafana datasource provisioning
- `grafana/dashboards/streaming_pipeline.json` - Kafka and pipeline dashboard
- `grafana/dashboards/spark_monitoring.json` - Spark cluster monitoring dashboard
- `grafana/dashboards/dashboard.yml` - Dashboard provisioning config
- `jmx/spark-master.yml` - JMX exporter config for Spark Master
- `jmx/spark-worker-1.yml` - JMX exporter config for Spark Worker 1
- `jmx/spark-worker-2.yml` - JMX exporter config for Spark Worker 2
- `jmx/spark-streaming.yml` - JMX exporter config for Spark Streaming Application
