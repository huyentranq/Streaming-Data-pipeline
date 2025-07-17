# Pizza Sales Streaming Pipeline

## 📋 Table of Contents

- [🏗️ Project Overview](#️-project-overview)
  - [Key Features](#key-features)
- [ Architecture](#️-architecture)
- [ Data Flow & Lineage](#-data-flow--lineage)

- [ Folder Structure](#-folder-structure)
- [ Installation & Deployment](#-installation--deployment)

- [🔌 Port Mappings & Service Access](#-port-mappings--service-access)

- [📡 Kafka Streaming Simulation](#-kafka-streaming-simulation)
  - [Start Data Generation](#start-data-generation)
  - [Kafka Topics](#kafka-topics)
  - [Monitor Kafka Streams](#monitor-kafka-streams)
- [⚡ Airflow Usage](#-airflow-usage)

- [📊 Power BI Connection](#-power-bi-connection)

- [🚧 Limitations & Future Improvements](#-limitations--future-improvements)
  - [Current Limitations](#current-limitations)
  - [Proposed Improvements](#proposed-improvements)

- [🙏 Acknowledgments](#-acknowledgments)

---

A real-time data engineering pipeline that simulates pizza sales data streaming using modern big data technologies. This project demonstrates a complete end-to-end streaming solution implementing the medallion architecture (Bronze-Silver-Gold layers) for data processing and analytics.

## 🏗️ Project Overview

The Pizza Sales Streaming Pipeline is designed to showcase real-time data processing capabilities by simulating pizza restaurant sales data. The pipeline ingests streaming data through Apache Kafka, processes it using Apache Spark Streaming, stores raw and processed data in MinIO and PostgreSQL respectively, orchestrates workflows with Apache Airflow, and provides business insights through Power BI dashboards.

### Key Features

- **Real-time Data Streaming**: Kafka producers simulate continuous pizza sales transactions
- **Stream Processing**: Spark Streaming processes data in near real-time
- **Medallion Architecture**: Implements Bronze, Silver, and Gold data layers
- **Data Lake Storage**: MinIO serves as the data lake for raw data storage
- **Data Warehouse**: PostgreSQL stores processed, analytics-ready data
- **Workflow Orchestration**: Airflow manages and schedules data pipelines
- **Business Intelligence**: Power BI dashboards for data visualization and insights
- **Containerized Deployment**: Docker-based setup for easy deployment and scalability

## 🏛️ Architecture

<img src="docs/images/architecture-diagram.png" alt="Pipeline Architecture Diagram" width="800"/>

The pipeline follows a modern data architecture pattern with the following components:

- **Data Ingestion Layer**: Kafka producers generate and stream pizza sales data
- **Stream Processing Layer**: Spark Streaming processes incoming data streams
- **Storage Layer**: MinIO (data lake) and PostgreSQL (data warehouse)
- **Orchestration Layer**: Airflow manages pipeline workflows and dependencies
- **Analytics Layer**: Power BI provides interactive dashboards and reports

## 📊 Data Flow & Lineage

<img src="docs/images/data-flow-diagram.png" alt="Data Flow Diagram" width="800"/>

### Data Pipeline Stages

1. **Data Generation**: Python scripts simulate pizza sales transactions with realistic patterns
2. **Bronze Layer (Raw Data)**: 
   - Kafka topics receive streaming sales data
   - Raw data stored in MinIO in Parquet format
   - Data includes: order_id, timestamp, pizza_type, size, quantity, price, customer_info
3. **Silver Layer (Cleaned Data)**:
   - Spark Streaming validates and cleanses data
   - Data deduplication and schema enforcement
   - Enrichment with calculated fields (total_amount, profit_margin)
4. **Gold Layer (Aggregated Data)**:
   - Business-ready aggregations and KPIs
   - Hourly, daily, and monthly sales summaries
   - Customer segmentation and product performance metrics
   - Data stored in PostgreSQL for BI consumption

### Data Lineage

```
Raw Sales Data → Kafka Topics → Spark Streaming → MinIO (Bronze)
                                      ↓
PostgreSQL (Silver) ← Data Validation & Cleansing
                                      ↓
PostgreSQL (Gold) ← Aggregations & Business Logic
                                      ↓
Power BI Dashboards ← Analytics & Visualization
```

## 📁 Folder Structure

```
pizza-sales-streaming-pipeline/
├── README.md
├── Makefile
├── docker-compose.yml
├── .env
├── docs/
│   ├── images/
│   └── setup-guides/
├── airflow/
│   ├── dags/
│   │   ├── bronze_to_silver_dag.py
│   │   ├── silver_to_gold_dag.py
│   │   └── data_quality_dag.py
│   ├── plugins/
│   └── config/
├── spark/
│   ├── jobs/
│   │   ├── bronze_layer_processor.py
│   │   ├── silver_layer_processor.py
│   │   └── gold_layer_processor.py
│   ├── config/
│   └── requirements.txt
├── kafka/
│   ├── producers/
│   │   ├── pizza_sales_producer.py
│   │   └── data_generator.py
│   ├── config/
│   └── schemas/
├── sql/
│   ├── create_tables.sql
│   ├── silver_layer_transforms.sql
│   └── gold_layer_aggregations.sql
├── minio/
│   └── config/
├── postgresql/
│   ├── init/
│   └── config/
├── powerbi/
│   ├── dashboards/
│   └── reports/
└── monitoring/
    ├── grafana/
    └── prometheus/
```

## 🚀 Installation & Deployment

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Make utility
- 8GB+ RAM recommended
- 20GB+ free disk space

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/pizza-sales-streaming-pipeline.git
   cd pizza-sales-streaming-pipeline
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env file with your configuration
   ```

3. **Deploy the entire pipeline**
   ```bash
   make deploy
   ```

4. **Initialize the data warehouse**
   ```bash
   make init-database
   ```

5. **Start data streaming**
   ```bash
   make start-streaming
   ```

### Makefile Commands

```bash
# Deploy all services
make deploy

# Stop all services
make stop

# Restart services
make restart

# View logs
make logs

# Clean up everything
make clean

# Initialize database schemas
make init-database

# Start Kafka producers
make start-streaming

# Stop streaming
make stop-streaming

# Run data quality checks
make data-quality-check

# Backup data
make backup

# Monitor services
make monitor
```

## 🔌 Port Mappings & Service Access

| Service | Port | URL | Description |
|---------|------|-----|-------------|
| Apache Airflow | 8080 | http://localhost:8080 | Workflow orchestration UI |
| Kafka UI | 8081 | http://localhost:8081 | Kafka cluster management |
| MinIO Console | 9001 | http://localhost:9001 | Data lake management |
| PostgreSQL | 5432 | localhost:5432 | Data warehouse |
| Spark Master UI | 4040 | http://localhost:4040 | Spark cluster monitoring |
| Grafana | 3000 | http://localhost:3000 | Pipeline monitoring |
| Jupyter Lab | 8888 | http://localhost:8888 | Data exploration |

### Default Credentials

- **Airflow**: admin / admin
- **MinIO**: minio / minio123
- **PostgreSQL**: postgres / postgres
- **Grafana**: admin / admin

### Check Container Status

```bash
# Check all container status
make status

# View detailed container information
docker-compose ps

# Check container logs
docker-compose logs [service-name]

# Monitor resource usage
docker stats
```

## 📡 Kafka Streaming Simulation

### Start Data Generation

```bash
# Start pizza sales data simulation
make start-streaming

# Start with custom parameters
docker-compose exec kafka python /app/producers/pizza_sales_producer.py \
    --rate 100 \
    --duration 3600 \
    --restaurants 5
```

### Kafka Topics

- `pizza-sales-raw`: Raw sales transactions
- `pizza-sales-validated`: Validated and cleaned data
- `pizza-sales-aggregated`: Hourly aggregations

### Monitor Kafka Streams

<img src="docs/images/kafka-ui-screenshot.png" alt="Kafka UI Screenshot" width="800"/>

```bash
# View topic messages in real-time
docker-compose exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic pizza-sales-raw \
    --from-beginning

# Check topic details
make kafka-topics
```

## ⚡ Airflow Usage

### Access Airflow WebUI

1. Navigate to http://localhost:8080
2. Login with admin/admin
3. Enable DAGs from the main dashboard

<img src="docs/images/airflow-dashboard.png" alt="Airflow Dashboard Screenshot" width="800"/>

### Available DAGs

- **bronze_to_silver_dag**: Processes raw data and applies data quality rules
- **silver_to_gold_dag**: Creates business aggregations and KPIs
- **data_quality_dag**: Runs data quality checks and alerts
- **backup_dag**: Scheduled backups of processed data

### Trigger DAGs Manually

```bash
# Trigger specific DAG
docker-compose exec airflow-webserver airflow dags trigger bronze_to_silver_dag

# Trigger with configuration
docker-compose exec airflow-webserver airflow dags trigger silver_to_gold_dag \
    --conf '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
```

### Monitor DAG Execution

<img src="docs/images/airflow-dag-runs.png" alt="Airflow DAG Runs Screenshot" width="600"/>

## 📊 Power BI Connection

### PostgreSQL Connection Setup

1. **Get connection details**
   ```bash
   make get-db-credentials
   ```

2. **Power BI Connection String**
   ```
   Server: localhost
   Port: 5432
   Database: pizza_sales_dw
   Username: postgres
   Password: postgres
   ```

### Available Tables for Analysis

- `gold.daily_sales_summary`
- `gold.pizza_performance`
- `gold.customer_segments`
- `gold.hourly_trends`
- `gold.revenue_analytics`

### Sample Power BI Dashboard

<img src="docs/images/powerbi-dashboard.png" alt="Power BI Dashboard Screenshot" width="800"/>

### Key Metrics Available

- Real-time sales volume and revenue
- Top-performing pizza types and sizes
- Peak hours and seasonal trends
- Customer ordering patterns
- Profit margin analysis
- Geographic sales distribution

## 🚧 Limitations & Future Improvements

### Current Limitations

1. **Scalability**: Single-node setup limits processing capacity
2. **Data Volume**: Optimized for moderate data volumes (< 1M records/day)
3. **Error Handling**: Basic error handling and retry mechanisms
4. **Security**: Default credentials and minimal security configuration
5. **Monitoring**: Limited observability and alerting capabilities

### Proposed Improvements

1. **Enhanced Scalability**
   - Multi-node Kafka and Spark clusters
   - Kubernetes deployment for auto-scaling
   - Partitioning strategies for large datasets

2. **Advanced Features**
   - Real-time ML model serving for demand forecasting
   - CDC (Change Data Capture) for real-time updates
   - Event-driven architecture with additional microservices

3. **Security Enhancements**
   - SSL/TLS encryption for all communications
   - OAuth2/SAML integration for authentication
   - Role-based access control (RBAC)
   - Data encryption at rest and in transit

4. **Monitoring & Observability**
   - Comprehensive logging with ELK stack
   - Prometheus metrics and custom dashboards
   - Alerting for data quality and system health
   - Distributed tracing for debugging

5. **Data Quality & Governance**
   - Data lineage tracking
   - Automated data profiling and validation
   - Schema evolution and compatibility checks
   - Data catalog integration

6. **Performance Optimization**
   - Caching strategies for frequently accessed data
   - Database indexing optimization
   - Spark job tuning and optimization
   - Efficient data formats (Delta Lake, Iceberg)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📧 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/yourusername)
- LinkedIn: [Your LinkedIn](https://linkedin.com/in/yourprofile)
- Email: your.email@example.com

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Software Foundation for the excellent open-source tools
- The data engineering community for inspiration and best practices
- Pizza lovers worldwide for the motivation to create this project

---

**Note**: This project is for educational and demonstration purposes. For production use, additional security, monitoring, and scalability considerations should be implemented.