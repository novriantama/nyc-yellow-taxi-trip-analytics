# NYC Yellow Taxi Trip Analytics
https://github.com/novriantama/nyc-yellow-taxi-trip-analytics

This repository contains a full end-to-end Data Engineering pipeline using the NYC Yellow Taxi Trip dataset. It features both Batch Processing and Real-Time Streaming pipelines using a fully containerized local stack.

## Architecture Stack
- **Data Lake:** MinIO (S3-compatible)
- **Data Warehouse:** PostgreSQL
- **Stream Processing / Messaging:** Apache Kafka
- **Data Processing Engine:** Apache Spark (PySpark)
- **Orchestration:** Apache Airflow
- **BI / Visualization:** Apache Superset
- **Simulated Real-Time Producer:** Go

## Prerequisites
- Docker and Docker Compose
- `make` utility
- Python 3.10+
- Go 1.20+ (for the Kafka Producer)
- Kaggle API token (to download the dataset)

## How to Run the App

### 1. Setup and Start Infrastructure
First, download the Kaggle dataset into the `data/` directory, and then start the fully containerized data infrastructure:

```bash
# Download the raw dataset from Kaggle
make download-data

# Build and start all Docker containers in detached mode
make up
# (Note: Use `make rebuild` if you modify Dockerfiles or need a clean start)
```

### 2. Run the Pipelines
You can run either the batch pipeline or the real-time streaming pipeline.

#### Batch Processing
This job reads the raw CSV files from the dataset, cleans the data, and writes parquet files into the MinIO Data Lake.
```bash
make run-batch
```

#### Real-Time Streaming
This involves simulating real-time taxi trips and running a PySpark streaming job to ingest and process them into the MinIO Data Lake and PostgreSQL Data Warehouse.

Open two separate terminal tabs:

**Terminal 1 (Start the Producer):**
```bash
# This creates the Kafka topic and starts simulating real-time taxi trips
make run-producer
```

**Terminal 2 (Start the Streaming ETL):**
```bash
# This starts the PySpark Structured Streaming job to process the Kafka stream
make run-stream
```

### 3. Accessing the Services
Once the containers are running, you can access the various UIs at the following URLs:

- **Apache Superset (BI & Dashboards):** [http://localhost:8088](http://localhost:8088)
  - *Default Credentials:* admin / admin
- **Apache Airflow (Orchestration):** [http://localhost:8085](http://localhost:8085)
- **MinIO Console (Data Lake):** [http://localhost:9001](http://localhost:9001)
- **Apache Spark Master UI:** [http://localhost:8080](http://localhost:8080)

## Managing the App
- `make logs`: View combined logs of all containers.
- `make down`: Stop and remove all containers.