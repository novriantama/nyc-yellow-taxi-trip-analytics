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

## BI Dashboard Results

The final stage of the pipeline visualizes the processed data using Apache Superset. The dashboard provides actionable insights into NYC Taxi operations, including geographic hotspots, demand patterns, executive KPIs, tipping behavior, and the relationship between trip distance and revenue. You can view the exported dashboard results in the [`yellow-taxi-nyc-2026-05-19T15-11-52.122Z.pdf`](./yellow-taxi-nyc-2026-05-19T15-11-52.122Z.pdf) file.

Below are the key queries powering the dashboard visualizations:

### 1. The Geographic Heatmap (Where are the hotspots?)
Visualizes trip volume and average tip amounts across different coordinates to identify high-demand pickup locations.

```sql
SELECT 
    pickup_longitude, 
    pickup_latitude, 
    COUNT(trip_id) AS trip_volume,
    AVG(tip_amount) AS avg_tip
FROM fact_trip
WHERE pickup_longitude BETWEEN -74.3 AND -73.7 
  AND pickup_latitude BETWEEN 40.5 AND 40.9
GROUP BY 
    pickup_longitude, 
    pickup_latitude;
```

### 2. Time/Day Matrix Heatmap (When is the highest demand?)
Analyzes peak hours and days of the week to show when taxi services are most requested.

```sql
SELECT 
    dd.day_of_week, 
    dtb.time_block_name, 
    COUNT(ft.trip_id) AS total_trips
FROM fact_trip ft
INNER JOIN dim_date dd 
    ON ft.pickup_date_sk = dd.date_sk
INNER JOIN dim_time_block dtb 
    ON ft.pickup_time_block_sk = dtb.time_block_sk
GROUP BY 
    dd.day_of_week, 
    dtb.time_block_name;
```

### 3. Executive KPIs (The "Big Picture")
Tracks high-level daily metrics including total trips, overall revenue, and average trip distance.

```sql
SELECT 
    dd.full_date,
    COUNT(ft.trip_id) AS total_trips,
    SUM(ft.total_amount) AS total_revenue,
    AVG(ft.trip_distance) AS avg_distance_miles
FROM fact_trip ft
INNER JOIN dim_date dd 
    ON ft.pickup_date_sk = dd.date_sk
GROUP BY 
    dd.full_date;
```

### 4. Tipping Behavior by Payment Type
Compares average tip amounts and percentages across different payment methods to understand passenger tipping habits.

```sql
SELECT 
    dpt.payment_description,
    AVG(ft.tip_amount) AS avg_tip_dollars,
    -- Calculate Tip Percentage (avoiding divide-by-zero errors)
    AVG(CASE WHEN ft.fare_amount > 0 THEN (ft.tip_amount / ft.fare_amount) * 100 ELSE 0 END) AS tip_percentage
FROM fact_trip ft
INNER JOIN dim_payment_type dpt 
    ON ft.payment_type_sk = dpt.payment_type_sk
GROUP BY 
    dpt.payment_description
ORDER BY 
    avg_tip_dollars DESC;
```

### 5. Revenue vs. Distance Scatter Plot
Explores the correlation between trip distance and fare amount, categorized by rate codes.

```sql
SELECT 
    trip_distance,
    fare_amount,
    drc.rate_description
FROM fact_trip ft
INNER JOIN dim_rate_code drc 
    ON ft.rate_code_sk = drc.rate_code_sk
WHERE trip_distance < 50 
  AND fare_amount < 200
LIMIT 50000;
```