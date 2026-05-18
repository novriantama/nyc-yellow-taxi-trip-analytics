.PHONY: help download-data up down logs rebuild run-batch

help:
	@echo "Available commands:"
	@echo "  make download-data  - Downloads and extracts NYC Yellow Taxi dataset from Kaggle"
	@echo "  make up             - Starts all Docker containers in detached mode"
	@echo "  make down           - Stops and removes all Docker containers"
	@echo "  make logs           - View logs of all Docker containers"
	@echo "  make rebuild        - Rebuilds Docker images and starts containers"
	@echo "  make run-batch      - Runs PySpark batch processing job to MinIO"

download-data:
	@echo "Downloading dataset from Kaggle..."
	@pip install kaggle
	mkdir -p data
	kaggle datasets download -d elemento/nyc-yellow-taxi-trip-data -p data --unzip
	@echo "Data downloaded and extracted to the 'data/' directory."

up:
	docker compose up -d
	@echo "Containers started! Check statuses with 'docker compose ps'."

down:
	docker compose down
	@echo "Containers stopped."

logs:
	docker compose logs -f

rebuild: down
	docker compose up -d --build
	@echo "Containers rebuilt and started!"

run-batch:
	@echo "Submitting PySpark batch job to Spark Master..."
	docker compose exec spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --master spark://spark-master:7077 /opt/spark/jobs/batch_processing.py

run-stream:
	@echo "Submitting PySpark Streaming job to Spark Master..."
	docker compose exec spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --master spark://spark-master:7077 /opt/spark/jobs/streaming_etl.py
