.PHONY: help download-data up down logs

help:
	@echo "Available commands:"
	@echo "  make download-data  - Downloads and extracts NYC Yellow Taxi dataset from Kaggle"
	@echo "  make up             - Starts all Docker containers in detached mode"
	@echo "  make down           - Stops and removes all Docker containers"
	@echo "  make logs           - View logs of all Docker containers"

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
