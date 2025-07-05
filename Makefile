include .env
install:
	pip install -r requirements.txt
open:
	docker exec -it broker bash

build_stream:
	docker-compose -f stream-docker-compose.yml build
up_stream: 
	docker-compose -f stream-docker-compose.yml up -d
down_stream:
	docker-compose -f stream-docker-compose.yml down -v

build_minio:
	docker-compose -f minio-docker-compose.yml build
up_minio:
	docker-compose -f minio-docker-compose.yml up -d
download-spark-jars:
	./download_spark_jars.sh

build:
	docker-compose build
up:
	docker-compose up -d
down:
	docker-compose down


kafka_stream:
	python -m kafka_streaming.streaming

test_bronze:
	docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/kafka_to_bronze.py