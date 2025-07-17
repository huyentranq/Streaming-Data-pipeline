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



# SILVER LAYER
test_silver:
	docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/silver_layer.py

# GOLD LAYER
test_gold:
	docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/gold_layer.py

test_warehouse:
	docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/warehouse.py 


to_psql:
	docker exec -it airflow-postgres psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

to_psql_no_db:
	docker exec -it de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres

psql_create:
	docker exec -it airflow-postgres psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} -f /tmp/load_dataset/psql_schema.sql -a