include env
install:
	pip install -r requirements.txt
open:
	docker exec -it kafka bash

build_stream:
	docker-compose -f stream-docker-compose.yml build
up_stream: 
	docker-compose -f stream-docker-compose.yml up -d
down_stream:
	docker-compose -f stream-docker-compose.yml down

download-spark-jars:
	./download_spark_jars.sh

build:
	docker-compose build
up:
	docker-compose up -d
down:
	docker-compose down


kafka_streaming:
	python -m kafka_streaming.streaming