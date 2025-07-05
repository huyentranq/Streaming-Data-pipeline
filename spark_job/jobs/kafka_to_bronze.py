from spark_job.resources.spark_manager import *
from spark_job.resources.minio_manager import *
from pyspark.sql.functions import from_json, col, current_timestamp
import logging
import os

KAFKA_TOPIC = "pizza_sales"
KAFKA_SERVERS = "broker:29092"
CHECKPOINT_DIR = "/tmp/checkpoints/kafka_to_bronze"
BRONZE_PATH = "s3a://lakehouse/bronze/pizza_sales"
LAYER ="bronze"
FMT="delta"
TABLE_NAME = "pizza_sales"
BUCKET = "lakehouse"

config = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

spark = create_spark_session(config, "Bronze_layer")
df_raw = read_kafka_stream(spark, KAFKA_TOPIC)
sales_schema = get_sales_schema()
logging.info(f"📑 Loaded schema: {sales_schema.simpleString()}")


df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .withColumn("json", from_json(col("value"), sales_schema)) \
    .select("json.*") \
    .withColumn("ingest_time", current_timestamp())

# make only one bucket in the lakehouse(don't have to replicate to other layer)
make_bucket(config, BUCKET)
write_stream_to_lake(df_parsed, bucket = BUCKET, layer=LAYER, table_name = TABLE_NAME)

