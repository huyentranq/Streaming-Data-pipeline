from spark_job.resources.spark_manager import *
from spark_job.resources.minio_manager import *
from pyspark.sql.functions import from_json, col, current_timestamp
import logging
import os

BRONZE_PATH = "s3a://lakehouse/bronze/pizza_sales"

LAYER = "silver"
FMT = "delta"
TABLE_NAME = "pizza_sales"
BUCKET  = "lakehouse"
config = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}
spark = create_spark_session(config, "Silver_layer")


bronze_df = read_delta_stream(spark, "lakehouse", "bronze", "pizza_sales")
