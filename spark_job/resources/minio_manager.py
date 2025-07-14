import os
import sys
import logging
import warnings
import traceback

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from delta import configure_spark_with_delta_pip
from minio import Minio


def make_bucket(config, bucket_name):
    client = Minio(
        config["endpoint_url"],
        access_key=config["minio_access_key"],
        secret_key=config["minio_secret_key"],
        secure=False
    )
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

# load_output
def read_delta_stream(spark, bucket, layer, table_name):
    path = f"s3a://{bucket}/{layer}/{table_name}"
    return spark.readStream.format("delta").load(path)



def write_stream_to_lake(df: DataFrame, bucket: str, layer: str, table_name, fmt: str = "delta") -> None:

    try:

        path = f"s3a://{bucket}/{layer}/{table_name}"
        checkpoint_path = f"s3a://{bucket}/checkpoints/{layer}/{table_name}"    
        logging.info(f"📦 Starting stream write to {path} (format: {fmt}, checkpoint: {checkpoint_path})")
        # logging.info(f"📑 DataFrame schema:\n{df.printSchema()}")
        # logging.info(f"🔍 Streaming DataFrame plan:\n{df._jdf.queryExecution().logical().toString()}")

        query = df.writeStream \
            .format(fmt) \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", path) \
            .trigger(once=True) \
            .start()

        logging.info(f"🚀 Stream started: {query.name} (isActive={query.isActive})")
        query.awaitTermination()

        logging.info(f"✅ Streaming to {fmt.upper()} on {layer} layer complete")

    except Exception as e:
        logging.error(f"❌ Failed to write stream to lakehouse path '{path}': {str(e)}")
        logging.error("📛 Exception details:", exc_info=True)
        raise