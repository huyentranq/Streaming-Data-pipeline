# spark_manager.py

import os
import sys
import logging
import warnings
import traceback

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from delta import configure_spark_with_delta_pip

from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
warnings.filterwarnings("ignore")


def create_spark_session(cfg, app_name="SparkKafkaPipeline"):

    try:
        pkgs = []

        builder = (
            SparkSession.builder
            .appName(app_name)
            # ------ Delta ------
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # ------ MinIO / S3A ------
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{cfg['endpoint_url']}")
            .config("spark.hadoop.fs.s3a.access.key",  str(cfg['minio_access_key']))
            .config("spark.hadoop.fs.s3a.secret.key",  str(cfg['minio_secret_key']))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # ------ Packages ------
            .config("spark.jars.packages", ",".join(pkgs))
            # ------ Optional resources ------
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logging.info("✅ SparkSession created successfully")
        return spark
    except Exception as e:
        logging.error("❌ Failed to create SparkSession")
        traceback.print_exc(file=sys.stderr)
        raise

# 2. Define Schema for JSON Records
def get_sales_schema() -> StructType:
    return StructType([
        StructField("pizza_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("pizza_name_id", StringType(), False),
        StructField("quantity", StringType(), True),
        StructField("order_date", StringType(), False),
        StructField("order_time", StringType(), False),
        StructField("unit_price", StringType(), True),
        StructField("total_price", StringType(), True),
        StructField("pizza_size", StringType(), True),
        StructField("pizza_category", StringType(), True),
        StructField("pizza_ingredients", StringType(), True),
        StructField("pizza_name", StringType(), True)
    ])


#  Read Stream from Kafka Topic 
def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.metadata.max.age.ms", "2000") \
            .load()

        logging.info(f"✅ Connected to Kafka topic: {topic}")
        return df
    except Exception as e:
        logging.error(f"❌ Failed to read from Kafka topic: {topic}")
        traceback.print_exc(file=sys.stderr)
        raise


#  Parse Kafka JSON and Enrich with Timestamp
def parse_sales_data(df: DataFrame, schema: StructType) -> DataFrame:
    try:
        parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingestion_timestamp", current_timestamp())

        logging.info("✅ Kafka records parsed into structured DataFrame")
        return parsed_df
    except Exception as e:
        logging.error("❌ Failed to parse sales data from Kafka")
        traceback.print_exc(file=sys.stderr)
        raise

