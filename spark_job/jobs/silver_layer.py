from spark_job.resources.spark_manager import *
from spark_job.resources.minio_manager import *
import logging
import os
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

import sys
BRONZE_PATH = "s3a://lakehouse/bronze/pizza_sales"

LAYER = "silver"
FMT = "delta"

BUCKET  = "lakehouse"
config = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}
spark = create_spark_session(config, "Silver_layer")


def silver_pizza_cleaned(bronze_df: DataFrame) -> DataFrame:
    spark_df = bronze_df

    # remove row that order_id is null
    spark_df = spark_df.filter(spark_df['order_id'].isNotNull())
    spark_df = spark_df.dropDuplicates(["order_id", "pizza_id"])

    spark_df = spark_df.withColumn(
        "order_date_tmp",
        F.regexp_replace(F.col("order_date"),r"[./]","-")
    )
    spark_df =(
        spark_df.withColumn("order_date_parsed",F.to_date("order_date_tmp","d-M-yyyy"))
        .withColumn(
            "order_date",
            F.date_format("order_date_parsed","yyyy-MM-dd")
        )
        .drop("order_date_tmp","order_date_parsed")
    )
    spark_df = (
        spark_df
        .withColumn("pizza_id",    F.col("pizza_id").cast("int"))
        .withColumn("order_id",    F.col("order_id").cast("int"))
        .withColumn("quantity",    F.col("quantity").cast("int"))
        # dùng DECIMAL để tránh sai số float
        .withColumn("unit_price",  F.col("unit_price").cast("decimal(8,2)"))
        .withColumn("total_price", F.col("total_price").cast("decimal(10,2)"))
    )
    logging.info(f"📑 data type of this DataFrame: \n {spark_df.dtypes}")
    logging.info(f"clean success")
    return spark_df
def silver_order_items(silver_pizza_cleaned: DataFrame) -> DataFrame:
    df = silver_pizza_cleaned
    selected_columns = [
        "pizza_id",
        "order_id",
        "pizza_name_id",
        "quantity",
        "order_date",
        "order_time",
        "unit_price",
        "total_price",

    ]
    logging.info("Selecting relevant columns for silver_order_items...")

    df_selected = df.select(*selected_columns)

    return df_selected
def silver_pizza_catalog(silver_pizza_cleaned: DataFrame) -> DataFrame:
    """
    Trả về DataFrame 'silver_pizza_catalog' với:
      - khóa chính        : pizza_name_id
      - pizza_base_id     : bỏ hậu tố _s/_m/_l/_xl khỏi pizza_name_id
      - giữ thêm các cột  : pizza_size, unit_price, pizza_ingredients, pizza_name
    """
    silver_df = silver_pizza_cleaned
    catalog_df = (
        silver_df
        .withColumn(
            "pizza_base_id",
            F.regexp_replace(F.col("pizza_name_id"), r"_[smlxl]{1}$", "")
        )
        .select(
            "pizza_name_id",       # PK
            "pizza_base_id",
            "pizza_size",
            "unit_price",
            "pizza_ingredients",
            "pizza_name"
        )
        .dropDuplicates(["pizza_name_id"])
    )

    return catalog_df

def silver_timestamp(silver_pizza_cleaned: DataFrame) -> DataFrame:
    df = silver_pizza_cleaned
    selected_columns = [
        "pizza_id",
        "order_date",
        "order_time"

    ]
    logging.info("Selecting relevant columns for silver_timestamp...")

    df_selected = df.select(*selected_columns)

    return df_selected



bronze_df = read_delta_stream(spark, "lakehouse", "bronze", "pizza_sales")

cleaned_df = silver_pizza_cleaned(bronze_df)
# write_stream_to_lake(cleaned_df, bucket = BUCKET, layer = LAYER, table_name ="silver_pizza_cleaned")
# write_stream_to_lake(silver_order_items(cleaned_df),bucket = BUCKET, layer = LAYER, table_name ="silver_order_items")
# write_stream_to_lake(silver_pizza_catalog(cleaned_df),bucket = BUCKET, layer = LAYER, table_name ="silver_pizza_catalog")
# write_stream_to_lake(silver_timestamp(cleaned_df), bucket = BUCKET, layer = LAYER, table_name ="silver_timestamp")


# ---------- 5. Chạy theo đối số ----------
target = sys.argv[1]            # ex: silver_order_items

if target == "silver_pizza_cleaned":
    write_stream_to_lake(cleaned_df, bucket = BUCKET, layer = LAYER, table_name ="silver_pizza_cleaned")

elif target == "silver_order_items":
    write_stream_to_lake(silver_order_items(cleaned_df),bucket = BUCKET, layer = LAYER, table_name ="silver_order_items")

elif target == "silver_pizza_catalog":
    write_stream_to_lake(silver_pizza_catalog(cleaned_df),bucket = BUCKET, layer = LAYER, table_name ="silver_pizza_catalog")

elif target == "silver_timestamp":
    write_stream_to_lake(silver_timestamp(cleaned_df), bucket = BUCKET, layer = LAYER, table_name ="silver_timestamp")

else:
    raise ValueError(f"Unknown table: {target}")