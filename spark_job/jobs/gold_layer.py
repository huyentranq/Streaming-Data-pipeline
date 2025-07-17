from spark_job.resources.spark_manager import *
from spark_job.resources.minio_manager import *
from pyspark.sql.functions import  broadcast
import logging
import os
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
# from pyspark.sql.functions import collect_list, col, udf
# from pyspark.sql.types import StringType
# from datetime import datetime, timedelta
import sys
LAYER = "gold"
FMT = "delta"
BUCKET  = "lakehouse"
config = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

spark = create_spark_session(config, "gold_layer")

# bronze_df = read_delta_stream(spark, "lakehouse", "silver", "pizza_sales")

def gold_dim_date(silver_timestamp: DataFrame) -> DataFrame:
    df = (
        silver_timestamp
        .select("order_date")
        .withColumn("order_date", F.to_timestamp("order_date"))  
        .withColumn("date", F.to_date("order_date"))   # tránh lệch múi giờ
        .distinct()
        .withColumn("date_id",      F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("day",          F.dayofmonth("date"))
        .withColumn("month",        F.month("date"))
        .withColumn("year",         F.year("date"))
        .withColumn("quarter",      F.quarter("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        # dayofweek: Sun=1 … Sat=7  ⟹  ((x + 5) % 7) + 1  ⇒ Mon=1 … Sun=7
        .withColumn("weekday_num", ((F.dayofweek("date") + 5) % 7) + 1)
        .withColumn("weekday_name", F.date_format("date", "EEEE"))
    )

    return df.select(
        "date_id", "order_date", "day", "month", "year",
        "quarter", "week_of_year", "weekday_num", "weekday_name"
    )

def gold_dim_time(silver_timestamp):
    df = (

        silver_timestamp
        .select("order_time")
        .withColumn("order_time", F.to_timestamp("order_time"))      # ép kiểu rõ ràng                                        # hoặc .withWatermark...
        .withColumn("time_id", F.date_format("order_time", "HHmm").cast("int")) # để dạng string
        .dropDuplicates(["time_id"])
        .withColumn("hour",     F.hour("order_time"))
        .withColumn("minute",   F.minute("order_time"))
        .withColumn(
            "timeslot",
            F.when(F.col("hour").between(5, 10),  "Breakfast")
             .when(F.col("hour").between(11,15),  "Lunch")
             .when(F.col("hour").between(16,21),  "Dinner")
             .otherwise("Late‑night")
        )
    )
    return df.select("time_id", "order_time", "hour", "minute", "timeslot")

def gold_dim_pizza(silver_pizza_catalog: DataFrame) -> DataFrame:
    return (
        silver_pizza_catalog
        # .withColumnRenamed("pizza_name_id", "pizza_id")
        .withColumn("pizza_sk", F.crc32(F.col("pizza_name_id"))) # safe for streaming
    )



def gold_fact_order_item(silver_order_items: DataFrame, dim_date: DataFrame,
                         dim_time: DataFrame, dim_pizza: DataFrame) -> DataFrame:
    fact = (
        silver_order_items.alias("f")
        .join(broadcast(dim_date.select("order_date", "date_id")), "order_date")
        .join(broadcast(dim_time.select("order_time", "time_id")), "order_time")
        .join(broadcast(dim_pizza.select("pizza_name_id", "pizza_sk")), "pizza_name_id")
        .select(
            F.to_timestamp("order_date").alias("order_date"),
            F.col("date_id").cast("int"),
            F.col("time_id").cast("int"),
            F.col("pizza_sk").cast("long"),
            F.col("quantity").cast("int"),
            F.col("unit_price").cast("decimal(8,2)"),
            F.col("total_price").cast("decimal(10,2)")
        )
    )
    return fact

# silver_ts = read_delta_stream(spark, "lakehouse", "silver", "silver_timestamp")
# silver_catalog = read_delta_stream(spark, "lakehouse", "silver", "silver_pizza_catalog")
# silver_order = read_delta_stream(spark, "lakehouse", "silver", "silver_order_items")

# dim_date = gold_dim_date(silver_ts)
# dim_time = gold_dim_time(silver_ts)
# dim_pizza = gold_dim_pizza(silver_catalog)


# fact_order_item = gold_fact_order_item(
#     silver_order,
#     dim_date=dim_date,
#     dim_time=dim_time,
#     dim_pizza=dim_pizza
# )



# write_stream_to_lake(dim_date, bucket = BUCKET, layer = LAYER, table_name ="gold_dim_date")
# write_stream_to_lake(dim_time, bucket = BUCKET, layer = LAYER, table_name ="gold_dim_time")
# write_stream_to_lake(dim_pizza, bucket = BUCKET, layer = LAYER, table_name ="gold_dim_pizza")
# # logging.info(f"Is streaming: {fact_order_item.isStreaming}")
# write_stream_to_lake(fact_order_item, bucket = BUCKET, layer = LAYER, table_name ="gold_fact_order_item")


# write_stream_to_lake(fact_daily_sales, bucket = BUCKET, layer = LAYER, table_name ="gold_fact_daily_sales")




target = sys.argv[1]            # ex: silver_order_items

if target == "gold_dim_date":
    silver_ts = read_delta_stream(spark, "lakehouse", "silver", "silver_timestamp")
    dim_date = gold_dim_date(silver_ts)
    write_stream_to_lake(dim_date, bucket = BUCKET, layer = LAYER, table_name ="gold_dim_date")

elif target == "gold_dim_time":
    silver_ts = read_delta_stream(spark, "lakehouse", "silver", "silver_timestamp")
    dim_time = gold_dim_time(silver_ts)
    write_stream_to_lake(dim_time, bucket = BUCKET, layer = LAYER, table_name ="gold_dim_time")

elif target == "gold_dim_pizza":
    silver_catalog = read_delta_stream(spark, "lakehouse", "silver", "silver_pizza_catalog")
    dim_pizza = gold_dim_pizza(silver_catalog)
    write_stream_to_lake(dim_pizza, bucket = BUCKET, layer = LAYER, table_name ="gold_dim_pizza")

elif target == "gold_fact_order_item":
    silver_order = read_delta_stream(spark, "lakehouse", "silver", "silver_order_items")
    silver_ts = read_delta_stream(spark, "lakehouse", "silver", "silver_timestamp")
    silver_catalog = read_delta_stream(spark, "lakehouse", "silver", "silver_pizza_catalog")
    dim_date = gold_dim_date(silver_ts)
    dim_time = gold_dim_time(silver_ts)
    dim_pizza = gold_dim_pizza(silver_catalog)
    fact_order_item = gold_fact_order_item(
        silver_order,
        dim_date=dim_date,
        dim_time=dim_time,
        dim_pizza=dim_pizza
    )
    write_stream_to_lake(fact_order_item, bucket = BUCKET, layer = LAYER, table_name ="gold_fact_order_item")



else:
    raise ValueError(f"Unknown table: {target}")