from pyspark.sql.dataframe import DataFrame
from spark_job.resources.spark_manager import *
from spark_job.resources.minio_manager import *
from pyspark.sql import functions as F
import os
POSTGRESQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST") ,     
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

print("✅ POSTGRESQL_CONFIG =", POSTGRESQL_CONFIG)

config = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

def write_to_postgres(df: DataFrame, schema: str, table: str):
    full_table = f"{schema}.{table}"
    df.write \
      .format("jdbc") \
      .option("url", f"jdbc:postgresql://{POSTGRESQL_CONFIG['host']}:{POSTGRESQL_CONFIG['port']}/{POSTGRESQL_CONFIG['database']}") \
      .option("dbtable", full_table) \
      .option("user", POSTGRESQL_CONFIG["user"]) \
      .option("password", POSTGRESQL_CONFIG["password"]) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

spark = create_spark_session(config, "warehouse")  # Ensure SparkSession is initialized
dim_date = read_delta_batch(spark, bucket = "lakehouse",layer = "gold", table_name  ="gold_dim_date")
dim_time = read_delta_batch(spark, bucket = "lakehouse",layer = "gold", table_name  ="gold_dim_time")
dim_pizza = read_delta_batch(spark, bucket = "lakehouse",layer = "gold", table_name  ="gold_dim_pizza")
fact_order_item = read_delta_batch(spark, bucket = "lakehouse",layer = "gold", table_name  ="gold_fact_order_item")

write_to_postgres(dim_date, "pizza_sales", "dim_date")
write_to_postgres(dim_time, "pizza_sales", "dim_time")
write_to_postgres(dim_pizza, "pizza_sales", "dim_pizza")
write_to_postgres(fact_order_item, "pizza_sales", "fact_order_item")


