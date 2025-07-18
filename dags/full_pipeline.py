from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "trang",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pizza_streaming_pipeline",
    default_args=default_args,
    description="Pipeline Spark Streaming: Kafka -> Bronze -> Silver -> Gold",
    schedule_interval="@once",  
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["streaming", "spark", "elt"]
) as dag:


    # bronze_layer
    kafka_to_bronze = BashOperator(
        task_id="bronze_pizza_sales",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/kafka_to_bronze.py",
    )


    ## silver layer
    silver_cleaned = BashOperator(
        task_id="silver_pizza_cleaned",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/silver_layer.py silver_pizza_cleaned"
    )

    silver_order_items = BashOperator(
        task_id="silver_order_items",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/silver_layer.py silver_order_items"
    )

    silver_pizza_catalog = BashOperator(
        task_id="silver_pizza_catalog",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/silver_layer.py silver_pizza_catalog"
    )

    silver_timestamp = BashOperator(
        task_id="silver_timestamp",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/silver_layer.py silver_timestamp"
    )


    # gold_layer 

    gold_dim_date = BashOperator(
        task_id="gold_dim_date",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/gold_layer.py gold_dim_date"
    )

    gold_dim_time = BashOperator(
        task_id="gold_dim_time",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/gold_layer.py gold_dim_time"
    )

    gold_dim_pizza = BashOperator(
        task_id="gold_dim_pizza",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/gold_layer.py gold_dim_pizza"
    )

    gold_fact_order_item = BashOperator(
        task_id="gold_fact_order_item",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/gold_layer.py gold_fact_order_item"
    )


    # warehouse
    load_to_psql = BashOperator(
        task_id = "load_to_psql",
        bash_command ="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/warehouse.py"
    )


# 1. Bronze -> Silver
kafka_to_bronze >> silver_cleaned
silver_cleaned  >> [silver_order_items, silver_pizza_catalog, silver_timestamp]

# 2. Gold dimensions
silver_timestamp     >> [gold_dim_date, gold_dim_time]
silver_pizza_catalog >>  gold_dim_pizza        # chỉ dim_pizza cần

# 3. Fact detail
[gold_dim_date, gold_dim_time, gold_dim_pizza, silver_order_items] >> gold_fact_order_item

# 4. Fact aggregate / daily KPI

[gold_dim_date, gold_dim_time, gold_dim_pizza, gold_fact_order_item] >> load_to_psql
