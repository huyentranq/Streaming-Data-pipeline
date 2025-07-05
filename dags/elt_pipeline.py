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
    schedule_interval="@once",  # Có thể đổi thành "*/5 * * * *" nếu muốn chạy định kỳ
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["streaming", "spark", "etl"]
) as dag:


    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/kafka_to_bronze.py",
    )
    # bronze_to_silver = BashOperator(
    #     task_id="bronze_to_silver",
    #     bash_command="spark-submit /opt/airflow/scripts/bronze_to_silver.py",
    #     dag=dag,
    # )

    # silver_to_gold = BashOperator(
    #     task_id="silver_to_gold",
    #     bash_command="spark-submit /opt/airflow/scripts/silver_to_gold.py",
    #     dag=dag,
    # )

    kafka_to_bronze ## >> bronze_to_silver >> silver_to_gold
