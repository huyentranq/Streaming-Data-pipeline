from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "trang",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_layer_pipeline",
    default_args=default_args,
    description="Bronze Layer: Kafka -> Bronze",
    schedule_interval="@once",
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["bronze", "streaming", "spark"]
) as dag:

    kafka_to_bronze = BashOperator(
        task_id="bronze_pizza_sales",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/kafka_to_bronze.py",
    )

    trigger_silver_dag = TriggerDagRunOperator(
        task_id="trigger_silver_layer_pipeline",
        trigger_dag_id="silver_layer_pipeline"
    )

    kafka_to_bronze >> trigger_silver_dag
    