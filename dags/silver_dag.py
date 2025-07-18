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
    dag_id="silver_layer_pipeline",
    default_args=default_args,
    description="Silver Layer: Cleaned data from Bronze",
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["silver", "spark"]
) as dag:

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

    trigger_gold_dag = TriggerDagRunOperator(
        task_id="trigger_gold_layer_pipeline",
        trigger_dag_id="gold_layer_pipeline"
    )

    silver_cleaned >> [silver_order_items, silver_pizza_catalog, silver_timestamp]
    [silver_order_items, silver_pizza_catalog, silver_timestamp] >> trigger_gold_dag
