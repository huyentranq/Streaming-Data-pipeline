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
    dag_id="gold_layer_pipeline",
    default_args=default_args,
    description="Gold Layer: Dimensions, Facts, Warehouse",
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["gold", "warehouse", "spark"]
) as dag:

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

    load_to_psql = BashOperator(
        task_id="load_to_psql",
        bash_command="docker exec spark-master spark-submit /opt/airflow/scripts/spark_job/jobs/warehouse.py"
    )

    [gold_dim_date, gold_dim_time, gold_dim_pizza] >> gold_fact_order_item
    [gold_dim_date, gold_dim_time, gold_dim_pizza,gold_fact_order_item] >> load_to_psql
