from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 3,                           
    "retry_delay": timedelta(seconds=20),     
}

with DAG(
    dag_id="HN_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL Hacker News",
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python3 /opt/airflow/ETL/extract.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python3 /opt/airflow/ETL/transform.py /opt/airflow/data/raw/$(ls -t /opt/airflow/data/raw | head -1)",
    )

    load = BashOperator(
        task_id="load",
        bash_command="python3 /opt/airflow/ETL/load.py",
    )

    extract >> transform >> load
