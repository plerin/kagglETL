from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException

from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import pendulum
import logging
import json


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'plerin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='test_process',
    default_args=default_args,
    start_date=datetime(2021, 1, 30, tzinfo=kst),
    schedule_interval='9/5 * * * *',
    tags=['test'],
    max_active_runs=5,
    concurrency=2,
    catchup=True
) as dag:

    process_summary_table = BashOperator(
        task_id='process_summary_table',
        bash_command='python /opt/airflow/sparkFiles/test_spark.py'
    )
