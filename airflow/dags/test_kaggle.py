from airflow import DAG
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
    dag_id='test_kaggle',
    default_args=default_args,
    start_date=datetime(2021, 1, 30, tzinfo=kst),
    schedule_interval='9/10 * * * *',
    tags=['test'],
    max_active_runs=5,
    concurrency=2,
    catchup=True
) as dag:

    download_data = BashOperator(
        task_id='download_data',
        bash_command='''cd /home/airflow/.kaggle/data;
        kaggle datasets download -d heesoo37/120-years-of-olympic-history-athletes-and-results;
        unzip 120-years-of-olympic-history-athletes-and-results.zip
        '''
    )

    download_data
