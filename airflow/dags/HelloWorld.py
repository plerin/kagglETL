from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from datetime import datetime, timedelta
import logging
import pendulum


def print_hello():
    print("hello!")
    return "hello!"


def print_goodbye():
    print("goodbye!")
    return "goodbye!"


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'plerin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    # 'on_failure_callback': on_failure

}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    start_date=datetime(2022, 2, 1, tzinfo=kst),
    schedule_interval='*/1 * * * *',
    tags=['mine'],
    max_active_runs=1,
    concurrency=1,
    catchup=True
) as dag:

    print_hello = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello)

    print_goodbye = PythonOperator(
        task_id='print_goodbye',
        python_callable=print_goodbye)

    print_hello >> print_goodbye
