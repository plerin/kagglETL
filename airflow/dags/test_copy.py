from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException

from plugins.operators.s3_to_redshift_operator import S3ToRedshiftOperator
# from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
    dag_id='test_copy',
    default_args=default_args,
    start_date=datetime(2021, 1, 30, tzinfo=kst),
    schedule_interval='9/5 * * * *',
    tags=['test'],
    max_active_runs=5,
    concurrency=2,
    catchup=True
) as dag:

    s3_to_redshift_olympics_history = S3ToRedshiftOperator(
        task_id="s3_to_redshift_olympics_history",
        s3_bucket='kaggletl',
        s3_key='olympics/athlete_events.csv',
        schema='kaggle_data',
        table='olympics_history',
        copy_options=["csv"],
        redshift_conn_id="redshift_dev_db",
        primary_key="",
        order_key="",
        truncate_table=True
    )

    s3_to_redshift_olympics_regions = S3ToRedshiftOperator(
        task_id="s3_to_redshift_olympics_regions",
        s3_bucket='kaggletl',
        s3_key='olympics/noc_regions.csv',
        schema='kaggle_data',
        table='olympics_history_noc_regions',
        copy_options=["csv"],
        redshift_conn_id="redshift_dev_db",
        primary_key="",
        order_key="",
        truncate_table=True
    )
    s3_to_redshift_olympics_history >> s3_to_redshift_olympics_regions
