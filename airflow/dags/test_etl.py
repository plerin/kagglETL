from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from plugins.operators.s3_to_redshift_operator import S3ToRedshiftOperator
from plugins.operators.data_quality import DataQualityOperator

from plugins.alert_by_slack import on_failure
from datetime import datetime, timedelta

import pendulum
import logging
import json
# get files
from pathlib import Path
import os
import shutil

s3_config = Variable.get("aws_s3_config", deserialize_json=True)
input_path = '/opt/airflow/data'


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def load_raw_data_into_s3(**context):
    logging.info('[START_TASK]_load_raw_data_into_s3')

    hook = S3Hook()
    bucket = s3_config['bucket']

    for file_path in Path(input_path).glob("*.csv"):
        file_name = str(file_path).split('/')[-1]
        hook.load_file(file_path, 'olympics/raw/'+file_name,
                       bucket_name=bucket, replace=True)

    logging.info('[END_TASK]_load_raw_data_into_s3')


def load_process_data_into_s3(**context):
    logging.info('[START_TASK]_load_process_data_into_s3')

    hook = S3Hook()
    bucket = s3_config['bucket']
    result_path = input_path + '/results'

    for file_path in Path(result_path).glob("*.csv"):
        logging.info(file_path)
        hook.load_file(file_path, 'olympics/process/results.csv',
                       bucket_name=bucket, replace=True)

    shutil.rmtree(result_path)
    logging.info('[END_TASK]_load_process_data_into_s3')


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'plerin',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure
}

with DAG(
    dag_id='test_etl',
    default_args=default_args,
    start_date=datetime(2021, 1, 30, tzinfo=kst),
    schedule_interval='9/5 * * * *',
    tags=['test'],
    max_active_runs=5,
    concurrency=2,
    catchup=True
) as dag:

    download_data = BashOperator(
        task_id='download_data',
        bash_command='''cd {path};
        rm -rf ./*;
        kaggle datasets download -d heesoo37/120-years-of-olympic-history-athletes-and-results;
        unzip 120-years-of-olympic-history-athletes-and-results.zip;
        rm -rf ./120-years-of-olympic-history-athletes-and-results.zip;
        '''.format(path=input_path)
    )

    load_raw_data_into_s3 = PythonOperator(
        task_id='load_raw_data_into_s3',
        python_callable=load_raw_data_into_s3
    )

    process_summary_table = BashOperator(
        task_id='process_summary_table',
        bash_command='python /opt/airflow/dags/python_scripts/test_spark.py'
    )

    load_process_data_into_s3 = PythonOperator(
        task_id='load_process_data_into_s3',
        python_callable=load_process_data_into_s3
    )

    copy_s3_to_redshift = S3ToRedshiftOperator(
        task_id="copy_s3_to_redshift",
        s3_bucket='kaggletl',
        s3_key='olympics/process/results.csv',
        schema='kaggle_data',
        table='summary_korea_medal',
        copy_options=["csv"],
        redshift_conn_id="redshift_dev_db",
        primary_key="",
        order_key="",
        truncate_table=True
    )

    tables = ["kaggle_data.summary_korea_medal"]
    check_data_quality = DataQualityOperator(task_id='check_data_quality',
                                             redshift_conn_id="redshift_dev_db",
                                             table_names=tables)

    endRun = DummyOperator(
        task_id='endRun',
        trigger_rule='none_failed_or_skipped'
    )

    download_data >> load_raw_data_into_s3
    load_raw_data_into_s3 >> process_summary_table
    process_summary_table >> load_process_data_into_s3
    load_process_data_into_s3 >> copy_s3_to_redshift
    copy_s3_to_redshift >> check_data_quality >> endRun
