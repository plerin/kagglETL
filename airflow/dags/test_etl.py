from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator

from datetime import datetime, timedelta

import pendulum
import logging
import json
# get files
from pathlib import Path
import os

s3_config = Variable.get("aws_s3_config", deserialize_json=True)


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def connect_s3(**context):
    logging.info('[START_TASK]_connect_s3')

    # setting s3
    hook = S3Hook()
    bucket = s3_config['bucket']
    path = '/home/airflow/.kaggle/data'
    # get data in local
    for file_path in Path(path).glob("*.csv"):
        file_name = str(file_path).split('/')[-1]
        # logging.info(file_path)
        # logging.info(file_name)
        hook.load_file(file_path, 'olympics/'+file_name,
                       bucket_name=bucket, replace=True)
    logging.info('[END_TASK]_connect_s3')
    pass


def connect_redshift(**context):
    logging.info('[START_TASK]_connect_redshift')

    cur = get_Redshift_connection()

    # read by s3
    hook = S3Hook()
    bucket = s3_config['bucket']
    data = []
    for key in hook.list_keys(bucket, prefix='olympics/'):
        logging.info(key)

        data.append(hook.read_key(key=key, bucket_name=bucket))

    for i in range(5):
        logging.info(data[i])
    logging.info(data[1][:100])
    logging.info(data[2][:100])
    # data = hook.read_key(key=key, bucket_name=bucket)

    logging.info('[END_TASK]_connect_redshift')
    pass


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'plerin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
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
        bash_command='''cd /home/airflow/.kaggle/data;
        rm -rf ./*;
        kaggle datasets download -d heesoo37/120-years-of-olympic-history-athletes-and-results;
        unzip 120-years-of-olympic-history-athletes-and-results.zip
        '''
    )

    connect_s3 = PythonOperator(
        task_id='connect_s3',
        python_callable=connect_s3
    )

    # connect_redshift = PythonOperator(
    #     task_id='connect_redshift',
    #     python_callable=connect_redshift
    # )

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

    download_data >> connect_s3 >> s3_to_redshift_olympics_history >> s3_to_redshift_olympics_regions
    # download_data >> connect_s3 >> s3_to_redshift_olympics_regions
    # s3_to_redshift_olympics_regions
