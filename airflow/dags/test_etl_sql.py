from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

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

'''
>> etl

extract
 - collect the data by kaggle
 - load into raw_data into s3
 
transform
 - create the table in redshift
 - call process(spark)

load
 - check the data quality
'''

input_path = '/opt/airflow/data'

s3_config = Variable.get("s3_config", deserialize_json=True)
redshift_config = Variable.get("redshift_config", deserialize_json=True)
aws_config = Variable.get("aws_config", deserialize_json=True)

params = {'aws_key': aws_config["aws_key"],
          'aws_secret_key': aws_config["aws_secret_key"],
          'db_user': redshift_config.get("db_user"),
          'db_pass': redshift_config.get("db_pass"),
          'redshift_conn_string': redshift_config.get("conn_string"),
          's3_bucket': s3_config["s3_bucket"],
          's3_key': s3_config["s3_key"]
          }


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def load_raw_data_into_s3(**context):
    logging.info('[START_TASK]_load_raw_data_into_s3')

    hook = S3Hook()
    bucket = s3_config['s3_bucket']
    key = s3_config['s3_key']

    for file_path in Path(input_path).glob("*.csv"):
        file_name = str(file_path).split('/')[-1]
        hook.load_file(file_path, key+'/raw/'+file_name,
                       bucket_name=bucket, replace=True)

    logging.info('[END_TASK]_load_raw_data_into_s3')


def load_process_data_into_s3(**context):
    logging.info('[START_TASK]_load_process_data_into_s3')

    hook = S3Hook()
    bucket = s3_config['s3_bucket']
    key = s3_config['s3_key']
    result_path = input_path + '/results'

    for file_path in Path(result_path).glob("*.csv"):
        logging.info(file_path)
        hook.load_file(file_path, key+'/process/results.csv',
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
    dag_id='test_etl_sql',
    default_args=default_args,
    start_date=datetime(2021, 1, 30, tzinfo=kst),
    schedule_interval='9/5 * * * *',
    tags=['test'],
    max_active_runs=5,
    concurrency=2,
    catchup=True
) as dag:

    start_operator = DummyOperator(task_id='begin-execution')

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

    create_tables = PostgresOperator(task_id='create-tables', postgres_conn_id="redshift_dev_db",
                                     sql="sql_scripts/create_tables.sql")

    params['python_script'] = 'process_korea_medal.py'
    process_korea_medal = BashOperator(
        task_id='process_korea_medal',
        bash_command='./bash_scripts/load_staging_table.sh',
        params=params
    )

    endRun = DummyOperator(
        task_id='endRun',
        trigger_rule='none_failed_or_skipped'
    )

    start_operator >> download_data >> load_raw_data_into_s3
    load_raw_data_into_s3 >> create_tables
    create_tables >> process_korea_medal
    process_korea_medal >> endRun
    # load_process_data_into_s3 >> copy_s3_to_redshift
    # copy_s3_to_redshift >> check_data_quality >> endRun
