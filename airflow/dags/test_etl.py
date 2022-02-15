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


'''
ETL

Extract
- collect data(olympics) from kaggle api 
- store into s3 _ origin data store in s3(datalake)

Transform
- call the task of bashoperator for processing data using pysparkSQL
- write result data save in file format (where ? ) 
- remove origin file

Load
- read data results in transform and load into s3
- connect redshift 
- copy from s3 to redshift
- remove file

extra
1. connect slack when occurring error
2. add task for making summary table 
3. ...
'''


s3_config = Variable.get("aws_s3_config", deserialize_json=True)


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def load_raw_data_into_s3(**context):
    logging.info('[START_TASK]_load_raw_data_into_s3')

    hook = S3Hook()
    bucket = s3_config['bucket']
    path = '/opt/airflow/sparkFiles/data'

    for file_path in Path(path).glob("*.csv"):
        file_name = str(file_path).split('/')[-1]
        hook.load_file(file_path, 'olympics/raw/'+file_name,
                       bucket_name=bucket, replace=True)

    logging.info('[END_TASK]_load_raw_data_into_s3')


def load_process_data_into_s3(**context):
    logging.info('[START_TASK]_load_process_data_into_s3')

    hook = S3Hook()
    bucket = s3_config['bucket']
    path = '/opt/airflow/sparkFiles/data/olympics/results'

    for file_path in Path(path).glob("*.csv"):
        logging.info(file_path)
        hook.load_file(file_path, 'olympics/process/results.csv',
                       bucket_name=bucket, replace=True)
        # file_name = str(file_path).split('/')[-1]
        # hook.load_file(file_path, 'olympics/process/'+file_name,
        #                bucket_name=bucket, replace=True)

    logging.info('[END_TASK]_load_process_data_into_s3')


def load_into_redshift(**context):
    logging.info('[START_TASK]_load_into_redshift')

    # results = '/opt/airflow/sparkFiles/olympics/results.csv'

    # load into s3
    hook = S3Hook()
    bucket = s3_config['bucket']
    path = '/opt/airflow/sparkFiles/olympics/results'

    for file_path in Path(path).glob("*.csv"):
        logging.info(file_path)
        hook.load_file(file_path, 'olympics/process/results.csv',
                       bucket_name=bucket, replace=True)
        # file_name = str(file_path).split('/')[-1]
        # hook.load_file(file_path, 'olympics/process/'+file_name,
        #                bucket_name=bucket, replace=True)

    # copy from s3 to redshift
    cur = get_Redshift_connection()

    sql = f"""
        COPY {schema}.{table}
        FROM 's3://{s3_bucket}/{s3_key}'
        with credentials
        IGNOREHEADER 1
    """
    try:
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise AirflowException(e)
    # conn.commit()
    cur.close()

    # remove file
    # os.remove(results)

    logging.info('[END_TASK]_load_into_redshift')


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
        bash_command='''cd /opt/airflow/sparkFiles/data;
        rm -rf ./*;
        kaggle datasets download -d heesoo37/120-years-of-olympic-history-athletes-and-results;
        unzip 120-years-of-olympic-history-athletes-and-results.zip
        '''
    )

    load_raw_data_into_s3 = PythonOperator(
        task_id='load_raw_data_into_s3',
        python_callable=load_raw_data_into_s3
    )

    process_summary_table = BashOperator(
        task_id='process_summary_table',
        bash_command='python /opt/airflow/sparkFiles/test_spark.py'
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
        table='olympics_history_noc_regions',
        copy_options=["csv"],
        redshift_conn_id="redshift_dev_db",
        primary_key="",
        order_key="",
        truncate_table=True
    )

    endRun = DummyOperator(
        task_id='endRun',
        trigger_rule='none_failed_or_skipped'
    )

    download_data >> load_raw_data_into_s3
    load_raw_data_into_s3 >> process_summary_table
    process_summary_table >> load_process_data_into_s3
    # load_process_data_into_s3
    load_process_data_into_s3 >> copy_s3_to_redshift
    copy_s3_to_redshift >> endRun
    # copy_olympics_noc_regions >> endRun
