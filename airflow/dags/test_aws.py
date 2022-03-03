from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

import pendulum
import logging
import json

s3_config = Variable.get("s3_config", deserialize_json=True)


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def connect_s3(**context):
    logging.info('[START_TASK]_connect_s3')

    hook = S3Hook()
    bucket = s3_config['s3_bucket']

    obj = hook.get_key('olympics/noc_regions.csv', bucket_name=bucket)
    logging.info(obj)
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

    logging.info('[END_TASK]_connect_redshift')
    pass


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'plerin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='test_aws',
    default_args=default_args,
    start_date=datetime(2021, 1, 30, tzinfo=kst),
    schedule_interval='9/10 * * * *',
    tags=['test'],
    max_active_runs=5,
    concurrency=2,
    catchup=True
) as dag:

    connect_s3 = PythonOperator(
        task_id='connect_s3',
        python_callable=connect_s3
    )

    # connect_redshift = PythonOperator(
    #     task_id='connect_redshift',
    #     python_callable=connect_redshift
    # )

    connect_s3
    # connect_s3 >> connect_redshift
