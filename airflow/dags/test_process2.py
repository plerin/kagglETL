from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException

from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import pendulum
import logging
import json


kst = pendulum.timezone("Asia/Seoul")

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


default_args = {
    'owner': 'plerin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='test_process2',
    default_args=default_args,
    start_date=datetime(2021, 1, 30, tzinfo=kst),
    schedule_interval='9/5 * * * *',
    tags=['test'],
    max_active_runs=5,
    concurrency=2,
    catchup=True
) as dag:

    params['python_script'] = 'connect_aws.py'
    process_summary_table = BashOperator(
        task_id='connect_aws',
        bash_command='./bash_scripts/load_staging_table.sh',
        params=params
    )
