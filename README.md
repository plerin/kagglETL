<h1 align="center">KagglETL</h1>

<p align="center">
    <a href="#Goal">Goal</a> •
    <a href="#Senario">Senario</a> •
    <a href="#Tech_Stack">Tech_Stack</a> •
    <a href="#Set-up">Set-up</a> •
    <a href="#Process">Process</a> •
    <a href="#License">License</a>
</p>

<br>

# 🚩Goal

This project is based on ETL(data-pipeline). collecting data from kaggle_api then load raw_data into s3, make a summary table for analytical activity using `sparkSQL`, finally process data load into redshift. now let's analylize summary data using BI tools(`preset.io`) connecting the redshift.

**_SparkSQL_**

- Do processing during the task of transform

**_Preset.io_**

- View graph with data connecting database, in this case Redshift!

<br>

# 📒Senario

**_Architecture_**

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/architecture.png"></p>

**_Data_**

- contents : [the 120 years of Olympics History](https://www.kaggle.com/heesoo37/120-years-of-olympic-history-athletes-and-results#, "olympics data link")
- format : zip
- size : 42mb

## ETL

**_Extact_**

- Collecting the olympics data using kaggle api in local
- Load the kaggle data into aws s3

**_Transform_**

- create schema and tables for summary-table
- call the process with spark

**_Load_**

- load process-data into the aws redshift
- check the data-quality

**_Analysis_**

- view summary-table using BI tools

  <br>

# 📚Tech_Stack

- **_Docker_**
- **_Spark_**
- **_AWS S3_**
- **_AWS RedShift_**
- **_Airflow_**
- **_Python_**
- **_Git_**

# Set-up

## prerequirement

<br>

**_Kaggle_**

- file : cred/kaggle/kaggle.json
- Need getting the key for collecting data

  ```
  {"username":"[username]","key":"[key]"}
  ```

**_AWS_**

- file : creds/aws/credentials
- Input access_key and secret_key for connecting aws in airflow

  ```
  [default]
  aws_access_key_id = [access_key_id]
  aws_secret_access_key = [secret_key]
  ```

## config

<br>

### **Variables**

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/airflow_variable.png"></p>

**_AWS_**

- components : `aws_key`, `aws_secret_key`
- in_code :

  ```Python
  aws_config = Variable.get("aws_config", deserialize_json=True)

  aws_key : aws_config["aws_key"]
  aws_secret_key : aws_config["aws_secret_key"],
  ```

**_S3_**

- components : `s3_bucket`, `s3_key`
- in_code :

  ```Python
  s3_config = Variable.get("s3_config", deserialize_json=True)

  s3_bucket : s3_config["s3_bucket"]
  s3_key : s3_config["s3_key"]
  ```

**_Redshift_**

- components : `db_user`, `db_pass`, `conn_string`
- in_code :

  ```Python
  redshift_config = Variable.get("redshift_config", deserialize_json=True)

  db_user : redshift_config.get("db_user")
  db_pass : redshift_config.get("db_pass")
  redshift_conn_string : redshift_config.get("conn_string")
  ```

**_Slack_**

- components : `channel`, `token_key`
- in_code :

  ```Python
  config_slack = Variable.get("slack_config", deserialize_json=True)

  channel = config_slack['channel']
  token = config_slack['token_key']
  ```

### **Connection**

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/setup_airflow_connection_2.jpg"></p>

**_Redshift_**

- purpose : Query to Redshift using PostgresHook

  ```
  hook = PostgresHook(postgres_conn_id='redshift_dev_db')
  ```

# Process

  <p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/dag_process.png"></p>

## **Task**

**Task `start_operator`**

- Operator : DummyOperator
- Contents : Staring the dag

**Task `download_data`**

- Operator : BashOperator
- Contents : Collecting the data in kaggle for processing

```python
bash_command='''mkdir {path}/olympics;
        cd {path}/olympics;
        kaggle datasets download -d heesoo37/120-years-of-olympic-history-athletes-and-results;
        unzip 120-years-of-olympic-history-athletes-and-results.zip;
        '''.format(path=input_path)
```

**Task `load_raw_data_into_s3`**

- Operator : PythonOperator
- Contents :
  1. Load collecting data into the aws s3 for processing
  2. remove the data

```python

    hook = S3Hook()
    bucket = s3_config['s3_bucket']
    key = s3_config['s3_key']

    for file_path in Path(input_path).glob("*.csv"):
        file_name = str(file_path).split('/')[-1]
        hook.load_file(file_path, key+'/raw/'+file_name,
                       bucket_name=bucket, replace=True)

    # delete data directory
    shutil.rmtree(input_path+"/olympics")
```

**Task `create_schema`**

- Operator : PostgresOperator
- Contents : create the schema if not exists for summary-table

```sql
BEGIN;

CREATE SCHEMA IF NOT EXISTS olympics;

END;
```

**Task `create_table`**

- Operator : PostgresOperator
- Contents : create the table if not exists for summary-table

```sql
BEGIN;

DROP TABLE IF EXISTS olympics.korea_medal;

CREATE TABLE IF NOT EXISTS olympics.korea_medal (
    sport VARCHAR(255),
    gold bigint NOT NULL,
    silver bigint NOT NULL,
    bronze bigint NOT NULL,
    total bigint NOT NULL,
    primary key (sport)
) diststyle key distkey(sport);

END;
```

**Task `process_korea_medal`**

- Operator : BashOperator
- Contents : Processing the data for making summary-table using pySparkSQL
- process :
  1. Run the spark-submit using bash
  2. Create spark-session and config for connecting aws
  3. Import data in s3
  4. Processing data using pySparkSQL
  5. Load into redshift

```python
params['python_script'] = 'process_korea_medal.py'
    process_korea_medal = BashOperator(
        task_id='process_korea_medal',
        bash_command='./bash_scripts/load_staging_table.sh',
        params=params
    )
```

**Task `check_data_quality`**

- Operator : DataQualityOperator
- Contents :
  1. Check the data through counting row
  2. Check the data through checking null

```python
    # check the data quality
    tables = ["olympics.korea_medal"]
    check_data_quality = DataQualityOperator(task_id='check_data_quality',
                                             redshift_conn_id="redshift_dev_db",
                                             table_names=tables)
```

**Task `endRun`**

- Operator : DummyOperator
- Contents : Ending the dag

## **Analysis**

**_preset.io_**

- purpose : Expressing the graph for analysis
- data : The amount of medal with Korea in 1896 ~ 2016 olympics

  <p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/result_02.png""></p>

# License

You can check out the full license [here](https://github.com/plerin/kagglETL/blob/main/LICENSE)

This project is licensed under the terms of the **MIT** license.
