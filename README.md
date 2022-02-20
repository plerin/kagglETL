<h1 align="center">KagglETL</h1>

<p align="center">
    <a href="#Goal">Goal</a> •
    <a href="#Senario">Senario</a> •
    <a href="#Tech_Stack">Tech_Stack</a> •
    <a href="#Set-up">Set-up</a> •
    <a href="#Process">Process</a>
    <a href="#License">License</a>
</p>

<br>

# 🚩Goal

this project is based on ETL(data-pipeline). collecting data from kaggle_api then load raw_data into s3, make a summary table for analytical activity using `sparkSQL`, finally process data load into s3 and copy to redshift. now let's analylize summary data using BI tools(`preset.io`) connecting the redshift.

**_SparkSQL_**

- Do processing during the task of transform

**_Preset.io_**

- View graph with data connecting db in this case, redshift!

<br>

# 📒Senario

**_Architecture_**

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/architecture.png"></p>

**_Data_**

- contents : [the 120 years of Olympics History](https://www.kaggle.com/heesoo37/120-years-of-olympic-history-athletes-and-results#, "olympics data link")
- format : zip
- size : 42mb

### Flow

**_Extact_**

1. Collecting the olympics data using kaggle api in local
2. Load raw_data into s3

**_Transform_**

1. Process raw_data using sparkSQL(pySpark) so that make the summary table
2. Load process data into local

**_Load_**

1. Read data in local and load into s3 for copy
2. copy from s3 to redshift
3. check the data quality

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

**_kaggle_**  
file : kaggle/kaggle.json  
Need getting the key for collecting data

```
{"username":"[username]","key":"[key]"}
```

**_aws_**  
file : airflow/creds/aws  
Input access_key and secret_key for connecting aws with airflow

```
[aws-info]
aws_access_key_id = [access_key_id]
aws_secret_access_key = [secret_key]
```

## config

<br>

**_Variable_**

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/setup_airflow_variable_2.jpg"></p>

**_Use-case_**

- s3 : `bucket`

      s3_config = Variable.get("aws_s3_config", deserialize_json=True)

      bucket = s3_config['bucket']

- slack : `channel`, `token_key`

      config_slack = Variable.get("slack_config", deserialize_json=True)

      channel = config_slack['channel']
      token = config_slack['token_key']

**_Connection_**

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/setup_airflow_connection_2.jpg"></p>

- redshift

      hook = PostgresHook(postgres_conn_id='redshift_dev_db')

# Process

  <p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_with_weather/dag.png"></p>

## Task

1. airflow dags 이미지
2. 각 task 별 역할 및 처리

**Task `get_data_by_api`**

Collecting data using api request

```python
API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={exclude}&appid={api_key}&units=metric"

 response = requests.get(
        API_URL.format(
            lat=COORD['lat'],
            lon=COORD['lon'],
            exclude=EXCLUDE,
            api_key=API_KEY
        )
    )
```

## Result

1. preset.io 에서 redshift 연동
2. 결과 표출(이미지)

  <p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/kaggletl/result_01.png""></p>

# License

You can check out the full license [here](https://github.com/plerin/kagglETL/blob/main/LICENSE)

This project is licensed under the terms of the **MIT** license.
