from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window
# from pyspark.sql.functions import lead, lag
import os

# parsedData = '/opt/airflow/sparkFiles/parsedData.csv'
jdbcURL = "jdbc:redshift://redshifthost:5439/database?user=username&password=pass"
tempS3Dir = "s3://path/for/temp/data"


spark = SparkSession \
    .builder \
    .appName("read_by_redshift") \
    .getOrCreate()

df = spark.read.csv(parsedData,
                    header='true',
                    inferSchema='true',
                    ignoreLeadingWhiteSpace=True,
                    ignoreTrailingWhiteSpace=True)

df = spark.read \
    .format("com.qubole.spark.redshift") \
    .option("url", jdbcURL) \
    .option("dbtable", "tbl") \
    .option("forward_spark_s3_credentials", "true")
.option("tempdir", tempS3Dir) \
    .load()
