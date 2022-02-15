from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window
# from pyspark.sql.functions import lead, lag
# import pyspark.sql.SaveMode as SaveMode
import os

event_path = '/opt/airflow/sparkFiles/data/athlete_events.csv'
region_path = '/opt/airflow/sparkFiles/data/noc_regions.csv'

spark = SparkSession \
    .builder \
    .appName("test_spark") \
    .getOrCreate()

df_event = spark.read.csv(event_path,
                          header='true',
                          inferSchema='true',
                          ignoreLeadingWhiteSpace=True,
                          ignoreTrailingWhiteSpace=True)

df_region = spark.read.csv(region_path,
                           header='true',
                           inferSchema='true',
                           ignoreLeadingWhiteSpace=True,
                           ignoreTrailingWhiteSpace=True)

df_region.coalesce(1)\
    .write.option("header", True)\
    .option('escape', '"')\
    .csv('/opt/airflow/sparkFiles/data/olympics/results')
# .mode(SaveMode.Overwrite)\

# delete the parsed data csv from the working directory
# os.remove(parsedData)
