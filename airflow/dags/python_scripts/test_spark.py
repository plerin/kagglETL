from pyspark.sql import SparkSession

from pathlib import Path
import os

input_data_path = '/opt/airflow/data'
event_path = input_data_path + '/olympics/athlete_events.csv'
region_path = input_data_path + '/olympics/noc_regions.csv'

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

df_event.createOrReplaceTempView("olympics_history")
df_region.createOrReplaceTempView("olympics_history_noc_regions")


korea_df = spark.sql('''
    select 
        sport,
        count(case when medal='Gold' then 1 else NULL end) as gold ,
        count(case when medal='Silver' then 1 else NULL end) as silver ,
        count(case when medal='Bronze' then 1 else NULL end) as bronze,
        count(case when medal <> 'NA' then 1 else NULL end) as total
    from olympics_history as oh
    join olympics_history_noc_regions as nr on nr.noc = oh.noc
    where nr.noc = 'KOR'
    group by 1;
''')

result_path = input_data_path + '/olympics/results'

korea_df.coalesce(1)\
    .write.option("header", True)\
    .option('escape', '"')\
    .mode('overwrite')\
    .csv(result_path)

# for file_path in Path(input_data_path).glob("*.csv"):
#     os.remove(file_path)
