import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld,
                               DateType as Date, FloatType as Float)
from pyspark.sql.functions import col


def create_spark_session(aws_key, aws_secret_key):

    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.executor.heartbeatInterval", "20s") \
        .getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl",
                                                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                                      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", aws_secret_key)
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint", "s3.amazonaws.com")
    return spark


if __name__ == "__main__":
    s3_bucket = sys.argv[1]
    s3_key = sys.argv[2]
    aws_key = sys.argv[3]
    aws_secret_key = sys.argv[4]
    redshift_conn_string = sys.argv[5]
    db_user = sys.argv[6]
    db_pass = sys.argv[7]

    spark = create_spark_session(aws_key, aws_secret_key)

    history_df = spark.read.option("header", "true") \
        .csv("s3a://{}/{}/athlete_events.csv".format(s3_bucket, s3_key))
    region_df = spark.read.option("header", "true") \
        .csv("s3a://{}/{}/noc_regions.csv".format(s3_bucket, s3_key))

    history_df.createOrReplaceTempView("olympics_history")
    region_df.createOrReplaceTempView("olympics_history_noc_regions")

    medal_df = spark.sql('''
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

    medal_df.write \
        .format("jdbc")  \
        .option("url", redshift_conn_string) \
        .option("dbtable", "olympics.korea_medal") \
        .option("user", sys.argv[6]) \
        .option("password", sys.argv[7]) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()

    medal_df.show()
    spark.stop()
