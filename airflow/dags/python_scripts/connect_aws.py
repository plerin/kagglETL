import sys
import os
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld,
                               DateType as Date, FloatType as Float)
from pyspark.sql.functions import col


def create_spark_session(aws_key, aws_secret_key):
    """
    Description: Creates spark session.
    Returns:
        spark session object
    """
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.executor.heartbeatInterval", "40s") \
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

    cpi_df = spark.read.option("header", "true") \
        .csv("s3a://{}/{}/noc_regions.csv".format(s3_bucket, s3_key))

    cpi_df = cpi_df.select(col("NOC").alias("nnoocc"),
                           col("region").alias("country_name"))

    cpi_df.write \
        .format("jdbc")  \
        .option("url", redshift_conn_string) \
        .option("dbtable", "kaggle_data.itsnew") \
        .option("user", sys.argv[6]) \
        .option("password", sys.argv[7]) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()

    cpi_df.show()
    spark.stop()
