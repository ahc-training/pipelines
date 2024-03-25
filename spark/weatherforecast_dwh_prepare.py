import json
import os
import logging
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType, TimestampType, LongType
from pyspark.sql.functions import col, to_timestamp, to_str


def main():
    spark = SparkSession.builder.getOrCreate()

    filter = spark.read.format("delta").load("s3a://weatherforecast/data").groupBy(['country', 'region', 'city']).agg(F.max('modifiedon').alias('modifiedon'))
    df = spark.read.format("delta").load("s3a://weatherforecast/streaming")
    df_select = df.select(col("location.name").alias("city"), 
                col("location.region").alias("region"), 
                col("location.country").alias("country"), 
                col("current.temp_c").alias("temperature"), 
                col("current.wind_kph").alias("windspeed"), 
                col("current.last_updated").alias("modifiedon")).where(
                    (df.location.country == "Netherlands") | (df.location.country == "Belgium")
                )
    df_filtered = df_select.alias('data').join(filter, ['country', 'region', 'city'], 'leftouter').where(df_select['modifiedon'] > filter['modifiedon']).select('data.*')
    df_filtered.write.format("delta").mode("append").partitionBy("country", "region", "city").save("s3a://weatherforecast/data")


if __name__ == "__main__":
    main()