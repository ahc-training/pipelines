import json
import os
import logging
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType, TimestampType


deltalake_path = "s3a://weatherforecast/data"


def process_data(df):
    return df.groupBy(['country', 'region', 'city']).agg(
            F.avg('temperature').alias('temperature'),
            F.avg('windspeed').alias('windspeed'),
            F.count('modifiedon').alias('number_of_rows'),
            F.max('modifiedon').alias('modifiedon')
        )

def write_to_postgresql(df, properties):
    df.write.format("jdbc").options(**properties).mode("overwrite").save()

def main():
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"REFRESH TABLE delta.`{deltalake_path}`")

    connection_properties = {
        "url": f"{spark.conf.get('spark.postgresql_url')}/dwh",
        "user": spark.conf.get("spark.postgresql_usr"),
        "password": spark.conf.get("spark.postgresql_pwd"),
        "driver": "org.postgresql.Driver",
        "dbtable": "weatherforecast"
    }
 
    df = spark.read.format("delta").load(deltalake_path)
    processed_df = process_data(df)
    write_to_postgresql(processed_df, properties=connection_properties)


if __name__ == "__main__":
    main()