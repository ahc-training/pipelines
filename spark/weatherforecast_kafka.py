import json, httpx, pathlib, avro.schema, logging
from enum import Enum
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.avro.functions import from_avro
import avro.schema


spark = SparkSession.builder.getOrCreate()
delta_path = "s3a://weatherforecast/streaming"
checkpoint_path = "s3a://weatherforecast/checkpoint"
fromAvroOptions = {"mode":"PERMISSIVE"}


def get_schema():
  url = "https://schemas.devops.svc:8443/avro/weatherforecast"
  headers = { 'Authorization': 'Basic dmFncmFudDp2YWdyYW50' }
  response = httpx.request("GET", url, headers=headers, verify=False)
  return json.loads(response.text)


def get_streaming_data():
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", spark.conf.get("spark.kafka_controller"))
        .option("startingOffsets", "earliest")
        .option("subscribe", spark.conf.get("spark.kafka_topic"))
        .option("group.id", spark.conf.get("spark.kafka_group_id"))
        .load())


def log_schema(df):
    logging.info(df.printSchema())
    return df


def decode_value(df):
    return df.select(from_avro(col("value"), get_schema(), fromAvroOptions).alias("parsed_value")).select("parsed_value.*")
    

def write_streaming_data(df):
    return (df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .start(delta_path))


def main():
    df = write_streaming_data(decode_value(log_schema(get_streaming_data())))
    df.awaitTermination()

if __name__ == "__main__":
    main()

