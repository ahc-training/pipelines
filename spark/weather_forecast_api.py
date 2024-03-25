import json
import httpx
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType, TimestampType


spark = SparkSession.builder.getOrCreate()

# Details about the api can be found at: https://rapidapi.com/weatherapi/api/weatherapi-com
api_url = "https://weatherapi-com.p.rapidapi.com/forecast.json"
querystring = {"q":"Eindhoven","days":"3"}
headers = {
	"X-RapidAPI-Key": "redacted",
	"X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}
delta_path = "s3a://raw/weatherforecast"


def read_api() -> str:
    with httpx.Client() as client:
        response = client.get(url=api_url, headers=headers, params=querystring)
        return json.loads(response.text)


def main():
    payload = read_api()
    df = spark.read.json(spark.sparkContext.parallelize([payload]))
    df.write.format("delta").mode("overwrite").save(delta_path)

if __name__ == "__main__":
    main()