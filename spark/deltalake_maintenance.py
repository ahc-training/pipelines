from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from delta.tables import DeltaTable


def main():
    spark = SparkSession.builder.getOrCreate()
    path = spark.conf.get("spark.deltalake.maintenance")

    ## Pythonic way
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.optimize().executeCompaction()
    deltaTable.vacuum(168)

    ## SQL way 
    # spark.sql(f"OPTIMIZE delta.`{path}`")
    # spark.sql(f"VACUUM delta.`{path}` RETAIN 72 HOURS")

if __name__ == "__main__":
    main()