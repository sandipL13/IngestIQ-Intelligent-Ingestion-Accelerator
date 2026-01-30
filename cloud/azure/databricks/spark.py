from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder.getOrCreate()
