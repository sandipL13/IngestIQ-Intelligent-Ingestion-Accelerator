from awsglue.context import GlueContext
from pyspark.context import SparkContext

def get_spark():
    sc = SparkContext.getOrCreate()
    return GlueContext(sc).spark_session
