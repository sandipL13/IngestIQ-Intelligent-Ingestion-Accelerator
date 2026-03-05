"""
IngestIQ Base Classes
"""
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from utils import Logger, SecretsManager
from catalog.catalog_manager import CatalogManager

class Initialization:
    """Base initialization with Spark and Logger"""
    
    def __init__(self, database_name, table_name="JOB_SUMMARY"):
        self.database_name = database_name
        self.table_name = table_name
        self.spark = self._get_spark_session()
        self.logger = None
        self.secrets = SecretsManager()
        self.catalog = CatalogManager()
    
    def _get_spark_session(self):
        """Get optimized Spark session"""
        return SparkSession.builder \
            .config("spark.sql.shuffle.partitions", "128") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.default.parallelism", "128") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .getOrCreate()
    
    def setup_logger(self, bucket_name):
        """Setup logger"""
        self.logger = Logger(bucket_name, self.database_name, self.table_name)
        return self.logger

class AbstractExtract(Initialization):
    """Abstract Extract Class"""
    
    @abstractmethod
    def read_source(self, **kwargs):
        pass
    
    @abstractmethod
    def give_output(self):
        pass

class AbstractTransform(Initialization):
    """Abstract Transform Class"""
    
    @abstractmethod
    def column_name_format(self, df):
        pass
    
    @abstractmethod
    def date_mapping(self, df):
        pass
    
    @abstractmethod
    def output(self, df):
        pass

class AbstractLoad(Initialization):
    """Abstract Load Class"""
    
    @abstractmethod
    def load_df(self, df, table_name, load_type="append"):
        pass