"""
IngestIQ Data Loader - Multi-Cloud
"""
from datetime import datetime
import pytz
from utils.secrets import SecretsManager

class DataLoader:
    def __init__(self, config):
        self.config = config
        self.cloud_provider = config.get("cloud_provider", "aws")
        self.secrets = SecretsManager(self.cloud_provider)
        self.sf_credentials = self.secrets.get_secret("snowflake")
    
    def load_to_storage(self, df, table_name, load_type):
        """Load data to cloud storage"""
        if self.cloud_provider == "aws":
            self._load_to_s3(df, table_name, load_type)
        elif self.cloud_provider == "azure":
            self._load_to_adls(df, table_name, load_type)
        elif self.cloud_provider == "gcp":
            self._load_to_gcs(df, table_name, load_type)
    
    def _load_to_s3(self, df, table_name, load_type):
        """Load data to S3"""
        target_path = self._get_storage_path("s3", table_name)
        write_mode = "overwrite" if load_type == "full_load" else "append"
        
        df.write \
            .mode(write_mode) \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", "1000000") \
            .parquet(target_path)
    
    def _load_to_adls(self, df, table_name, load_type):
        """Load data to Azure Data Lake Storage"""
        target_path = self._get_storage_path("adls", table_name)
        write_mode = "overwrite" if load_type == "full_load" else "append"
        
        df.write \
            .mode(write_mode) \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", "1000000") \
            .parquet(target_path)
    
    def _load_to_gcs(self, df, table_name, load_type):
        """Load data to Google Cloud Storage"""
        target_path = self._get_storage_path("gcs", table_name)
        write_mode = "overwrite" if load_type == "full_load" else "append"
        
        df.write \
            .mode(write_mode) \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", "1000000") \
            .parquet(target_path)
    
    def _get_storage_path(self, storage_type, table_name):
        """Get cloud storage path"""
        ist = pytz.timezone("Asia/Kolkata")
        current_time = datetime.now(ist)
        
        source_type = self.config["source_type"]
        database_name = self.config["database_name"]
        
        if storage_type == "s3":
            bucket = self.config["bucket_name"]
            return f"s3://{bucket}/{source_type}/{database_name}/{table_name}/{current_time.strftime('%Y-%m-%d')}/{current_time.strftime('%H')}/"
        
        elif storage_type == "adls":
            storage_account = self.config["storage_account"]
            return f"abfss://data@{storage_account}.dfs.core.windows.net/{source_type}/{database_name}/{table_name}/{current_time.strftime('%Y-%m-%d')}/{current_time.strftime('%H')}/"
        
        elif storage_type == "gcs":
            bucket = self.config["gcs_bucket"]
            return f"gs://{bucket}/{source_type}/{database_name}/{table_name}/{current_time.strftime('%Y-%m-%d')}/{current_time.strftime('%H')}/"
    
    def load_to_snowflake(self, df, table_name, load_type):
        """Load data to Snowflake"""
        sf_options = {
            "sfURL": "vdqiiha-zc94797.snowflakecomputing.com",
            "sfDatabase": "RAW",
            "sfSchema": self.config["sf_schema"],
            "sfWarehouse": "PRD_INGESTION_WH",
            "sfRole": "PRD_ETL_DEVOPS",
            "sfUser": self.sf_credentials["USERNAME"],
            "sfPassword": self.sf_credentials["PASSWORD"]
        }
        
        write_mode = "overwrite" if load_type == "full_load" else "append"
        
        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", table_name.upper()) \
            .mode(write_mode) \
            .save()