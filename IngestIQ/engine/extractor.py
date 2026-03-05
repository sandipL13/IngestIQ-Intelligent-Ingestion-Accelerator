"""
IngestIQ Data Extractor
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from utils.secrets import SecretsManager

class DataExtractor:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()
        self.secrets = SecretsManager(config.get("cloud_provider", "aws"))
        self.credentials = self._get_credentials()
    
    def _get_credentials(self):
        source_type = self.config["source_type"].lower()
        secret_name = f"glue_{source_type}_creds"
        return self.secrets.get_secret(secret_name)
    
    def extract_table(self, table_name, load_type, table_info):
        """Extract data from source table"""
        query = self._build_query(table_name, load_type, table_info)
        
        if not query:
            return None
        
        # Read with partitioning if configured
        if table_info.get("is_partitioned") == "TRUE":
            df = self._read_partitioned(query, table_info)
        else:
            df = self._read_simple(query)
        
        return df.withColumn("insert_timestamp", current_timestamp())
    
    def _build_query(self, table_name, load_type, table_info):
        """Build SQL query based on load type"""
        if load_type == "full_load":
            return f"SELECT * FROM {table_name}"
        
        elif load_type == "cdc_load":
            # Get incremental columns and last timestamp
            incremental_cols = table_info.get("incremental_cols", [])
            last_timestamp = table_info.get("last_timestamp")
            
            if not incremental_cols or not last_timestamp:
                return None
            
            conditions = " OR ".join([
                f"{col} > '{last_timestamp}'" for col in incremental_cols
            ])
            return f"SELECT * FROM {table_name} WHERE {conditions}"
        
        return None
    
    def _read_partitioned(self, query, table_info):
        """Read with JDBC partitioning"""
        partition_col = table_info["partition_col"]
        
        # Get bounds
        bounds_query = f"(SELECT MIN({partition_col}) AS min_val, MAX({partition_col}) AS max_val FROM ({query}) t) AS bounds"
        bounds_df = self._read_simple(bounds_query)
        min_val, max_val = bounds_df.first()
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config["jdbc_url"]) \
            .option("dbtable", f"({query}) AS tmp") \
            .option("user", self.credentials["username"]) \
            .option("password", self.credentials["password"]) \
            .option("partitionColumn", partition_col) \
            .option("lowerBound", int(min_val or 0)) \
            .option("upperBound", int(max_val or 1)) \
            .option("numPartitions", 10) \
            .load()
    
    def _read_simple(self, query):
        """Simple JDBC read"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config["jdbc_url"]) \
            .option("dbtable", f"({query}) AS tmp") \
            .option("user", self.credentials["username"]) \
            .option("password", self.credentials["password"]) \
            .load()