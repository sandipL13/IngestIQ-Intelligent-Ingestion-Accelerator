"""
scr1_df - Script1 Data Extraction
"""
from jobs import AbstractExtract
from pyspark.sql.functions import current_timestamp
from datetime import datetime, timedelta
from utils import parse_datetime

class Script1Extract(AbstractExtract):
    """Script1 Extract - scr1_df"""
    
    def __init__(self, database_name, table_name):
        super().__init__(database_name, table_name)
        self.jdbc_url = None
        self.credentials = None
        self.source_type = None
        self.datetime_cast = None
        self.df = None
    
    def setup_source(self, source_type, jdbc_url, secret_name):
        """Setup source configuration"""
        self.source_type = source_type.lower()
        self.jdbc_url = jdbc_url
        self.credentials = self.secrets.get_secret(secret_name)
        self.datetime_cast = "DATETIME" if source_type.lower() == "mysql" else "TIMESTAMP"
    
    def read_source(self, load_type="full_load", partition_col=None, is_partitioned="FALSE"):
        """READ_SCR - Read data from source"""
        try:
            self.logger.logger.info(f"Starting extraction: {self.table_name}, load_type={load_type}")
            
            # Get table metadata
            table_map = self.catalog.get_table_load_type_map(self.database_name)
            table_key = (self.database_name, self.table_name)
            
            if table_key not in table_map:
                raise ValueError(f"Table {self.database_name}.{self.table_name} not found in catalog")
            
            table_info = table_map[table_key]
            incremental_cols = [
                col for col in [
                    table_info.get("incremental_col1"),
                    table_info.get("incremental_col2"),
                    table_info.get("incremental_col3")
                ] if col
            ]
            
            # Build query
            query = self._build_query(load_type, incremental_cols)
            if not query:
                return None
            
            # Read data
            if is_partitioned == "TRUE" and partition_col:
                self.df = self._read_with_partitioning(query, partition_col)
            else:
                self.df = self._read_without_partitioning(query)
            
            # Add insert timestamp
            self.df = self.df.withColumn("insert_timestamp", current_timestamp())
            
            record_count = self.df.count()
            self.logger.logger.info(f"Extracted {record_count} records from {self.table_name}")
            
            return self.df
            
        except Exception as e:
            self.logger.logger.error(f"Failed to extract data: {str(e)}")
            raise
    
    def _build_query(self, load_type, incremental_cols):
        """Build SQL query based on load type"""
        if load_type == "full_load":
            return f"SELECT * FROM {self.table_name}"
        
        elif load_type == "cdc_load":
            if not incremental_cols:
                self.logger.logger.warning(f"No incremental columns for CDC: {self.table_name}")
                return None
            
            try:
                last_timestamp = self.catalog.get_last_run_timestamp(self.database_name, self.table_name)
                last_dt = parse_datetime(last_timestamp) - timedelta(minutes=10)
                last_dt_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")
                
                where_conditions = " OR ".join([
                    f"({col}) > CAST('{last_dt_str}' AS {self.datetime_cast})"
                    for col in incremental_cols
                ])
                
                return f"SELECT * FROM {self.table_name} WHERE {where_conditions}"
                
            except Exception as e:
                self.logger.logger.warning(f"No timestamp found for CDC: {str(e)}")
                return None
        
        else:
            raise ValueError(f"Unknown load_type: {load_type}")
    
    def _read_with_partitioning(self, query, partition_col):
        """Read with JDBC partitioning"""
        bounds_query = f"(SELECT MIN({partition_col}) AS min_val, MAX({partition_col}) AS max_val FROM {self.table_name}) AS bounds"
        
        bounds_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", bounds_query) \
            .option("user", self.credentials["username"]) \
            .option("password", self.credentials["password"]) \
            .load()
        
        min_val, max_val = bounds_df.first()
        if min_val is None or max_val is None:
            min_val, max_val = 0, 1
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", f"({query}) AS tmp") \
            .option("user", self.credentials["username"]) \
            .option("password", self.credentials["password"]) \
            .option("partitionColumn", partition_col) \
            .option("lowerBound", int(min_val)) \
            .option("upperBound", int(max_val)) \
            .option("numPartitions", 10) \
            .option("fetchsize", 10000) \
            .option("zeroDateTimeBehavior", "convertToNull") \
            .load()
    
    def _read_without_partitioning(self, query):
        """Read without partitioning"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", f"({query}) AS tmp") \
            .option("user", self.credentials["username"]) \
            .option("password", self.credentials["password"]) \
            .option("zeroDateTimeBehavior", "convertToNull") \
            .load()
    
    def give_output(self):
        """GIVE_OUTPUT -> DF"""
        return self.df