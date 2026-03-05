"""
df_s3 - Script2 S3 Loading
"""
from jobs import AbstractLoad
from utils import get_current_time

class S3Load(AbstractLoad):
    """S3 Load - df_s3"""
    
    def __init__(self, database_name, table_name="JOB_SUMMARY"):
        super().__init__(database_name, table_name)
        self.bucket_name = None
        self.base_path = None
    
    def setup_target(self, bucket_name, base_path):
        """Setup S3 target configuration"""
        self.bucket_name = bucket_name
        self.base_path = base_path
    
    def load_df(self, df, table_name, load_type="append"):
        """LOAD_DF - Load DataFrame to S3"""
        try:
            current_time = get_current_time()
            load_date = current_time.strftime("%Y-%m-%d")
            load_hour = current_time.strftime("%H")
            
            target_path = f"s3://{self.bucket_name}/{self.base_path}{self.database_name}/{table_name}/{load_date}/{load_hour}/"
            write_mode = "overwrite" if load_type == "full_load" else "append"
            
            df.write \
                .mode(write_mode) \
                .option("compression", "snappy") \
                .option("maxRecordsPerFile", "1000000") \
                .parquet(target_path)
            
            self.logger.logger.info(f"Loaded {df.count()} records to S3: {target_path}")
            
        except Exception as e:
            self.logger.logger.error(f"Failed to load to S3: {str(e)}")
            raise