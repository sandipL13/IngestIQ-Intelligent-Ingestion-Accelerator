"""
df_snowflake - Script3 Snowflake Loading
"""
from jobs import AbstractLoad

class SnowflakeLoad(AbstractLoad):
    """Snowflake Load - df_snowflake"""
    
    def __init__(self, database_name, table_name="JOB_SUMMARY"):
        super().__init__(database_name, table_name)
        self.sf_config = None
        self.credentials = None
    
    def setup_target(self, sf_config, secret_name):
        """Setup Snowflake target configuration"""
        self.sf_config = sf_config
        self.credentials = self.secrets.get_secret(secret_name)
    
    def load_df(self, df, table_name, load_type="append", max_retries=3):
        """LOAD_DF - Load DataFrame to Snowflake with retry logic"""
        try:
            sf_options = {
                "sfURL": self.sf_config["url"],
                "sfDatabase": self.sf_config["database"],
                "sfSchema": self.sf_config["schema"],
                "sfWarehouse": self.sf_config["warehouse"],
                "sfRole": self.sf_config["role"],
                "sfUser": self.credentials["USERNAME"],
                "sfPassword": self.credentials["PASSWORD"]
            }
            
            write_mode = "overwrite" if load_type == "full_load" else "append"
            
            # Retry logic
            for attempt in range(1, max_retries + 1):
                try:
                    self.logger.logger.info(f"Snowflake write attempt {attempt} for {table_name}")
                    
                    df.write \
                        .format("net.snowflake.spark.snowflake") \
                        .options(**sf_options) \
                        .option("dbtable", table_name.upper()) \
                        .mode(write_mode) \
                        .save()
                    
                    self.logger.logger.info(f"Loaded {df.count()} records to Snowflake: {table_name}")
                    break
                    
                except Exception as e:
                    self.logger.logger.error(f"Snowflake attempt {attempt} failed: {str(e)}")
                    if attempt == max_retries:
                        raise Exception(f"Snowflake write failed after {max_retries} attempts")
            
        except Exception as e:
            self.logger.logger.error(f"Failed to load to Snowflake: {str(e)}")
            raise