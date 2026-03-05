"""
Job Implementations - Orchestrate Extract -> Transform -> Load
"""
from jobs.scr1_df import Script1Extract
from jobs.transform import StandardTransform
from jobs.df_s3 import S3Load
from jobs.df_snowflake import SnowflakeLoad
from jobs.df_redshift import RedshiftLoad
from catalog.catalog_manager import CatalogManager
from utils import NotificationManager, parse_datetime
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
import pytz

class BaseJob:
    """Base Job Class"""
    
    def __init__(self, database_name, config):
        self.database_name = database_name
        self.config = config
        self.catalog = CatalogManager()
        self.notification = NotificationManager(config.get("slack_webhook_url"))
    
    def run_job(self, table_list=None, load_type=None):
        """Run job - to be implemented by subclasses"""
        raise NotImplementedError

class JOB_SCR1_SNOWFLAKE_JOB(BaseJob):
    """Job: scr1_df.GIVE_OUTPUT -> TRNS.OUTPUT -> LOAD_DF.FUNC"""
    
    def run_job(self, table_list=None, load_type=None):
        """Execute pipeline: Script1Extract -> Transform -> SnowflakeLoad"""
        try:
            # Get tables to process
            table_map = self.catalog.get_table_load_type_map(self.database_name)
            
            if table_list:
                table_map = {k: v for k, v in table_map.items() if k[1] in table_list}
            if load_type:
                table_map = {k: v for k, v in table_map.items() if v["load_type"] == load_type}
            
            results = []
            
            for (db_name, table_name), table_info in table_map.items():
                result = self._process_table(db_name, table_name, table_info)
                results.append(result)
            
            # Write audit logs
            self._write_audit_logs(results)
            
            self.notification.send(self.database_name, "success", 
                                 f"Processed {len(results)} tables successfully")
            
            return {"status": "success", "processed_tables": len(results), "results": results}
            
        except Exception as e:
            self.notification.send(self.database_name, "failed", str(e))
            raise
    
    def _process_table(self, database_name, table_name, table_info):
        """Process single table: Extract -> Transform -> Load"""
        # Initialize components
        extract = Script1Extract(database_name, table_name)
        transform = StandardTransform(database_name, table_name)
        load = SnowflakeLoad(database_name, table_name)
        
        # Setup configurations
        extract.setup_source(
            self.config["source_type"],
            self.config["jdbc_url"],
            self.config["db_secret_name"]
        )
        
        extract.setup_logger(self.config["bucket_name"])
        transform.logger = extract.logger
        load.logger = extract.logger
        
        load.setup_target(
            self.config["snowflake"],
            self.config["sf_secret_name"]
        )
        
        try:
            extract.logger.logger.info(f"Starting pipeline for {table_name}")
            
            # Extract: scr1_df.GIVE_OUTPUT
            df = extract.read_source(
                load_type=table_info["load_type"],
                partition_col=table_info["partition_col"],
                is_partitioned=table_info["is_partitioned"]
            )
            
            if df is None or df.count() == 0:
                extract.logger.logger.info(f"No data to process for {table_name}")
                return {"table": table_name, "status": "skipped", "row_count": 0}
            
            # Transform: TRNS.OUTPUT
            df = transform.output(df)
            
            # Load: LOAD_DF.FUNC
            load.load_df(df, table_name, table_info["load_type"])
            
            # Update CDC metadata
            if table_info["load_type"] == "cdc_load":
                self._update_cdc_metadata(database_name, table_name, df, table_info)
            
            row_count = df.count()
            extract.logger.logger.info(f"Pipeline completed for {table_name}: {row_count} rows")
            extract.logger.upload_to_s3()
            
            return {"table": table_name, "status": "success", "row_count": row_count}
            
        except Exception as e:
            extract.logger.logger.error(f"Pipeline failed for {table_name}: {str(e)}")
            extract.logger.upload_to_s3()
            return {"table": table_name, "status": "failed", "error": str(e)}
    
    def _update_cdc_metadata(self, database_name, table_name, df, table_info):
        """Update CDC metadata with latest timestamp"""
        try:
            incremental_cols = [
                col for col in [
                    table_info.get("incremental_col1"),
                    table_info.get("incremental_col2"),
                    table_info.get("incremental_col3")
                ] if col
            ]
            
            if incremental_cols:
                agg_exprs = [spark_max(col(c)).alias(c) for c in incremental_cols]
                max_vals = df.agg(*agg_exprs).collect()[0].asDict()
                
                non_null_vals = [v for v in max_vals.values() if v is not None]
                if non_null_vals:
                    normalized_vals = [parse_datetime(str(v)) if isinstance(v, str) else v for v in non_null_vals]
                    max_timestamp = max(normalized_vals)
                    
                    self.catalog.update_last_run_timestamp(
                        database_name, table_name, str(max_timestamp)
                    )
                    
        except Exception as e:
            print(f"Failed to update CDC metadata for {table_name}: {str(e)}")
    
    def _write_audit_logs(self, results):
        """Write audit logs to Snowflake"""
        try:
            ist = pytz.timezone("Asia/Kolkata")
            audit_time = datetime.now(ist)
            
            audit_rows = [
                Row(
                    DATABASE_NAME=self.database_name,
                    TABLE_NAME=result["table"],
                    LAYER="RAW",
                    STATUS=result["status"].upper(),
                    ROW_COUNT=result.get("row_count", 0),
                    INSERTED_AT=audit_time
                )
                for result in results
            ]
            
            if audit_rows:
                extract = Script1Extract(self.database_name, "audit")
                extract.setup_logger(self.config["bucket_name"])
                
                schema = StructType([
                    StructField("DATABASE_NAME", StringType(), True),
                    StructField("TABLE_NAME", StringType(), True),
                    StructField("LAYER", StringType(), True),
                    StructField("STATUS", StringType(), True),
                    StructField("ROW_COUNT", IntegerType(), True),
                    StructField("INSERTED_AT", TimestampType(), True)
                ])
                
                audit_df = extract.spark.createDataFrame(audit_rows, schema)
                
                audit_load = SnowflakeLoad(self.database_name, "audit")
                audit_config = self.config["snowflake"].copy()
                audit_config["database"] = "AUDIT"
                audit_config["schema"] = "AUDIT_PROGRAM"
                
                audit_load.setup_target(audit_config, self.config["sf_secret_name"])
                audit_load.load_df(audit_df, "ETL_AUDIT_LOG", "append")
                
        except Exception as e:
            print(f"Failed to write audit logs: {str(e)}")

class JOB_SCR1_REDSHIFT_JOB(BaseJob):
    """Job: scr1_df.GIVE_OUTPUT -> TRNS.OUTPUT -> LOAD_DF.RED_OP"""
    
    def run_job(self, table_list=None, load_type=None):
        """Execute pipeline: Script1Extract -> Transform -> RedshiftLoad"""
        # Similar implementation but with RedshiftLoad
        pass

class JOB_SCR1_S3_JOB(BaseJob):
    """Job: scr1_df.GIVE_OUTPUT -> TRNS.OUTPUT -> LOAD_DF.S3_OP"""
    
    def run_job(self, table_list=None, load_type=None):
        """Execute pipeline: Script1Extract -> Transform -> S3Load"""
        # Similar implementation but with S3Load
        pass