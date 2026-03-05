"""
Gateway - Main Entry Point
Args: job_name, load_type, table_list
job_mapping[job_name].run_job
"""
import sys
from router import JOB_MAPPING
from utils import NotificationManager

def run_job(job_name, database_name, config, table_list=None, load_type=None):
    """
    Gateway function - Routes jobs to implementations
    
    Args:
        job_name: Name of job to run
        database_name: Database name to process
        config: Configuration dictionary
        table_list: Optional list of specific tables
        load_type: Optional filter (full_load, cdc_load)
    
    Returns:
        Job execution result
    """
    notification = NotificationManager(config.get("slack_webhook_url"))
    
    try:
        print(f"🚀 Starting IngestIQ job: {job_name} for database: {database_name}")
        
        # Validate job name
        if job_name not in JOB_MAPPING:
            available_jobs = list(JOB_MAPPING.keys())
            raise ValueError(f"Job '{job_name}' not found. Available: {available_jobs}")
        
        # Get and run job: job_mapping[job_name].run_job
        job_class = JOB_MAPPING[job_name]
        job = job_class(database_name, config)
        result = job.run_job(table_list=table_list, load_type=load_type)
        
        print(f"✅ Job {job_name} completed successfully")
        print(f"📊 Result: {result}")
        
        return result
        
    except Exception as e:
        error_msg = f"❌ Job {job_name} failed: {str(e)}"
        print(error_msg)
        notification.send(database_name, "failed", str(e))
        raise Exception(error_msg)

def main():
    """Main function for Glue job execution"""
    
    # For Glue job, get parameters from Glue context
    try:
        from awsglue.utils import getResolvedOptions
        
        required_args = ["JOB_NAME", "job_name", "database_name", "source_type", 
                        "jdbc_url", "bucket_name", "sf_schema"]
        args = getResolvedOptions(sys.argv, required_args)
        
        # Extract parameters
        job_name = args["job_name"]
        database_name = args["database_name"]
        
        # Build config from Glue parameters
        config = {
            "source_type": args["source_type"],
            "jdbc_url": args["jdbc_url"],
            "bucket_name": args["bucket_name"],
            "db_secret_name": f"glue_{args['source_type']}_creds",
            "snowflake": {
                "url": "vdqiiha-zc94797.snowflakecomputing.com",
                "database": "RAW",
                "schema": args["sf_schema"],
                "warehouse": "PRD_INGESTION_WH",
                "role": "PRD_ETL_DEVOPS"
            },
            "sf_secret_name": "snowflake",
            "slack_webhook_url": "https://hooks.slack.com/services/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"
        }
        
        # Optional parameters
        table_list = args.get("table_list")
        if table_list and table_list != "None":
            table_list = table_list.split(",")
        else:
            table_list = None
            
        load_type = args.get("load_type")
        if load_type == "None":
            load_type = None
        
        # Run job
        result = run_job(job_name, database_name, config, table_list, load_type)
        print("🎉 IngestIQ job execution completed successfully")
        
    except ImportError:
        # For local testing
        if len(sys.argv) < 3:
            print("Usage: python gateway.py <job_name> <database_name>")
            print("Available jobs:", list(JOB_MAPPING.keys()))
            sys.exit(1)
        
        job_name = sys.argv[1]
        database_name = sys.argv[2]
        
        # Sample config for testing
        config = {
            "source_type": "mysql",
            "jdbc_url": "jdbc:mysql://hostname:3306/database",
            "bucket_name": "test-bucket",
            "db_secret_name": "glue_mysql_creds",
            "snowflake": {
                "url": "account.snowflakecomputing.com",
                "database": "RAW",
                "schema": "TEST_SCHEMA",
                "warehouse": "COMPUTE_WH",
                "role": "ETL_ROLE"
            },
            "sf_secret_name": "snowflake"
        }
        
        result = run_job(job_name, database_name, config)
        
    except Exception as e:
        print(f"💥 IngestIQ job execution failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()