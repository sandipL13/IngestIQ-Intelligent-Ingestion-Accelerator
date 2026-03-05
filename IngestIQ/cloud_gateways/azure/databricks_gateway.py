"""
IngestIQ Azure Databricks Gateway
"""
import sys
from pyspark.sql import SparkSession
from gateway import IngestIQGateway

def get_databricks_config():
    """Get configuration from Databricks widgets or environment"""
    try:
        # Try to get from Databricks widgets
        dbutils = globals().get('dbutils')
        if dbutils:
            config = {
                "cloud_provider": "azure",
                "database_name": dbutils.widgets.get("database_name"),
                "source_type": dbutils.widgets.get("source_type"),
                "jdbc_url": dbutils.widgets.get("jdbc_url"),
                "sf_schema": dbutils.widgets.get("sf_schema"),
                "thread_count": int(dbutils.widgets.get("thread_count") or "4"),
                "storage_account": dbutils.widgets.get("storage_account"),
                "resource_group": dbutils.widgets.get("resource_group")
            }
        else:
            # Fallback to command line args
            config = {
                "cloud_provider": "azure",
                "database_name": sys.argv[1],
                "source_type": sys.argv[2],
                "jdbc_url": sys.argv[3],
                "sf_schema": sys.argv[4],
                "thread_count": 4,
                "storage_account": "default-storage",
                "resource_group": "default-rg"
            }
        return config
    except Exception as e:
        raise ValueError(f"Failed to get configuration: {str(e)}")

def main():
    config = get_databricks_config()
    gateway = IngestIQGateway(config)
    gateway.execute()

if __name__ == "__main__":
    main()