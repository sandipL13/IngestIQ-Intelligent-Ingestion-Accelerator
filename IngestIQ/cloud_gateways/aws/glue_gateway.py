"""
IngestIQ AWS Glue Gateway
"""
import sys
from awsglue.utils import getResolvedOptions
from gateway import IngestIQGateway

def main():
    required_args = ["JOB_NAME", "database_name", "source_type", "jdbc_url", "sf_schema"]
    args = getResolvedOptions(sys.argv, required_args)
    
    config = {
        "cloud_provider": "aws",
        "database_name": args["database_name"],
        "source_type": args["source_type"],
        "jdbc_url": args["jdbc_url"],
        "sf_schema": args["sf_schema"],
        "thread_count": int(args.get("thread_count", "4")),
        "bucket_name": args.get("bucket_name", "default-bucket"),
        "region": args.get("region", "ap-south-1")
    }
    
    gateway = IngestIQGateway(config)
    gateway.execute()

if __name__ == "__main__":
    main()