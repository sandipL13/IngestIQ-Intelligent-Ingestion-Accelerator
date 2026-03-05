"""
IngestIQ GCP Dataflow Gateway
"""
import sys
import argparse
from gateway import IngestIQGateway

def parse_args():
    parser = argparse.ArgumentParser(description="IngestIQ GCP Dataflow Job")
    parser.add_argument("--database_name", required=True)
    parser.add_argument("--source_type", required=True)
    parser.add_argument("--jdbc_url", required=True)
    parser.add_argument("--sf_schema", required=True)
    parser.add_argument("--thread_count", default="4")
    parser.add_argument("--gcs_bucket", default="default-bucket")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--region", default="us-central1")
    return parser.parse_args()

def main():
    args = parse_args()
    
    config = {
        "cloud_provider": "gcp",
        "compute_engine": "dataflow",
        "database_name": args.database_name,
        "source_type": args.source_type,
        "jdbc_url": args.jdbc_url,
        "sf_schema": args.sf_schema,
        "thread_count": int(args.thread_count),
        "gcs_bucket": args.gcs_bucket,
        "project_id": args.project_id,
        "region": args.region
    }
    
    gateway = IngestIQGateway(config)
    gateway.execute()

if __name__ == "__main__":
    main()