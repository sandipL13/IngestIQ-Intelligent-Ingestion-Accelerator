"""
IngestIQ Azure Synapse Gateway
"""
import sys
import argparse
from gateway import IngestIQGateway

def parse_args():
    parser = argparse.ArgumentParser(description="IngestIQ Azure Synapse Job")
    parser.add_argument("--database_name", required=True)
    parser.add_argument("--source_type", required=True)
    parser.add_argument("--jdbc_url", required=True)
    parser.add_argument("--sf_schema", required=True)
    parser.add_argument("--thread_count", default="4")
    parser.add_argument("--storage_account", default="default-storage")
    parser.add_argument("--resource_group", required=True)
    return parser.parse_args()

def main():
    args = parse_args()
    
    config = {
        "cloud_provider": "azure",
        "database_name": args.database_name,
        "source_type": args.source_type,
        "jdbc_url": args.jdbc_url,
        "sf_schema": args.sf_schema,
        "thread_count": int(args.thread_count),
        "storage_account": args.storage_account,
        "resource_group": args.resource_group
    }
    
    gateway = IngestIQGateway(config)
    gateway.execute()

if __name__ == "__main__":
    main()