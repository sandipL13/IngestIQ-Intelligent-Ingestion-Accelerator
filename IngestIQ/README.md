# IngestIQ Framework

Multi-cloud data ingestion framework based on Euclidean-RPSG pattern.

## Architecture

```
IngestIQ/
├── glue_gateway.py      # AWS Glue entry point
├── gateway.py           # Main orchestrator
├── engine/
│   ├── router.py        # Job routing
│   ├── extractor.py     # Data extraction
│   └── loader.py        # Data loading
├── jobs/
│   └── etl_job.py       # ETL job implementation
├── utils/
│   ├── metadata.py      # Metadata management
│   ├── secrets.py       # Secrets management
│   ├── logger.py        # Logging
│   └── notification.py  # Notifications
└── config/
    └── framework_config.json
```

## Usage

### AWS Glue Job
```bash
aws glue start-job-run \
  --job-name "ingestiq-framework" \
  --arguments '{
    "--database_name": "sales_db",
    "--source_type": "mysql",
    "--jdbc_url": "jdbc:mysql://host:3306/db",
    "--sf_schema": "SALES_DB",
    "--thread_count": "4",
    "--bucket_name": "data-bucket"
  }'
```

### Programmatic Usage
```python
from IngestIQ.gateway import IngestIQGateway

config = {
    "database_name": "sales_db",
    "source_type": "mysql",
    "jdbc_url": "jdbc:mysql://host:3306/db",
    "sf_schema": "SALES_DB",
    "thread_count": 4,
    "bucket_name": "data-bucket"
}

gateway = IngestIQGateway(config)
result = gateway.execute()
```

## Features

✅ **Multi-Cloud Support** - AWS, Azure, GCP
✅ **Multiple Sources** - MySQL, PostgreSQL, Oracle, SQL Server
✅ **Multiple Targets** - S3, Snowflake, Redshift, BigQuery
✅ **CDC Support** - Change data capture
✅ **Multi-Threading** - Parallel table processing
✅ **Metadata Management** - DynamoDB/CosmosDB/Firestore
✅ **Monitoring** - Logging and notifications
✅ **Extensible** - Easy to add new sources/targets