# IngestIQ - Intelligent Ingestion Accelerator

IngestIQ is a complete multi-cloud data ingestion framework to deploy PySpark jobs across AWS, Azure, and GCP. The framework provides a unified approach to extract data from various sources (MySQL, PostgreSQL, Oracle, SQL Server) and load to cloud storage and data warehouses with CDC support.

## Table of Contents

1. [IngestIQ Framework Overview](#1-ingestiq-framework-overview)
2. [Installation & Prerequisites](#2-installation--prerequisites)
3. [Cloud Provider Setup](#3-cloud-provider-setup)
4. [Framework Architecture](#4-framework-architecture)
5. [Configuration](#5-configuration)
6. [Running Jobs](#6-running-jobs)
7. [Deployment](#7-deployment)
8. [Development Guide](#8-development-guide)
9. [Git Workflow](#9-git-workflow)
10. [Best Practices](#10-best-practices)

---

## 1. IngestIQ Framework Overview

### Core Components

#### **cloud_gateways/**
Cloud-specific entry points for different compute engines:
- **aws/** - AWS Glue and EMR gateway implementations
- **azure/** - Azure Synapse and Databricks gateway implementations  
- **gcp/** - GCP Dataproc and Dataflow gateway implementations

#### **config/**
Framework configuration files:
- **framework_config.json** - Multi-cloud settings, supported sources/targets, default parameters

#### **engine/**
Core processing components:
- **router.py** - Routes jobs based on source type
- **extractor.py** - Extracts data from source databases with CDC support
- **loader.py** - Loads data to cloud storage (S3/ADLS/GCS) and Snowflake

#### **jobs/**
Job implementations:
- **etl_job.py** - Main ETL job with multi-threading support for parallel table processing

#### **utils/**
Multi-cloud utility modules:
- **secrets.py** - Secrets management (AWS Secrets Manager, Azure Key Vault, GCP Secret Manager)
- **metadata.py** - Metadata management (DynamoDB, Cosmos DB, Firestore)
- **logger.py** - Cloud storage logging (S3, ADLS, GCS)
- **notification.py** - Slack notifications for job status

#### **gateway.py**
Main orchestrator that initializes router, logger, and notification manager

#### **requirements.txt**
All required packages for AWS, Azure, and GCP deployments

---

## 2. Installation & Prerequisites

### Required Software

1. **Python 3.8+**: https://www.python.org/downloads/
2. **PySpark 3.0+**: Included in requirements.txt
3. **IDE**: PyCharm or VS Code recommended
4. **Git**: https://git-scm.com/downloads

### Python Dependencies

Install all dependencies:
```bash
pip install -r requirements.txt
```

### Cloud CLI Tools

**AWS:**
```bash
pip install awscli
aws configure
```

**Azure:**
```bash
pip install azure-cli
az login
```

**GCP:**
```bash
# Install gcloud SDK: https://cloud.google.com/sdk/docs/install
gcloud auth login
```

---

## 3. Cloud Provider Setup

### AWS Setup

**Required Services:**
- AWS Glue or EMR for compute
- S3 for storage
- DynamoDB for metadata
- Secrets Manager for credentials
- IAM roles with appropriate permissions

**IAM Role Permissions:**
- AWSGlueServiceRole
- S3 Read/Write access
- DynamoDB Read/Write access
- Secrets Manager Read access

**Documentation:**
- AWS Glue: https://docs.aws.amazon.com/glue/
- AWS EMR: https://docs.aws.amazon.com/emr/

### Azure Setup

**Required Services:**
- Azure Synapse or Databricks for compute
- ADLS Gen2 for storage
- Cosmos DB for metadata
- Key Vault for credentials
- Managed Identity or Service Principal

**Documentation:**
- Azure Synapse: https://docs.microsoft.com/azure/synapse-analytics/
- Azure Databricks: https://docs.microsoft.com/azure/databricks/

### GCP Setup

**Required Services:**
- Dataproc or Dataflow for compute
- Google Cloud Storage for storage
- Firestore for metadata
- Secret Manager for credentials
- Service Account with appropriate permissions

**Documentation:**
- GCP Dataproc: https://cloud.google.com/dataproc/docs
- GCP Dataflow: https://cloud.google.com/dataflow/docs

---

## 4. Framework Architecture

### Data Flow

```
Source DB → Extractor → Loader → Cloud Storage + Snowflake
              ↓           ↓
          Metadata    Logging
```

### Multi-Threading Architecture

```
ETLJob
  ├── Thread 1: Table A (Extract → Load)
  ├── Thread 2: Table B (Extract → Load)
  ├── Thread 3: Table C (Extract → Load)
  └── Thread N: Table N (Extract → Load)
```

### CDC (Change Data Capture) Flow

```
1. Get last_updated_on from metadata store
2. Query: WHERE incremental_col > last_updated_on
3. Extract changed records
4. Load to storage + warehouse
5. Update last_updated_on with max timestamp
```

---

## 5. Configuration

### Job Configuration

```python
config = {
    "cloud_provider": "aws",           # aws | azure | gcp
    "database_name": "sales_db",       # Source database name
    "source_type": "mysql",            # mysql | postgresql | oracle | sqlserver
    "jdbc_url": "jdbc:mysql://host:3306/db",
    "sf_schema": "SALES_DB",           # Snowflake schema
    "thread_count": 4,                 # Parallel table processing
    "bucket_name": "data-bucket",      # S3 bucket (AWS)
    "storage_account": "storage",      # Storage account (Azure)
    "gcs_bucket": "bucket",            # GCS bucket (GCP)
    "region": "ap-south-1"             # Cloud region
}
```

### Metadata Tables

**TABLE_LOAD_TYPE** - Defines load strategy per table:
```json
{
  "database_name": "sales_db",
  "table_name": "customers",
  "load_type": "cdc_load",           # full_load | cdc_load
  "partition_col": "customer_id",
  "is_partitioned": "TRUE",
  "incremental_col1": "updated_at",
  "incremental_col2": "created_at",
  "incremental_col3": null
}
```

**TABLE_RUN_TIME_METADATA** - Tracks last run timestamp:
```json
{
  "database_name": "sales_db",
  "table_name": "customers",
  "last_updated_on": "2024-01-15 10:30:45.123456"
}
```

---

## 6. Running Jobs

### AWS Glue

```bash
aws glue start-job-run \
  --job-name "ingestiq-framework" \
  --arguments '{
    "--database_name": "sales_db",
    "--source_type": "mysql",
    "--jdbc_url": "jdbc:mysql://host:3306/db",
    "--sf_schema": "SALES_DB",
    "--thread_count": "4",
    "--bucket_name": "data-bucket",
    "--region": "ap-south-1"
  }'
```

### AWS EMR

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files IngestIQ.zip \
  cloud_gateways/aws/emr_gateway.py \
  --database_name sales_db \
  --source_type mysql \
  --jdbc_url jdbc:mysql://host:3306/db \
  --sf_schema SALES_DB \
  --thread_count 4 \
  --bucket_name data-bucket
```

### Azure Synapse

```bash
python cloud_gateways/azure/synapse_gateway.py \
  --database_name sales_db \
  --source_type mysql \
  --jdbc_url jdbc:mysql://host:3306/db \
  --sf_schema SALES_DB \
  --thread_count 4 \
  --storage_account mystorageaccount \
  --resource_group my-rg
```

### Azure Databricks

Upload to Databricks workspace and run with widgets:
```python
dbutils.widgets.text("database_name", "sales_db")
dbutils.widgets.text("source_type", "mysql")
# ... other widgets
```

### GCP Dataproc

```bash
gcloud dataproc jobs submit pyspark \
  cloud_gateways/gcp/dataproc_gateway.py \
  --cluster=my-cluster \
  --region=us-central1 \
  -- --database_name sales_db \
     --source_type mysql \
     --jdbc_url jdbc:mysql://host:3306/db \
     --sf_schema SALES_DB \
     --thread_count 4 \
     --gcs_bucket my-bucket \
     --project_id my-project
```

### Local Development

```bash
python gateway.py
```

---

## 7. Deployment

### Create Distribution Package

```bash
# Install build tools
pip install --upgrade setuptools wheel

# Create wheel file
python setup.py bdist_wheel

# Output: dist/IngestIQ-1.0.0-py3-none-any.whl
```

### Deploy to AWS Glue

1. Upload .whl to S3:
```bash
aws s3 cp dist/IngestIQ-1.0.0-py3-none-any.whl s3://my-bucket/libs/
```

2. Create Glue job with Python library path:
```
--extra-py-files s3://my-bucket/libs/IngestIQ-1.0.0-py3-none-any.whl
```

### Deploy to Azure Databricks

1. Upload to DBFS:
```bash
databricks fs cp dist/IngestIQ-1.0.0-py3-none-any.whl dbfs:/libs/
```

2. Install in cluster libraries

### Deploy to GCP Dataproc

1. Upload to GCS:
```bash
gsutil cp dist/IngestIQ-1.0.0-py3-none-any.whl gs://my-bucket/libs/
```

2. Use --py-files in spark-submit

---

## 8. Development Guide

### Project Structure

```
IngestIQ/
├── cloud_gateways/          # Cloud-specific entry points
│   ├── aws/
│   ├── azure/
│   └── gcp/
├── engine/                  # Core processing
│   ├── router.py
│   ├── extractor.py
│   └── loader.py
├── jobs/                    # Job implementations
│   └── etl_job.py
├── utils/                   # Utilities
│   ├── secrets.py
│   ├── metadata.py
│   ├── logger.py
│   └── notification.py
├── config/                  # Configuration
│   └── framework_config.json
├── gateway.py               # Main orchestrator
└── requirements.txt         # Dependencies
```

### Adding New Source Type

1. Update `engine/extractor.py`:
```python
def _get_credentials(self):
    source_type = self.config["source_type"].lower()
    if source_type == "newsource":
        secret_name = f"glue_newsource_creds"
    return self.secrets.get_secret(secret_name)
```

2. Update `config/framework_config.json`:
```json
"supported_sources": ["mysql", "postgresql", "oracle", "sqlserver", "newsource"]
```

### Adding New Target

1. Update `engine/loader.py`:
```python
def load_to_newtarget(self, df, table_name, load_type):
    # Implementation
    pass
```

2. Update `jobs/etl_job.py`:
```python
self.loader.load_to_newtarget(df, table_name, load_type)
```

### Coding Standards

Follow PEP 8: https://peps.python.org/pep-0008/

**Key Guidelines:**
- Use 4 spaces for indentation
- Max line length: 100 characters
- Use descriptive variable names
- Add docstrings to all functions
- Use type hints where applicable

---

## 9. Git Workflow

### Clone Repository

```bash
git clone https://github.com/sandipL13/IngestIQ-Intelligent-Ingestion-Accelerator.git
cd IngestIQ-Intelligent-Ingestion-Accelerator
```

### Create Feature Branch

```bash
git checkout -b feature/new-feature
```

### Check Changes

```bash
git status
git diff HEAD --name-only
```

### Commit Changes

```bash
git add .
git commit -m "feat: add new feature description"
```

### Push to Remote

```bash
git push origin feature/new-feature
```

### Create Pull Request

1. Go to GitHub repository
2. Click "New Pull Request"
3. Select your feature branch
4. Add description and submit

### Commit Message Convention

```
feat: new feature
fix: bug fix
docs: documentation update
refactor: code refactoring
test: add tests
chore: maintenance tasks
```

---

## 10. Best Practices

### Security
- Never commit credentials to Git
- Use cloud-native secret managers
- Rotate credentials regularly
- Use least privilege IAM policies

### Performance
- Use partitioning for large tables
- Tune thread_count based on cluster size
- Monitor Spark UI for bottlenecks
- Use appropriate file formats (Parquet/Snappy)

### Monitoring
- Check CloudWatch/Azure Monitor/Cloud Logging
- Set up alerts for job failures
- Review Slack notifications
- Analyze S3/ADLS/GCS logs

### Data Quality
- Validate incremental columns exist
- Monitor CDC timestamp updates
- Check for data skew
- Implement data quality checks

### Cost Optimization
- Use spot instances for EMR
- Right-size cluster configurations
- Clean up old logs and data
- Use lifecycle policies for storage

### Checklists
- AWS S3: https://github.com/servian/amazon-s3-checklist
- AWS Redshift: https://github.com/servian/amazon-redshift-checklist

---

## Support

For issues and questions:
- Create GitHub Issue
- Contact: sandipL13@github.com

## License

Proprietary - All rights reserved

---

**Version:** 1.0.0  
**Last Updated:** 2024-01-15