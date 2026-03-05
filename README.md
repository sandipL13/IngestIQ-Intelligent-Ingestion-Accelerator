# IngestIQ - Clean Data Ingestion Framework

## 🎯 Overview

A **production-ready, cloud-agnostic data ingestion framework** following clean architecture principles:
- **Extract** - Read data from sources (scr1_df)
- **Transform** - Process and transform data
- **Load** - Store data to targets (df_s3, df_snowflake, df_redshift)
- **Gateway** - Orchestrate jobs

## 🏗️ Clean Architecture

```
IngestIQ/
├── infrastructure/          # Terraform for AWS/Azure
│   ├── aws/main.tf         # AWS: IAM, DynamoDB, Secrets
│   └── azure/main.tf       # Azure: Storage, Key Vault
├── utils/                  # Utilities (Secrets, Logger, Notifications)
├── catalog/                # Table Metadata (10 sample tables)
├── jobs/                   # ETL Components
│   ├── __init__.py        # Base Classes (Initialization, Abstract classes)
│   ├── scr1_df.py         # Script1 Extract (READ_SCR, GIVE_OUTPUT)
│   ├── df_s3.py           # Script2 S3 Load (LOAD_DF)
│   ├── df_snowflake.py    # Script3 Snowflake Load (LOAD_DF)
│   ├── df_redshift.py     # Script3 Redshift Load (LOAD_DF)
│   ├── transform.py       # Transform (COLNAME_FORMAT, DATE_MAPPING, OUTPUT)
│   └── job_implementations.py # Job Orchestration
├── gateway.py             # Main Gateway Entry Point
└── router.py              # Job Name Mapping
```

## 🚀 Quick Start

### 1. Infrastructure Setup
```bash
# AWS
cd infrastructure/aws && terraform apply

# Azure  
cd infrastructure/azure && terraform apply
```

### 2. Setup Catalog (10 Tables)
```python
from catalog.catalog_manager import setup_catalog
setup_catalog("sales_db")
```

### 3. Run Jobs

#### Glue Job Parameters
```bash
--job_name JOB1
--database_name sales_db
--source_type mysql
--jdbc_url jdbc:mysql://host:3306/db
--bucket_name my-bucket
--sf_schema SALES_DB
--table_list customers,orders  # Optional
--load_type cdc_load           # Optional
```

#### Programmatic Usage
```python
from gateway import run_job

config = {
    "source_type": "mysql",
    "jdbc_url": "jdbc:mysql://host:3306/db", 
    "bucket_name": "data-bucket",
    "db_secret_name": "glue_mysql_creds",
    "snowflake": {
        "url": "account.snowflakecomputing.com",
        "database": "RAW",
        "schema": "SALES_DB", 
        "warehouse": "COMPUTE_WH",
        "role": "ETL_ROLE"
    },
    "sf_secret_name": "snowflake"
}

result = run_job("JOB1", "sales_db", config)
```

## 🎯 Framework Implementation

### **Extract (scr1_df)**
```python
class Script1Extract(AbstractExtract):
    def read_source(self, load_type, partition_col, is_partitioned):
        # READ_SCR - Extract data from JDBC sources
        # Supports full_load and cdc_load
        # Handles partitioning for large tables
        
    def give_output(self):
        # GIVE_OUTPUT -> DF
```

### **Transform**
```python
class StandardTransform(AbstractTransform):
    def column_name_format(self, df):
        # COLNAME_FORMAT - Standardize column names
        
    def date_mapping(self, df):
        # DATE_MAPPING - Apply date transformations
        
    def output(self, df):
        # OUTPUT - Return transformed DataFrame
```

### **Load (df_s3, df_snowflake, df_redshift)**
```python
class S3Load(AbstractLoad):          # df_s3
class SnowflakeLoad(AbstractLoad):   # df_snowflake  
class RedshiftLoad(AbstractLoad):    # df_redshift

def load_df(self, df, table_name, load_type):
    # LOAD_DF - Load DataFrame to target system
```

### **Gateway (Orchestrate)**
```python
# Gateway routes jobs: job_mapping[job_name].run_job
JOB_MAPPING = {
    "JOB_SCR1_SNOWFLAKE_JOB": JOB_SCR1_SNOWFLAKE_JOB,  # scr1_df->TRNS->LOAD_DF.FUNC
    "JOB_SCR1_REDSHIFT_JOB": JOB_SCR1_REDSHIFT_JOB,    # scr1_df->TRNS->LOAD_DF.RED_OP
    "JOB_SCR1_S3_JOB": JOB_SCR1_S3_JOB                 # scr1_df->TRNS->LOAD_DF.S3_OP
}
```

## 📊 Sample Catalog (10 Tables)

| Table | Load Type | Partitioned | Incremental Columns |
|-------|-----------|-------------|---------------------|
| customers | full_load | ✅ | created_at, updated_at |
| orders | cdc_load | ✅ | order_date, updated_at |
| products | full_load | ✅ | created_at |
| order_items | cdc_load | ✅ | created_at, updated_at |
| categories | full_load | ❌ | updated_at |
| suppliers | cdc_load | ✅ | created_at, updated_at |
| inventory | cdc_load | ✅ | last_updated |
| payments | cdc_load | ✅ | payment_date, updated_at |
| reviews | cdc_load | ✅ | review_date, updated_at |
| user_sessions | cdc_load | ✅ | session_start, last_activity |

## 🎯 Available Jobs

| Job Name | Pipeline | Description |
|----------|----------|-------------|
| JOB_SCR1_SNOWFLAKE_JOB | scr1_df.GIVE_OUTPUT → TRNS.OUTPUT → LOAD_DF.FUNC | Load to Snowflake |
| JOB_SCR1_REDSHIFT_JOB | scr1_df.GIVE_OUTPUT → TRNS.OUTPUT → LOAD_DF.RED_OP | Load to Redshift |
| JOB_SCR1_S3_JOB | scr1_df.GIVE_OUTPUT → TRNS.OUTPUT → LOAD_DF.S3_OP | Load to S3 |

**Aliases:** JOB1, JOB2, JOB3, SNOWFLAKE, REDSHIFT, S3

## 🔧 Key Features

### ✅ **Clean Architecture**
- **Single Responsibility**: Each component has one job (Extract, Transform, Load)
- **Separation of Concerns**: Infrastructure, Utils, Jobs, Gateway
- **Abstract Base Classes**: Easy to extend and modify

### ✅ **Production Ready**
- **Error Handling**: Comprehensive try-catch with logging
- **Retry Logic**: Snowflake writes with 3 retry attempts
- **CDC Support**: Incremental loading with timestamp tracking
- **Audit Logging**: Complete audit trail in Snowflake
- **Notifications**: Slack alerts for success/failure

### ✅ **Easy to Deploy**
- **Terraform**: Infrastructure as Code for AWS/Azure
- **Glue Integration**: Direct parameter mapping from Glue jobs
- **Cloud Agnostic**: Same code works on AWS/Azure

### ✅ **Easy to Modify**
- **Add 11th Table**: Just update catalog
- **New Job Type**: Inherit from BaseJob, register in router
- **New Storer**: Inherit from AbstractStorer
- **New Reader**: Inherit from AbstractReader

### ✅ **Easy to Understand**
- **Clear Naming**: scr1_df, df_s3, df_snowflake, df_redshift
- **Logical Flow**: Extract → Transform → Load orchestrated by Gateway
- **Minimal Code**: Only essential functionality, no bloat

## 🛠️ Extending the Framework

### Add 11th Table
```python
from catalog.catalog_manager import CatalogManager
catalog = CatalogManager()
catalog.add_table("sales_db", "new_table", "cdc_load", "id", "TRUE", ["created_at"])
```

### Add New Job
```python
class CustomJob(BaseJob):
    def run_job(self, table_list=None, load_type=None):
        # Custom RPSG pipeline
        return {"status": "success"}

# Register in router.py
JOB_MAPPING["CUSTOM_JOB"] = CustomJob
```

### Add New Load Component
```python
class BigQueryLoad(AbstractLoad):
    def load_df(self, df, table_name, load_type="append"):
        # BigQuery loading logic
        pass
```

## 🚀 Deployment

### AWS Glue Job
```bash
aws glue create-job \
  --name "ingestiq-framework" \
  --role "arn:aws:iam::account:role/IngestIQ-GlueRole" \
  --command '{"Name": "glueetl", "ScriptLocation": "s3://bucket/gateway/__init__.py"}'
```

### Run Job
```bash
aws glue start-job-run \
  --job-name "ingestiq-framework" \
  --arguments '--job_name=JOB1 --database_name=sales_db --source_type=mysql --jdbc_url=jdbc:mysql://host:3306/db --bucket_name=data-bucket --sf_schema=SALES_DB'
```

## 📈 Usage Examples

```python
# Process all tables
run_job("JOB1", "sales_db", config)

# Process specific tables  
run_job("JOB1", "sales_db", config, table_list=["customers", "orders"])

# Process only CDC tables
run_job("JOB1", "sales_db", config, load_type="cdc_load")

# Process to different targets
run_job("SNOWFLAKE", "sales_db", config)  # To Snowflake
run_job("REDSHIFT", "sales_db", config)   # To Redshift  
run_job("S3", "sales_db", config)         # To S3 only
```

## 🎓 Benefits

1. **Clean**: Clear Extract-Transform-Load separation, minimal code
2. **Concise**: Only essential functionality
3. **Understandable**: Logical naming (scr1_df, df_s3, df_snowflake)
4. **Production Ready**: Error handling, logging, monitoring
5. **Easy to Deploy**: Terraform + Glue integration
6. **Easy to Modify**: Abstract classes + registration pattern
7. **Generalizable**: Works for any RDS → Raw Zone → Process Zone pipeline

The framework transforms your existing Glue job into a clean, modular, production-ready system with clear component separation.