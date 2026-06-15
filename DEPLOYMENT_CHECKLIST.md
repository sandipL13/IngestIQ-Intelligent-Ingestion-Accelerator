# Deployment Checklist - Step by Step Guide

## 📋 Complete Deployment & Testing Guide for New Client

This guide walks you through deploying IngestIQ for a new client from scratch, including all testing steps.

---

## Phase 1: Pre-Deployment Setup (30 minutes)

### Step 1: Gather Client Information
Create a document with:
- [ ] Client name
- [ ] Database type (MySQL/PostgreSQL)
- [ ] Database host/endpoint
- [ ] Database name
- [ ] Database credentials
- [ ] List of tables to ingest
- [ ] Which tables need full load vs CDC
- [ ] Cloud platform (AWS/Azure/GCP)
- [ ] Target storage location
- [ ] Snowflake details (if applicable)

**Example:**
```
Client: NewClient
Database Type: MySQL
Host: newclient-rds.amazonaws.com:3306
Database: sales_db
Tables: 
  - customers (full_load)
  - orders (cdc_load)
  - products (full_load)
Cloud: AWS
S3 Bucket: newclient-data-bucket
```

### Step 2: Create Project Configuration
```bash
cd IngestIQ-Intelligent-Ingestion-Accelerator
cp config/aws_example.yaml config/newclient.yaml
```

Edit `config/newclient.yaml`:
```yaml
---
database_name: "sales_db"
thread_count: 3  # Start small for testing
source_type: "mysql"
jdbc_url: "jdbc:mysql://newclient-rds.amazonaws.com:3306/sales_db"

# AWS Specific
region: "us-east-1"
bucket_name: "newclient-data-bucket"
log_prefix: "glue-logs"

# Secrets
db_secret_name: "newclient_mysql_creds"
sf_secret_name: "newclient_snowflake"

# Snowflake Target
snowflake_enabled: true
sf_url: "account.snowflakecomputing.com"
sf_database: "RAW"
sf_schema: "SALES"
sf_warehouse: "INGESTION_WH"
sf_role: "ETL_ROLE"

# Audit
audit_database: "AUDIT"
audit_schema: "AUDIT_LOGS"

# Metadata
run_time_table: "TABLE_RUN_TIME_METADATA"
load_type_table: "TABLE_LOAD_TYPE"

# Notifications
slack_webhook_url: "https://hooks.slack.com/services/YOUR/WEBHOOK"
```

---

## Phase 2: AWS Infrastructure Setup (45 minutes)

### Step 3: Create S3 Bucket
```bash
# Create bucket
aws s3 mb s3://newclient-data-bucket --region us-east-1

# Create folder structure
aws s3api put-object --bucket newclient-data-bucket --key MySQL/
aws s3api put-object --bucket newclient-data-bucket --key glue-logs/
aws s3api put-object --bucket newclient-data-bucket --key config/
aws s3api put-object --bucket newclient-data-bucket --key scripts/
```

### Step 4: Store Secrets in AWS Secrets Manager
```bash
# Database credentials
aws secretsmanager create-secret \
  --name newclient_mysql_creds \
  --description "NewClient MySQL credentials" \
  --secret-string '{"username":"admin","password":"YourPassword123"}' \
  --region us-east-1

# Snowflake credentials (if using)
aws secretsmanager create-secret \
  --name newclient_snowflake \
  --description "NewClient Snowflake credentials" \
  --secret-string '{"USERNAME":"sf_user","PASSWORD":"SnowflakePass123"}' \
  --region us-east-1
```

**Verify secrets:**
```bash
aws secretsmanager get-secret-value --secret-id newclient_mysql_creds
aws secretsmanager get-secret-value --secret-id newclient_snowflake
```

### Step 5: Create DynamoDB Tables

**Create TABLE_LOAD_TYPE:**
```bash
aws dynamodb create-table \
  --table-name TABLE_LOAD_TYPE \
  --attribute-definitions \
    AttributeName=database_name,AttributeType=S \
    AttributeName=table_name,AttributeType=S \
  --key-schema \
    AttributeName=database_name,KeyType=HASH \
    AttributeName=table_name,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

**Create TABLE_RUN_TIME_METADATA:**
```bash
aws dynamodb create-table \
  --table-name TABLE_RUN_TIME_METADATA \
  --attribute-definitions \
    AttributeName=database_name,AttributeType=S \
    AttributeName=table_name,AttributeType=S \
  --key-schema \
    AttributeName=database_name,KeyType=HASH \
    AttributeName=table_name,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

**Verify tables created:**
```bash
aws dynamodb list-tables
aws dynamodb describe-table --table-name TABLE_LOAD_TYPE
aws dynamodb describe-table --table-name TABLE_RUN_TIME_METADATA
```

### Step 6: Populate Metadata Tables

Create `scripts/populate_metadata.py`:
```python
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
load_type_table = dynamodb.Table('TABLE_LOAD_TYPE')
run_time_table = dynamodb.Table('TABLE_RUN_TIME_METADATA')

# Define tables
tables = [
    {
        'database_name': 'sales_db',
        'table_name': 'customers',
        'load_type': 'full_load',
        'partition_col': 'customer_id',
        'is_partitioned': 'TRUE',
        'incremental_col1': '',
        'incremental_col2': '',
        'incremental_col3': ''
    },
    {
        'database_name': 'sales_db',
        'table_name': 'orders',
        'load_type': 'cdc_load',
        'partition_col': 'order_id',
        'is_partitioned': 'TRUE',
        'incremental_col1': 'updated_at',
        'incremental_col2': 'created_at',
        'incremental_col3': ''
    },
    {
        'database_name': 'sales_db',
        'table_name': 'products',
        'load_type': 'full_load',
        'partition_col': 'product_id',
        'is_partitioned': 'FALSE',
        'incremental_col1': '',
        'incremental_col2': '',
        'incremental_col3': ''
    }
]

# Populate load type table
for table in tables:
    print(f"Adding {table['table_name']}...")
    load_type_table.put_item(Item=table)

# Initialize CDC timestamps
cdc_tables = [t for t in tables if t['load_type'] == 'cdc_load']
for table in cdc_tables:
    print(f"Initializing CDC timestamp for {table['table_name']}...")
    run_time_table.put_item(Item={
        'database_name': table['database_name'],
        'table_name': table['table_name'],
        'last_updated_on': '2024-01-01 00:00:00.000000'  # Set appropriate start date
    })

print("Metadata population complete!")
```

**Run the script:**
```bash
python scripts/populate_metadata.py
```

**Verify metadata:**
```bash
aws dynamodb scan --table-name TABLE_LOAD_TYPE
aws dynamodb scan --table-name TABLE_RUN_TIME_METADATA
```

### Step 7: Create VPC Resources (if RDS is in VPC)

**Check if RDS is in VPC:**
```bash
aws rds describe-db-instances --db-instance-identifier newclient-rds
```

**If in VPC, note:**
- Subnet IDs
- Security Group IDs
- Availability Zone

### Step 8: Create Glue Connection

```bash
aws glue create-connection \
  --connection-input '{
    "Name": "newclient-mysql-connection",
    "ConnectionType": "JDBC",
    "ConnectionProperties": {
      "JDBC_CONNECTION_URL": "jdbc:mysql://newclient-rds.amazonaws.com:3306/sales_db",
      "USERNAME": "admin",
      "PASSWORD": "YourPassword123"
    },
    "PhysicalConnectionRequirements": {
      "SubnetId": "subnet-xxxxx",
      "SecurityGroupIdList": ["sg-xxxxx"],
      "AvailabilityZone": "us-east-1a"
    }
  }' \
  --region us-east-1
```

**Test connection:**
```bash
aws glue get-connection --name newclient-mysql-connection
```

---

## Phase 3: Deploy Framework Code (20 minutes)

### Step 9: Package and Upload Code

**Create deployment package:**
```bash
cd IngestIQ-Intelligent-Ingestion-Accelerator

# Create zip of core and jobs
zip -r ingestiq-framework.zip core/ jobs/ gateway.py router.py -x "*.pyc" -x "__pycache__/*"
```

**Upload to S3:**
```bash
# Upload framework code
aws s3 cp ingestiq-framework.zip s3://newclient-data-bucket/scripts/

# Upload entrypoint
aws s3 cp jobs/glue_entrypoint.py s3://newclient-data-bucket/scripts/

# Upload config
aws s3 cp config/newclient.yaml s3://newclient-data-bucket/config/
```

**Verify uploads:**
```bash
aws s3 ls s3://newclient-data-bucket/scripts/
aws s3 ls s3://newclient-data-bucket/config/
```

### Step 10: Create IAM Role for Glue

**Create trust policy file** `glue-trust-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**Create IAM role:**
```bash
aws iam create-role \
  --role-name NewClientGlueRole \
  --assume-role-policy-document file://glue-trust-policy.json
```

**Attach policies:**
```bash
# Glue service role
aws iam attach-role-policy \
  --role-name NewClientGlueRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# S3 access
aws iam attach-role-policy \
  --role-name NewClientGlueRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# DynamoDB access
aws iam attach-role-policy \
  --role-name NewClientGlueRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess

# Secrets Manager access
aws iam attach-role-policy \
  --role-name NewClientGlueRole \
  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite
```

---

## Phase 4: Testing (Critical - Do NOT Skip!)

### Step 11: Local Testing (RECOMMENDED FIRST)

**Create test script** `test_local.py`:
```python
from pyspark.sql import SparkSession
from gateway import run
import yaml

# Initialize local Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("IngestIQ-LocalTest") \
    .config("spark.driver.extraClassPath", "/path/to/mysql-connector.jar") \
    .getOrCreate()

# Load config
with open('config/newclient.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Override for single table testing
config['database_name'] = 'sales_db'
config['thread_count'] = 1

# Run job
try:
    print("Starting local test...")
    run("AWS_RDS_INGESTION", spark, config)
    print("Local test completed successfully!")
except Exception as e:
    print(f"Local test failed: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
```

**Download MySQL JDBC driver:**
```bash
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar
```

**Run local test:**
```bash
python test_local.py
```

**Expected output:**
```
Starting local test...
[INFO] Starting Ingestion Job for Database: sales_db
[INFO] Total tables to process: 3
[INFO] Completed: customers (1500 rows)
[INFO] Completed: orders (2300 rows)
[INFO] Completed: products (450 rows)
[INFO] Job completed successfully for sales_db!
Local test completed successfully!
```

### Step 12: Test Single Table on Glue

**Create test Glue job:**
```bash
aws glue create-job \
  --name newclient-test-single-table \
  --role NewClientGlueRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://newclient-data-bucket/scripts/glue_entrypoint.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job_type": "AWS_RDS_INGESTION",
    "--config_path": "s3://newclient-data-bucket/config/newclient.yaml",
    "--extra-py-files": "s3://newclient-data-bucket/scripts/ingestiq-framework.zip",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-spark-ui": "true",
    "--TempDir": "s3://newclient-data-bucket/temp/"
  }' \
  --connections Connections=newclient-mysql-connection \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X" \
  --region us-east-1
```

**Before running, modify metadata to test ONE table only:**
```python
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
load_type_table = dynamodb.Table('TABLE_LOAD_TYPE')

# Temporarily set other tables to skip
load_type_table.update_item(
    Key={'database_name': 'sales_db', 'table_name': 'orders'},
    UpdateExpression='SET load_type = :val',
    ExpressionAttributeValues={':val': 'skip'}
)
load_type_table.update_item(
    Key={'database_name': 'sales_db', 'table_name': 'products'},
    UpdateExpression='SET load_type = :val',
    ExpressionAttributeValues={':val': 'skip'}
)
```

**Run test job:**
```bash
aws glue start-job-run --job-name newclient-test-single-table
```

**Monitor job:**
```bash
# Get run ID from previous command output
JOB_RUN_ID="jr_xxxxx"

# Check status
aws glue get-job-run \
  --job-name newclient-test-single-table \
  --run-id $JOB_RUN_ID \
  --query 'JobRun.JobRunState'

# View logs
aws logs tail /aws-glue/jobs/output --follow
```

**Validation Checklist:**
- [ ] Job completed successfully
- [ ] Check CloudWatch logs for errors
- [ ] Verify data in S3: `aws s3 ls s3://newclient-data-bucket/MySQL/sales_db/customers/`
- [ ] Verify Parquet files created
- [ ] Check row count in S3 data
- [ ] Verify Snowflake data (if enabled)
- [ ] Check audit logs in Snowflake
- [ ] Verify Slack notification received

**Verify data in S3:**
```bash
aws s3 ls s3://newclient-data-bucket/MySQL/sales_db/customers/ --recursive
aws s3 cp s3://newclient-data-bucket/MySQL/sales_db/customers/2024-01-15/10/part-00000.snappy.parquet - | head
```

**Test data quality:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("s3://newclient-data-bucket/MySQL/sales_db/customers/")
print(f"Row count: {df.count()}")
print("Schema:")
df.printSchema()
print("Sample data:")
df.show(5)
```

### Step 13: Test CDC Functionality

**Update one record in source database:**
```sql
-- In your MySQL database
UPDATE orders 
SET order_status = 'SHIPPED', updated_at = NOW() 
WHERE order_id = 12345;
```

**Restore table metadata:**
```python
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
load_type_table = dynamodb.Table('TABLE_LOAD_TYPE')

# Set orders back to cdc_load
load_type_table.update_item(
    Key={'database_name': 'sales_db', 'table_name': 'orders'},
    UpdateExpression='SET load_type = :val',
    ExpressionAttributeValues={':val': 'cdc_load'}
)
```

**Run job again:**
```bash
aws glue start-job-run --job-name newclient-test-single-table
```

**Verify CDC worked:**
- [ ] Check DynamoDB timestamp was updated
- [ ] Verify only changed records in S3 (check file sizes)
- [ ] Check CloudWatch logs show CDC query
- [ ] Verify incremental data in Snowflake

```bash
# Check timestamp in DynamoDB
aws dynamodb get-item \
  --table-name TABLE_RUN_TIME_METADATA \
  --key '{"database_name":{"S":"sales_db"},"table_name":{"S":"orders"}}'
```

---

## Phase 5: Production Deployment (30 minutes)

### Step 14: Create Production Glue Job

**Restore all tables to correct load type:**
```python
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
load_type_table = dynamodb.Table('TABLE_LOAD_TYPE')

# Restore all tables
tables = {
    'customers': 'full_load',
    'orders': 'cdc_load',
    'products': 'full_load'
}

for table_name, load_type in tables.items():
    load_type_table.update_item(
        Key={'database_name': 'sales_db', 'table_name': table_name},
        UpdateExpression='SET load_type = :val',
        ExpressionAttributeValues={':val': load_type}
    )
```

**Create production job:**
```bash
aws glue create-job \
  --name newclient-rds-ingestion-prod \
  --role NewClientGlueRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://newclient-data-bucket/scripts/glue_entrypoint.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job_type": "AWS_RDS_INGESTION",
    "--config_path": "s3://newclient-data-bucket/config/newclient.yaml",
    "--extra-py-files": "s3://newclient-data-bucket/scripts/ingestiq-framework.zip",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-spark-ui": "true",
    "--enable-job-insights": "true",
    "--TempDir": "s3://newclient-data-bucket/temp/",
    "--spark-event-logs-path": "s3://newclient-data-bucket/spark-logs/"
  }' \
  --connections Connections=newclient-mysql-connection \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X" \
  --max-retries 1 \
  --timeout 2880 \
  --region us-east-1
```

### Step 15: Full Production Test

**Run production job:**
```bash
aws glue start-job-run --job-name newclient-rds-ingestion-prod
```

**Monitor closely:**
```bash
# Watch job status
watch -n 10 'aws glue get-job-runs --job-name newclient-rds-ingestion-prod --max-results 1'

# Tail logs
aws logs tail /aws-glue/jobs/output --follow --filter-pattern "newclient-rds-ingestion-prod"
```

**Full validation:**
- [ ] All 3 tables processed successfully
- [ ] Check S3 data for all tables
- [ ] Verify row counts match source
- [ ] Check Snowflake data
- [ ] Verify audit logs
- [ ] Check Slack notifications
- [ ] Verify CDC timestamps updated
- [ ] Check job duration is reasonable

### Step 16: Schedule Production Job

**Create schedule:**
```bash
# Daily at 2 AM UTC
aws glue create-trigger \
  --name newclient-daily-ingestion \
  --type SCHEDULED \
  --schedule "cron(0 2 * * ? *)" \
  --actions JobName=newclient-rds-ingestion-prod \
  --start-on-creation \
  --region us-east-1
```

**Verify trigger:**
```bash
aws glue get-trigger --name newclient-daily-ingestion
```

---

## Phase 6: Monitoring & Alerts (15 minutes)

### Step 17: Set Up CloudWatch Alarms

**Create SNS topic:**
```bash
aws sns create-topic --name newclient-glue-alerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:123456789012:newclient-glue-alerts \
  --protocol email \
  --notification-endpoint your-email@company.com
```

**Create CloudWatch alarm for job failures:**
```bash
aws cloudwatch put-metric-alarm \
  --alarm-name newclient-glue-job-failure \
  --alarm-description "Alert on Glue job failure" \
  --metric-name JobFailed \
  --namespace Glue \
  --statistic Sum \
  --period 300 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:us-east-1:123456789012:newclient-glue-alerts
```

### Step 18: Create Monitoring Dashboard

**Create CloudWatch dashboard:**
```bash
aws cloudwatch put-dashboard \
  --dashboard-name NewClient-Ingestion-Dashboard \
  --dashboard-body file://dashboard-config.json
```

**dashboard-config.json:**
```json
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["Glue", "JobRun", {"stat": "Sum"}],
          [".", "JobFailed", {"stat": "Sum"}]
        ],
        "period": 300,
        "stat": "Sum",
        "region": "us-east-1",
        "title": "Glue Job Status"
      }
    }
  ]
}
```

---

## Phase 7: Documentation & Handoff (15 minutes)

### Step 19: Create Client-Specific Documentation

Create `docs/newclient-runbook.md`:
```markdown
# NewClient Ingestion Runbook

## Job Details
- **Job Name**: newclient-rds-ingestion-prod
- **Schedule**: Daily at 2 AM UTC
- **Duration**: ~15 minutes
- **Tables**: customers, orders, products

## Monitoring
- **Dashboard**: https://console.aws.amazon.com/cloudwatch/dashboards/NewClient-Ingestion-Dashboard
- **Logs**: https://console.aws.amazon.com/glue/home#/job/newclient-rds-ingestion-prod
- **Slack**: #newclient-alerts

## Common Issues

### Issue: Job failed with "Connection timeout"
**Solution**: Check VPC security group allows Glue access

### Issue: CDC not loading new data
**Solution**: Check DynamoDB timestamp and incremental columns

## Emergency Contacts
- Data Engineer: name@company.com
- Client Contact: client@newclient.com
```

### Step 20: Final Checklist

**Pre-Go-Live Checklist:**
- [ ] All secrets stored securely
- [ ] DynamoDB tables populated correctly
- [ ] S3 bucket permissions set
- [ ] VPC/Security groups configured
- [ ] Glue connection tested
- [ ] Local test passed
- [ ] Single table test passed
- [ ] CDC test passed
- [ ] Full production test passed
- [ ] Monitoring dashboards created
- [ ] Alerts configured
- [ ] Schedule created and enabled
- [ ] Documentation complete
- [ ] Client notified
- [ ] Team trained

---

## Troubleshooting Guide

### Common Issues & Solutions

#### 1. "Module not found" Error
```
Solution: Check --extra-py-files includes ingestiq-framework.zip
```

#### 2. "Connection refused" to Database
```
Solution: 
- Check Glue connection is attached to job
- Verify security group allows inbound on port 3306
- Check RDS is in correct VPC/subnet
```

#### 3. "Access Denied" to S3
```
Solution: Check IAM role has S3 permissions
aws iam get-role-policy --role-name NewClientGlueRole --policy-name S3Access
```

#### 4. "Table not found" in DynamoDB
```
Solution: Verify table names match config
aws dynamodb list-tables
```

#### 5. CDC Not Working
```
Solution:
1. Check TABLE_RUN_TIME_METADATA has entry
2. Verify incremental columns exist in source table
3. Check last_updated_on timestamp format
```

#### 6. Slow Job Performance
```
Solution:
1. Increase worker count
2. Enable partitioning for large tables
3. Adjust thread_count in config
```

---

## Post-Deployment Maintenance

### Daily Tasks
- [ ] Check Slack notifications
- [ ] Verify job completed successfully
- [ ] Spot check data quality

### Weekly Tasks
- [ ] Review CloudWatch metrics
- [ ] Check S3 storage growth
- [ ] Review audit logs in Snowflake
- [ ] Verify CDC timestamps updating

### Monthly Tasks
- [ ] Review and optimize job performance
- [ ] Check DynamoDB capacity
- [ ] Update documentation if needed
- [ ] Client status meeting

---

## Success Metrics

After deployment, track:
- Job success rate (target: >99%)
- Average job duration (baseline: ~15 min)
- Data freshness (CDC lag < 1 hour)
- Cost per month (S3 + Glue + DynamoDB)
- Data quality issues (target: 0)

---

## Next Steps After Successful Deployment

1. Run job for 1 week, monitor daily
2. Validate data quality with client
3. Optimize performance if needed
4. Add more tables if requested
5. Consider adding data quality checks
6. Plan for disaster recovery testing

---

**🎉 Congratulations! Your IngestIQ deployment is complete!**
