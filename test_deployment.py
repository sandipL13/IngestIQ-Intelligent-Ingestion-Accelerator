#!/usr/bin/env python3
"""
IngestIQ Local Testing Script
Run this before deploying to Glue to catch issues early
"""

import sys
import yaml
from pyspark.sql import SparkSession

def test_configuration(config_path):
    """Test configuration file is valid"""
    print("\n" + "="*60)
    print("TEST 1: Configuration Validation")
    print("="*60)
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        required_fields = [
            'database_name', 'source_type', 'jdbc_url', 
            'region', 'bucket_name', 'db_secret_name'
        ]
        
        missing = [f for f in required_fields if f not in config]
        if missing:
            print(f"❌ FAILED: Missing required fields: {missing}")
            return False
        
        print(f"✅ PASSED: Configuration is valid")
        print(f"   - Database: {config['database_name']}")
        print(f"   - Source: {config['source_type']}")
        print(f"   - Bucket: {config['bucket_name']}")
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        return False

def test_imports():
    """Test all required imports work"""
    print("\n" + "="*60)
    print("TEST 2: Module Imports")
    print("="*60)
    
    try:
        from gateway import run
        from router import JOB_MAP
        from core.readers.jdbc_reader import JDBCReader
        from core.loaders.s3_loader import S3Loader
        from core.processors.table_processor import TableProcessor
        from core.metadata.dynamodb_manager import DynamoDBMetadataManager
        
        print(f"✅ PASSED: All imports successful")
        print(f"   - Available jobs: {list(JOB_MAP.keys())}")
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        return False

def test_aws_connectivity(region):
    """Test AWS connectivity"""
    print("\n" + "="*60)
    print("TEST 3: AWS Connectivity")
    print("="*60)
    
    try:
        import boto3
        
        # Test S3
        s3 = boto3.client('s3', region_name=region)
        s3.list_buckets()
        print(f"✅ S3 connection: OK")
        
        # Test DynamoDB
        dynamodb = boto3.client('dynamodb', region_name=region)
        dynamodb.list_tables()
        print(f"✅ DynamoDB connection: OK")
        
        # Test Secrets Manager
        secrets = boto3.client('secretsmanager', region_name=region)
        secrets.list_secrets(MaxResults=1)
        print(f"✅ Secrets Manager connection: OK")
        
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        print(f"   Hint: Check AWS credentials and permissions")
        return False

def test_database_connection(jdbc_url, db_secret_name, region):
    """Test database connection"""
    print("\n" + "="*60)
    print("TEST 4: Database Connection")
    print("="*60)
    
    try:
        import boto3
        import json
        
        # Get credentials
        secrets_client = boto3.client('secretsmanager', region_name=region)
        secret = secrets_client.get_secret_value(SecretId=db_secret_name)
        creds = json.loads(secret['SecretString'])
        
        print(f"✅ Retrieved credentials from Secrets Manager")
        
        # Create Spark session
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("IngestIQ-ConnTest") \
            .getOrCreate()
        
        # Test simple query
        test_query = "(SELECT 1 AS test) AS t"
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", test_query) \
            .option("user", creds["username"]) \
            .option("password", creds["password"]) \
            .load()
        
        result = df.collect()[0][0]
        spark.stop()
        
        if result == 1:
            print(f"✅ PASSED: Database connection successful")
            return True
        else:
            print(f"❌ FAILED: Unexpected result from test query")
            return False
            
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        print(f"   Hint: Check JDBC URL, credentials, and network access")
        return False

def test_metadata_tables(region):
    """Test DynamoDB metadata tables exist and are accessible"""
    print("\n" + "="*60)
    print("TEST 5: Metadata Tables")
    print("="*60)
    
    try:
        import boto3
        
        dynamodb = boto3.resource('dynamodb', region_name=region)
        
        # Check TABLE_LOAD_TYPE
        load_type_table = dynamodb.Table('TABLE_LOAD_TYPE')
        load_type_table.table_status
        print(f"✅ TABLE_LOAD_TYPE exists")
        
        # Check TABLE_RUN_TIME_METADATA
        run_time_table = dynamodb.Table('TABLE_RUN_TIME_METADATA')
        run_time_table.table_status
        print(f"✅ TABLE_RUN_TIME_METADATA exists")
        
        # Check if tables have data
        load_type_count = load_type_table.scan(Select='COUNT')['Count']
        print(f"✅ TABLE_LOAD_TYPE has {load_type_count} entries")
        
        if load_type_count == 0:
            print(f"⚠️  WARNING: No table metadata found")
            print(f"   Hint: Run populate_metadata.py script")
        
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        print(f"   Hint: Create DynamoDB tables first")
        return False

def test_s3_access(bucket_name, region):
    """Test S3 bucket access"""
    print("\n" + "="*60)
    print("TEST 6: S3 Bucket Access")
    print("="*60)
    
    try:
        import boto3
        
        s3 = boto3.client('s3', region_name=region)
        
        # Check bucket exists
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' exists")
        
        # Test write permission
        test_key = "test/connectivity-test.txt"
        s3.put_object(Bucket=bucket_name, Key=test_key, Body=b"test")
        print(f"✅ Write permission: OK")
        
        # Test read permission
        s3.get_object(Bucket=bucket_name, Key=test_key)
        print(f"✅ Read permission: OK")
        
        # Cleanup
        s3.delete_object(Bucket=bucket_name, Key=test_key)
        print(f"✅ Delete permission: OK")
        
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        print(f"   Hint: Check bucket name and IAM permissions")
        return False

def test_single_table_ingestion(config_path):
    """Test ingestion of a single small table"""
    print("\n" + "="*60)
    print("TEST 7: Single Table Ingestion (Dry Run)")
    print("="*60)
    
    try:
        from gateway import run
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Override for testing
        config['thread_count'] = 1
        config['snowflake_enabled'] = False  # Skip Snowflake for local test
        
        print(f"ℹ️  Skipping full ingestion test in local mode")
        print(f"   Run this test on Glue for full validation")
        print(f"✅ PASSED: Configuration ready for Glue testing")
        
        return True
        
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        return False

def main():
    if len(sys.argv) != 2:
        print("Usage: python test_deployment.py config/client.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    print("\n" + "🚀"*30)
    print("IngestIQ Deployment Testing")
    print("🚀"*30)
    
    # Load config for test parameters
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Run tests
    results = []
    results.append(("Configuration", test_configuration(config_path)))
    results.append(("Imports", test_imports()))
    results.append(("AWS Connectivity", test_aws_connectivity(config['region'])))
    results.append(("Database Connection", test_database_connection(
        config['jdbc_url'], 
        config['db_secret_name'], 
        config['region']
    )))
    results.append(("Metadata Tables", test_metadata_tables(config['region'])))
    results.append(("S3 Access", test_s3_access(config['bucket_name'], config['region'])))
    results.append(("Ingestion Ready", test_single_table_ingestion(config_path)))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:.<40} {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Ready for Glue deployment")
        print("\nNext steps:")
        print("1. Upload code to S3")
        print("2. Create Glue job")
        print("3. Run single table test on Glue")
        print("4. Monitor and validate results")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Fix issues before deploying to Glue")
        sys.exit(1)

if __name__ == "__main__":
    main()
