#!/usr/bin/env python3
"""
Populate DynamoDB Metadata Tables
Usage: python populate_metadata.py tables_config.yaml
"""

import sys
import yaml
import boto3
from datetime import datetime

def populate_metadata(config_path, region='us-east-1'):
    """Populate DynamoDB tables from YAML configuration"""
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    database_name = config['database_name']
    tables = config['tables']
    
    dynamodb = boto3.resource('dynamodb', region_name=region)
    load_type_table = dynamodb.Table('TABLE_LOAD_TYPE')
    run_time_table = dynamodb.Table('TABLE_RUN_TIME_METADATA')
    
    print(f"\nPopulating Metadata for Database: {database_name}\n")
    
    for table_config in tables:
        table_name = table_config['name']
        load_type = table_config.get('load_type', 'full_load')
        
        print(f"Processing: {table_name}")
        
        item = {
            'database_name': database_name,
            'table_name': table_name,
            'load_type': load_type,
            'partition_col': table_config.get('partition_col', ''),
            'is_partitioned': table_config.get('is_partitioned', 'FALSE'),
            'incremental_col1': table_config.get('incremental_col1', ''),
            'incremental_col2': table_config.get('incremental_col2', ''),
            'incremental_col3': table_config.get('incremental_col3', '')
        }
        
        load_type_table.put_item(Item=item)
        print(f"  ✅ Added to TABLE_LOAD_TYPE")
        
        if load_type == 'cdc_load':
            cdc_start_date = table_config.get('cdc_start_date', '2024-01-01 00:00:00.000000')
            run_time_table.put_item(Item={
                'database_name': database_name,
                'table_name': table_name,
                'last_updated_on': cdc_start_date
            })
            print(f"  ✅ Initialized CDC timestamp: {cdc_start_date}")
        
        print()
    
    print(f"Total tables configured: {len(tables)}")

def verify_metadata(database_name, region='us-east-1'):
    """Verify metadata was populated correctly"""
    
    dynamodb = boto3.resource('dynamodb', region_name=region)
    load_type_table = dynamodb.Table('TABLE_LOAD_TYPE')
    
    response = load_type_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('database_name').eq(database_name)
    )
    
    tables = response['Items']
    print(f"\nFound {len(tables)} tables:")
    
    for table in tables:
        print(f"  - {table['table_name']}: {table['load_type']}")
    
    print()

def main():
    if len(sys.argv) < 2:
        print("Usage: python populate_metadata.py tables_config.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    try:
        populate_metadata(config_path)
        
        if '--verify' in sys.argv:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            verify_metadata(config['database_name'])
        
        print("✅ Success!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
