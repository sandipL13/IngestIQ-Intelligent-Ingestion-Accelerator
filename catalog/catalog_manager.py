"""
IngestIQ Catalog - Table Metadata Management
"""
import boto3

class CatalogManager:
    def __init__(self, region="us-east-1"):
        self.dynamodb = boto3.resource("dynamodb", region_name=region)
        self.run_time_table = self.dynamodb.Table("TABLE_RUN_TIME_METADATA")
        self.load_type_table = self.dynamodb.Table("TABLE_LOAD_TYPE")
    
    def get_table_load_type_map(self, database_name):
        """Get load type mapping for all tables in database"""
        response = self.load_type_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('database_name').eq(database_name)
        )
        
        items = response.get("Items", [])
        while "LastEvaluatedKey" in response:
            response = self.load_type_table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('database_name').eq(database_name),
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))
        
        return {
            (item["database_name"], item["table_name"]): {
                "load_type": item.get("load_type", "full_load"),
                "partition_col": item.get("partition_col"),
                "is_partitioned": item.get("is_partitioned", "FALSE"),
                "incremental_col1": item.get("incremental_col1"),
                "incremental_col2": item.get("incremental_col2"),
                "incremental_col3": item.get("incremental_col3")
            }
            for item in items
        }
    
    def get_last_run_timestamp(self, database_name, table_name):
        """Get last run timestamp for CDC"""
        key = {"database_name": database_name, "table_name": table_name}
        response = self.run_time_table.get_item(Key=key)
        
        if "Item" in response:
            return response["Item"]["last_updated_on"]
        else:
            raise ValueError(f"No timestamp found for {database_name}.{table_name}")
    
    def update_last_run_timestamp(self, database_name, table_name, timestamp):
        """Update last run timestamp after successful processing"""
        self.run_time_table.update_item(
            Key={"database_name": database_name, "table_name": table_name},
            UpdateExpression="SET last_updated_on = :timestamp",
            ExpressionAttributeValues={":timestamp": timestamp},
            ReturnValues="UPDATED_NEW"
        )
    
    def add_table(self, database_name, table_name, load_type="full_load", 
                  partition_col=None, is_partitioned="FALSE", incremental_cols=None):
        """Add table to catalog"""
        item = {
            "database_name": database_name,
            "table_name": table_name,
            "load_type": load_type,
            "partition_col": partition_col,
            "is_partitioned": is_partitioned
        }
        
        if incremental_cols:
            for i, col in enumerate(incremental_cols[:3], 1):
                item[f"incremental_col{i}"] = col
        
        self.load_type_table.put_item(Item=item)

# Sample 10 tables setup
SAMPLE_TABLES = [
    {"table_name": "customers", "load_type": "full_load", "partition_col": "customer_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["created_at", "updated_at"]},
    {"table_name": "orders", "load_type": "cdc_load", "partition_col": "order_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["order_date", "updated_at"]},
    {"table_name": "products", "load_type": "full_load", "partition_col": "product_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["created_at"]},
    {"table_name": "order_items", "load_type": "cdc_load", "partition_col": "item_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["created_at", "updated_at"]},
    {"table_name": "categories", "load_type": "full_load", "partition_col": None, 
     "is_partitioned": "FALSE", "incremental_cols": ["updated_at"]},
    {"table_name": "suppliers", "load_type": "cdc_load", "partition_col": "supplier_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["created_at", "updated_at"]},
    {"table_name": "inventory", "load_type": "cdc_load", "partition_col": "product_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["last_updated"]},
    {"table_name": "payments", "load_type": "cdc_load", "partition_col": "payment_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["payment_date", "updated_at"]},
    {"table_name": "reviews", "load_type": "cdc_load", "partition_col": "review_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["review_date", "updated_at"]},
    {"table_name": "user_sessions", "load_type": "cdc_load", "partition_col": "session_id", 
     "is_partitioned": "TRUE", "incremental_cols": ["session_start", "last_activity"]}
]

def setup_catalog(database_name="sales_db"):
    """Setup sample catalog with 10 tables"""
    catalog = CatalogManager()
    for table in SAMPLE_TABLES:
        catalog.add_table(database_name, **table)
    print(f"Added {len(SAMPLE_TABLES)} tables to catalog for {database_name}")

if __name__ == "__main__":
    setup_catalog()