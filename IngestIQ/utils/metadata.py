"""
IngestIQ Metadata Manager - Multi-Cloud
"""
from datetime import datetime, timedelta
from pyspark.sql.functions import max as sp_max, col

class MetadataManager:
    def __init__(self, config):
        self.config = config
        self.cloud_provider = config.get("cloud_provider", "aws")
        self._init_metadata_client()
    
    def _init_metadata_client(self):
        """Initialize cloud-specific metadata client"""
        if self.cloud_provider == "aws":
            import boto3
            self.client = boto3.resource("dynamodb", region_name=self.config.get("region", "ap-south-1"))
            self.run_time_table = self.client.Table("TABLE_RUN_TIME_METADATA")
            self.load_type_table = self.client.Table("TABLE_LOAD_TYPE")
        
        elif self.cloud_provider == "azure":
            from azure.cosmos import CosmosClient
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            self.client = CosmosClient("https://default-cosmos.documents.azure.com:443/", credential)
            self.database = self.client.get_database_client("metadata")
            self.run_time_container = self.database.get_container_client("run_time")
            self.load_type_container = self.database.get_container_client("load_type")
        
        elif self.cloud_provider == "gcp":
            from google.cloud import firestore
            self.client = firestore.Client()
            self.run_time_collection = self.client.collection("run_time_metadata")
            self.load_type_collection = self.client.collection("load_type_metadata")
    
    def get_tables_to_process(self):
        """Get list of tables with their metadata"""
        database_name = self.config["database_name"]
        
        if self.cloud_provider == "aws":
            return self._get_tables_aws(database_name)
        elif self.cloud_provider == "azure":
            return self._get_tables_azure(database_name)
        elif self.cloud_provider == "gcp":
            return self._get_tables_gcp(database_name)
    
    def _get_tables_aws(self, database_name):
        """Get tables from DynamoDB"""
        import boto3
        response = self.load_type_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('database_name').eq(database_name)
        )
        
        tables = []
        for item in response["Items"]:
            table_info = {
                "table_name": item["table_name"],
                "load_type": item.get("load_type", "full_load"),
                "partition_col": item.get("partition_col"),
                "is_partitioned": item.get("is_partitioned"),
                "incremental_cols": self._get_incremental_cols(item)
            }
            
            if table_info["load_type"] == "cdc_load":
                table_info["last_timestamp"] = self._get_last_timestamp_aws(database_name, item["table_name"])
            
            tables.append(table_info)
        
        return tables
    
    def _get_tables_azure(self, database_name):
        """Get tables from Cosmos DB"""
        query = f"SELECT * FROM c WHERE c.database_name = '{database_name}'"
        items = list(self.load_type_container.query_items(query=query, enable_cross_partition_query=True))
        
        tables = []
        for item in items:
            table_info = {
                "table_name": item["table_name"],
                "load_type": item.get("load_type", "full_load"),
                "partition_col": item.get("partition_col"),
                "is_partitioned": item.get("is_partitioned"),
                "incremental_cols": self._get_incremental_cols(item)
            }
            
            if table_info["load_type"] == "cdc_load":
                table_info["last_timestamp"] = self._get_last_timestamp_azure(database_name, item["table_name"])
            
            tables.append(table_info)
        
        return tables
    
    def _get_tables_gcp(self, database_name):
        """Get tables from Firestore"""
        docs = self.load_type_collection.where("database_name", "==", database_name).stream()
        
        tables = []
        for doc in docs:
            item = doc.to_dict()
            table_info = {
                "table_name": item["table_name"],
                "load_type": item.get("load_type", "full_load"),
                "partition_col": item.get("partition_col"),
                "is_partitioned": item.get("is_partitioned"),
                "incremental_cols": self._get_incremental_cols(item)
            }
            
            if table_info["load_type"] == "cdc_load":
                table_info["last_timestamp"] = self._get_last_timestamp_gcp(database_name, item["table_name"])
            
            tables.append(table_info)
        
        return tables
    
    def _get_incremental_cols(self, item):
        """Extract incremental columns from metadata"""
        cols = []
        for i in range(1, 4):
            col_name = item.get(f"incremental_col{i}")
            if col_name and col_name.strip():
                cols.append(col_name.strip())
        return cols
    
    def _get_last_timestamp_aws(self, database_name, table_name):
        """Get last timestamp from DynamoDB"""
        try:
            response = self.run_time_table.get_item(
                Key={"database_name": database_name, "table_name": table_name}
            )
            if "Item" in response:
                timestamp = response["Item"]["last_updated_on"]
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
                return (dt - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
        return None
    
    def _get_last_timestamp_azure(self, database_name, table_name):
        """Get last timestamp from Cosmos DB"""
        try:
            item = self.run_time_container.read_item(
                item=f"{database_name}_{table_name}", 
                partition_key=database_name
            )
            timestamp = item["last_updated_on"]
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
            return (dt - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
        return None
    
    def _get_last_timestamp_gcp(self, database_name, table_name):
        """Get last timestamp from Firestore"""
        try:
            doc = self.run_time_collection.document(f"{database_name}_{table_name}").get()
            if doc.exists:
                timestamp = doc.to_dict()["last_updated_on"]
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
                return (dt - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
        return None
    
    def update_timestamp(self, table_name, df):
        """Update last run timestamp after successful load"""
        database_name = self.config["database_name"]
        
        # Get max timestamp from incremental columns
        incremental_cols = self._get_table_incremental_cols(table_name)
        if not incremental_cols:
            return
        
        agg_exprs = [sp_max(col(c)).alias(c) for c in incremental_cols]
        max_vals = df.agg(*agg_exprs).collect()[0].asDict()
        
        # Find the latest timestamp
        timestamps = [v for v in max_vals.values() if v is not None]
        if timestamps:
            max_timestamp = str(max(timestamps))
            
            if self.cloud_provider == "aws":
                self._update_timestamp_aws(database_name, table_name, max_timestamp)
            elif self.cloud_provider == "azure":
                self._update_timestamp_azure(database_name, table_name, max_timestamp)
            elif self.cloud_provider == "gcp":
                self._update_timestamp_gcp(database_name, table_name, max_timestamp)
    
    def _update_timestamp_aws(self, database_name, table_name, timestamp):
        """Update timestamp in DynamoDB"""
        self.run_time_table.update_item(
            Key={"database_name": database_name, "table_name": table_name},
            UpdateExpression="SET last_updated_on = :timestamp",
            ExpressionAttributeValues={":timestamp": timestamp}
        )
    
    def _update_timestamp_azure(self, database_name, table_name, timestamp):
        """Update timestamp in Cosmos DB"""
        item = {
            "id": f"{database_name}_{table_name}",
            "database_name": database_name,
            "table_name": table_name,
            "last_updated_on": timestamp
        }
        self.run_time_container.upsert_item(item)
    
    def _update_timestamp_gcp(self, database_name, table_name, timestamp):
        """Update timestamp in Firestore"""
        doc_ref = self.run_time_collection.document(f"{database_name}_{table_name}")
        doc_ref.set({
            "database_name": database_name,
            "table_name": table_name,
            "last_updated_on": timestamp
        })
    
    def _get_table_incremental_cols(self, table_name):
        """Get incremental columns for specific table"""
        database_name = self.config["database_name"]
        
        if self.cloud_provider == "aws":
            response = self.load_type_table.get_item(
                Key={"database_name": database_name, "table_name": table_name}
            )
            if "Item" in response:
                return self._get_incremental_cols(response["Item"])
        
        elif self.cloud_provider == "azure":
            try:
                item = self.load_type_container.read_item(
                    item=f"{database_name}_{table_name}",
                    partition_key=database_name
                )
                return self._get_incremental_cols(item)
            except:
                pass
        
        elif self.cloud_provider == "gcp":
            doc = self.load_type_collection.document(f"{database_name}_{table_name}").get()
            if doc.exists:
                return self._get_incremental_cols(doc.to_dict())
        
        return []ed_on": timestamp
        })
    
    def _get_table_incremental_cols(self, table_name):
        """Get incremental columns for specific table"""
        database_name = self.config["database_name"]
        
        if self.cloud_provider == "aws":
            response = self.load_type_table.get_item(
                Key={"database_name": database_name, "table_name": table_name}
            )
            if "Item" in response:
                return self._get_incremental_cols(response["Item"])
        
        elif self.cloud_provider == "azure":
            try:
                item = self.load_type_container.read_item(
                    item=f"{database_name}_{table_name}",
                    partition_key=database_name
                )
                return self._get_incremental_cols(item)
            except:
                pass
        
        elif self.cloud_provider == "gcp":
            doc = self.load_type_collection.document(f"{database_name}_{table_name}").get()
            if doc.exists:
                return self._get_incremental_cols(doc.to_dict())
        
        return []