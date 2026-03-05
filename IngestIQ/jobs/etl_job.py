"""
IngestIQ ETL Job - Database to S3/Snowflake
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from engine.extractor import DataExtractor
from engine.loader import DataLoader
from utils.metadata import MetadataManager

class ETLJob:
    def __init__(self, config):
        self.config = config
        self.extractor = DataExtractor(config)
        self.loader = DataLoader(config)
        self.metadata = MetadataManager(config)
    
    def execute(self):
        """Execute ETL job with multi-threading"""
        # Get tables to process
        tables = self.metadata.get_tables_to_process()
        
        results = []
        thread_count = self.config.get("thread_count", 4)
        
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = {
                executor.submit(self._process_table, table): table
                for table in tables
            }
            
            for future in as_completed(futures):
                table = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({"table": table["table_name"], "status": "FAILED", "error": str(e)})
        
        return results
    
    def _process_table(self, table_info):
        """Process individual table"""
        table_name = table_info["table_name"]
        load_type = table_info["load_type"]
        
        # Extract data
        df = self.extractor.extract_table(table_name, load_type, table_info)
        
        if df is None or df.count() == 0:
            return {"table": table_name, "status": "SKIPPED", "rows": 0}
        
        # Load to targets
        self.loader.load_to_storage(df, table_name, load_type)
        self.loader.load_to_snowflake(df, table_name, load_type)
        
        # Update metadata
        if load_type == "cdc_load":
            self.metadata.update_timestamp(table_name, df)
        
        return {"table": table_name, "status": "SUCCESS", "rows": df.count()}