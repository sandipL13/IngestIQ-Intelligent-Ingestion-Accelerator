"""
IngestIQ Logger - Multi-Cloud
"""
import logging
from datetime import datetime
import pytz
from io import StringIO

class Logger:
    def __init__(self, config):
        self.config = config
        self.cloud_provider = config.get("cloud_provider", "aws")
        self.bucket_name = config.get("bucket_name") or config.get("storage_account") or config.get("gcs_bucket", "default-bucket")
        self.database_name = config["database_name"]
        self.log_stream = StringIO()
        
        self.logger = logging.getLogger(f"IngestIQ_{self.database_name}")
        self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        handler = logging.StreamHandler(self.log_stream)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def upload_to_storage(self):
        """Upload logs to cloud storage"""
        try:
            ist = pytz.timezone("Asia/Kolkata")
            timestamp = datetime.now(ist).strftime("%Y-%m-%d_%H%M%S")
            
            if self.cloud_provider == "aws":
                return self._upload_to_s3(timestamp)
            elif self.cloud_provider == "azure":
                return self._upload_to_adls(timestamp)
            elif self.cloud_provider == "gcp":
                return self._upload_to_gcs(timestamp)
        except Exception:
            return False
    
    def _upload_to_s3(self, timestamp):
        """Upload to S3"""
        import boto3
        s3 = boto3.client("s3")
        s3_key = f"ingestiq-logs/{self.database_name}/{timestamp}/job.log"
        
        s3.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=self.log_stream.getvalue().encode("utf-8")
        )
        return True
    
    def _upload_to_adls(self, timestamp):
        """Upload to Azure Data Lake Storage"""
        from azure.storage.blob import BlobServiceClient
        from azure.identity import DefaultAzureCredential
        
        credential = DefaultAzureCredential()
        blob_service = BlobServiceClient(
            account_url=f"https://{self.bucket_name}.blob.core.windows.net",
            credential=credential
        )
        
        blob_name = f"ingestiq-logs/{self.database_name}/{timestamp}/job.log"
        blob_client = blob_service.get_blob_client(container="logs", blob=blob_name)
        blob_client.upload_blob(self.log_stream.getvalue(), overwrite=True)
        return True
    
    def _upload_to_gcs(self, timestamp):
        """Upload to Google Cloud Storage"""
        from google.cloud import storage
        
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        blob_name = f"ingestiq-logs/{self.database_name}/{timestamp}/job.log"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(self.log_stream.getvalue())
        return True