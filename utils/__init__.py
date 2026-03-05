"""
IngestIQ Utils - Secrets, Logger, Notifications
"""
import boto3
import json
import logging
import pytz
from datetime import datetime
from io import StringIO
import requests

class SecretsManager:
    def __init__(self, region="us-east-1"):
        self.region = region
        self.client = boto3.client('secretsmanager', region_name=region)
    
    def get_secret(self, secret_name):
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            return json.loads(response['SecretString'])
        except Exception as e:
            raise Exception(f"Failed to get secret {secret_name}: {str(e)}")

class Logger:
    def __init__(self, bucket_name, database_name, table_name):
        self.bucket_name = bucket_name
        self.database_name = database_name
        self.table_name = table_name
        self.log_stream = StringIO()
        
        self.logger = logging.getLogger(f"{database_name}_{table_name}")
        self.logger.setLevel(logging.INFO)
        
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        handler = logging.StreamHandler(self.log_stream)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.logger.info(f"Logger initialized for {database_name}.{table_name}")
    
    def upload_to_s3(self):
        try:
            s3 = boto3.client("s3")
            ist = pytz.timezone("Asia/Kolkata")
            timestamp = datetime.now(ist).strftime("%Y-%m-%d_%H%M%S")
            s3_key = f"glue-logs/{self.database_name}/{self.table_name}/{timestamp}/job.log"
            
            s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=self.log_stream.getvalue().encode("utf-8")
            )
            return True
        except Exception:
            return False

class NotificationManager:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
    
    def send(self, database_name, status, message=None):
        if not self.webhook_url:
            return
        
        if status.lower() == "success":
            color = "#36a64f"
            text = f":white_check_mark: *{database_name}* completed successfully!"
        else:
            color = "#ff0000"
            text = f":x: *{database_name}* failed. Please check logs."
        
        if message:
            text += f"\n{message}"
        
        payload = {
            "attachments": [{
                "color": color,
                "title": "IngestIQ Job Notification",
                "text": text
            }]
        }
        
        try:
            requests.post(self.webhook_url, data=json.dumps(payload), 
                         headers={"Content-Type": "application/json"})
        except Exception:
            pass

def get_current_time():
    ist = pytz.timezone("Asia/Kolkata")
    return datetime.now(ist)

def parse_datetime(dt_str):
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return dt_str