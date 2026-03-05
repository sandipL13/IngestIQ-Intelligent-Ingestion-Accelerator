"""
IngestIQ Notification Manager
"""
import json
import requests

class NotificationManager:
    def __init__(self, config):
        self.webhook_url = "https://hooks.slack.com/services/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"
    
    def send_success(self, database_name):
        """Send success notification"""
        self._send_notification(database_name, "success")
    
    def send_failure(self, database_name, error_message):
        """Send failure notification"""
        self._send_notification(database_name, "failed", error_message)
    
    def _send_notification(self, database_name, status, message=None):
        """Send Slack notification"""
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