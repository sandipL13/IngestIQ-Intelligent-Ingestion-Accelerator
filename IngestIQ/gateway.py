"""
IngestIQ Gateway - Main Orchestrator
"""
from engine.router import JobRouter
from utils.logger import Logger
from utils.notification import NotificationManager

class IngestIQGateway:
    def __init__(self, config):
        self.config = config
        self.logger = Logger(config)
        self.notification = NotificationManager(config)
        self.router = JobRouter(config)
    
    def execute(self):
        try:
            self.logger.info(f"🚀 Starting IngestIQ job for: {self.config['database_name']}")
            
            # Route to appropriate job
            result = self.router.route_job()
            
            self.logger.info(f"✅ Job completed successfully")
            self.notification.send_success(self.config['database_name'])
            return result
            
        except Exception as e:
            error_msg = f"❌ Job failed: {str(e)}"
            self.logger.error(error_msg)
            self.notification.send_failure(self.config['database_name'], str(e))
            raise