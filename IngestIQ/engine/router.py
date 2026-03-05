"""
IngestIQ Job Router
"""
from jobs.etl_job import ETLJob

class JobRouter:
    def __init__(self, config):
        self.config = config
    
    def route_job(self):
        """Route to appropriate job based on source type"""
        source_type = self.config.get("source_type", "").lower()
        
        if source_type in ["mysql", "postgresql", "oracle", "sqlserver"]:
            job = ETLJob(self.config)
            return job.execute()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")