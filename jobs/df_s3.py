from core.loader import BaseLoader

class S3LoadJob(BaseLoader):
    def __init__(self, path):
        self.path = path

    def load(self, df):
        df.write.mode("append").parquet(self.path)
