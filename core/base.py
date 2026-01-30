"""BaseSparkJob - minimal placeholder for spark jobs."""

class BaseSparkJob:
    def __init__(self, spark=None, logger=None):
        self.spark = spark
        self.logger = logger

    def run(self):
        raise NotImplementedError("Subclasses must implement run()")
