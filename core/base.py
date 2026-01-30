class BaseSparkJob:
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config

    def run(self):
        raise NotImplementedError("Jobs must implement run()")
