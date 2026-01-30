from abc import ABC, abstractmethod

class BaseReader(ABC):
    def __init__(self, spark):
        self.spark = spark

    @abstractmethod
    def read(self):
        pass
