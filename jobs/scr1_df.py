from core.reader import BaseReader

class JdbcReadJob(BaseReader):
    def __init__(self, spark, jdbc_url, table, user, password):
        super().__init__(spark)
        self.opts = {
            "url": jdbc_url,
            "dbtable": table,
            "user": user,
            "password": password
        }

    def read(self):
        return self.spark.read.format("jdbc").options(**self.opts).load()
