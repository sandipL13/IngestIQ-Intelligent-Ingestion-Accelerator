from core.loader import BaseLoader

class RedshiftLoader(BaseLoader):
    def __init__(self, jdbc_url, table, user, password):
        self.opts = {
            "url": jdbc_url,
            "dbtable": table,
            "user": user,
            "password": password
        }

    def load(self, df):
        df.write.format("jdbc").options(**self.opts).mode("append").save()
