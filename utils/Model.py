import polars as pl
import config.connections as conn

# base class for all models ETL
class Model:
    
    # TODO check base class for all models ETL
    def __init__(self, table_name):
        self.table_name = table_name
        self.postgres_connection = conn.postgresql_connection()
        self.mssql_connection = conn.msserver_connection()

    def extract(self,query) -> pl.DataFrame:
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        return df

    def transform(self) -> pl.DataFrame:
        pass
    def load(self):
        pass



