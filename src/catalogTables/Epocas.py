import json
import os
import polars as pl 
from utils.Model import Model

class Epoca(Model):
    def __init__(self):
        super().__init__(table_name="Epocas")

    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/epocas.json')
        data = json.load(open(path))

        df = pl.DataFrame(data)
        return df
    
    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract = df_extract.rename({ "idepoca": "IdEpoca", "epoca": "Epoca" })
        df_extract = df_extract.with_columns(df_extract['IdEpoca'].cast(pl.Int32), df_extract['Epoca'].cast(pl.Utf8))
        return df_extract
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Epocas Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Epocas
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx')) 

        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdEpoca", how="left")

        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Epoca_right").is_null()).select(pl.col("IdEpoca"), pl.col("Epoca"))
        return df_diff