import json
import os
import polars as pl 
from utils.Model import Model

class IntervaloRemesa(Model):
    def __init__(self):
        super().__init__(table_name="IntervalosRemesas")

    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/intervalos.json')
        data = json.load(open(path))
        df = pl.DataFrame(data)
        return df
    
    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract = df_extract.rename({ "id": "IdIntervalo", "name": "Intervalo" })
        df_extract = df_extract.with_columns(df_extract['IdIntervalo'].cast(pl.Int32), df_extract['Intervalo'].cast(pl.Utf8))
        return df_extract
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('IntervaloRemesa Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame:
        
        querySQLServer = f' select * from {self.table_name}'

        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx')) 

        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdIntervalo", how="left")

        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Intervalo_right").is_null()).select(pl.col("IdIntervalo"), pl.col("Intervalo"))
        return df_diff