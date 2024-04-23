import os
import polars as pl
import config.connections as conn
from utils.Model import Model 
import json

class Departamento(Model):
    def __init__(self):
        super().__init__(table_name="Departamento")

    """ select r.value IdDepartamento,r."text" departamento  from ws_dea.reusablecategoricaloptions r 
       where categoriesid  = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf' """
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/departmentsData.json')
        data = json.load(open(path))
        
        #getting only idDepto and Departamento
        df_departamento = pl.DataFrame(data).select("IdDepto", "Departamento")
        return df_departamento
    
    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract  = df_extract.with_columns(df_extract['IdDepto'].cast(pl.Int32), df_extract['Departamento'].cast(pl.Utf8))
        return df_extract

    def load(self):
        #load on sql server
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name="Departamento", connection=self.mssql_connection, if_table_exists="append")
            print('Departamento Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame: 
        querySQLServer = """
            select * from Departamento
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdDepto", how="left")
    
        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Departamento_right").is_null()).select(pl.col("IdDepto"), pl.col("Departamento"))
        return df_diff