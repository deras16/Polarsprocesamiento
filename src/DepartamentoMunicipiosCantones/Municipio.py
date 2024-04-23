import os; import polars as pl; import json
from utils.Model import Model

class Municipio(Model):
    def __init__(self):
        super().__init__(table_name="Municipio")


    """ select r.value IdMunicipio,r."text" municipio  from ws_dea.reusablecategoricaloptions r 
       where categoriesid  = 'c0eac36c-1598-dd17-53ed-9fb351d194dd' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf'  """
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/departmentsData.json')
        data = json.load(open(path))

        data_municipio = [{"IdMunicipio": y["IdMunicipio"], "Municipio": y["Municipio"], "IdDepto": x["IdDepto"]} for x in data for y in x["Municipios"]]
        df_municipios = pl.DataFrame(data_municipio)
        return df_municipios

    def transform(self):
        df_extract = self.extract()
        df_extract = df_extract.with_columns(df_extract['IdMunicipio'].cast(pl.Int32), df_extract['Municipio'].cast(pl.Utf8), df_extract['IdDepto'].cast(pl.Int32))
        return df_extract

    def load(self):
        df_load = self.__validateData(self.transform())
        
        if df_load.shape[0] > 0:
            df_load.write_database(table_name="Municipio", connection=self.mssql_connection, if_table_exists="append")
            print('Municipio Data loaded')
        else:
            print('No data to load')
    
    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Municipio
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))

        df_diff = df_transform.join(df_sql_server, on="IdMunicipio", how="left")
        df_diff = df_diff.filter(pl.col("Municipio_right").is_null()).select(pl.col("IdMunicipio"), pl.col("Municipio"), pl.col("IdDepto"))
        return df_diff