import os
import polars as pl
import json
from utils.Model import Model

class Canton(Model):
    def __init__(self):
        super().__init__(table_name="Canton")

    """ select r.value IdCanton, r."text" canton  from ws_dea.reusablecategoricaloptions r 
       where categoriesid  = '4bc2d4f4-b202-5279-b1b4-b1758e11033c' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf' """
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/departmentsData.json')
        data = json.load(open(path))

        IdCanton = 1
        new_object = []
        for x in data:
            for y in x["Municipios"]:
                for z in y["Cantones"]:
                    new_object.append({"IdCanton": IdCanton, "Canton": z, "IdMunicipio": y["IdMunicipio"]})
                    IdCanton += 1

        df_cantones = pl.DataFrame(new_object)
        return df_cantones
    
    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract = df_extract.with_columns(df_extract['IdCanton'].cast(pl.Int32), df_extract['Canton'].cast(pl.Utf8), df_extract['IdMunicipio'].cast(pl.Int32))
        return df_extract
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name="Canton", connection=self.mssql_connection, if_table_exists="append")
            print('Canton Data loaded')
        else:
            print('No data to load')

        
    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Canton
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        df_diff = df_transform.join(df_sql_server, on="IdCanton", how="left")
        df_diff = df_diff.filter(pl.col("Canton_right").is_null()).select(pl.col("IdCanton"), pl.col("Canton"), pl.col("IdMunicipio"))
        return df_diff