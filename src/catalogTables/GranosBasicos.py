import polars as pl 
from utils.Model import Model

class Grano(Model):
    def __init__(self):
        super().__init__(table_name="GranosBasicos")

    def extract(self) -> pl.DataFrame:
        query = """
            select 
                value as idgrano, 
                text as grano 
            from 
                ws_dea.reusablecategoricaloptions r 
            where 
                r.categoriesid = 'f3c93701-b8e5-4d86-b7bc-e1836fe9fdb3'
            order by idgrano
        """

        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        return df
    
    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract  = df_extract.with_columns(df_extract['idgrano'].cast(pl.Int32), df_extract['grano'].cast(pl.Utf8))
        df_extract  = df_extract.rename({ "idgrano": "IdGrano", "grano": "Grano" })
        
        return df_extract
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('GranosBasicos Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from GranosBasicos
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx')) 

        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdGrano", how="left")

        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Grano_right").is_null()).select(pl.col("IdGrano"), pl.col("Grano"))
        return df_diff