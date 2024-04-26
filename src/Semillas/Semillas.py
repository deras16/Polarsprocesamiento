import polars as pl 
from utils.Model import Model

class Semillas(Model):
    def __init__(self):
        super().__init__(table_name="Semillas")

    def extract(self) -> pl.DataFrame:
        query = """
            select 
                value as idsemilla, 
                text as semilla 
            from 
                ws_dea.reusablecategoricaloptions r 
            where 
                r.categoriesid = 'f3c93701-b8e5-4d86-b7bc-e1836fe9fdb3'
            order by idsemilla
        """

        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        df = df.sort("idsemilla")
        return df
    
    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract  = df_extract.with_columns(df_extract['idsemilla'].cast(pl.Int32), df_extract['semilla'].cast(pl.Utf8))
        df_extract  = df_extract.rename({ "idsemilla": "Idsemilla", "semilla": "Semilla" })
        
        return df_extract
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name="Semilla", connection=self.mssql_connection, if_table_exists="append")
            print('Semillas Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Semilla
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx')) 

        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="Idsemilla", how="left")

        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Semilla_right").is_null()).select(pl.col("Idsemilla"), pl.col("Semilla"))
        return df_diff