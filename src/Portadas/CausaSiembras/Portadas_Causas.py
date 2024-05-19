import polars as pl
from utils.Model import Model


class PortadaCausaSiembra(Model):
    def __init__(self):
        super().__init__(table_name="CausaSiembra")

    def extract(self) -> pl.DataFrame:
        query = """
            select interview__id, unnest(resul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            where e.resultado = 1 order by fecha_entr  
        """
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        return df


    def transfom(self) -> pl.DataFrame:
        df = self.extract()
        df = df.rename({"interview__id": "IdPortada",  "unnest": "IdCausa"})
        df = df.with_columns(df['IdPortada'].cast(pl.Utf8), df['IdCausa'].cast(pl.Int32))
        return df
    
    def load(self):
        df  = self.__validateIfExist(self.transfom())
        if df.shape[0] > 0:
            df.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('CausaSiembra Data loaded')
        else:
            print('No data to load')


    def __validateIfExist(self, df: pl.DataFrame) -> pl.DataFrame:
        query = f"""
            select * from {self.table_name}
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=query, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df.join(df_sql_server, on="IdPortada", how="semi")
        
        #delete existing rows on df
        df_filter = df.filter(~df["IdPortada"].is_in(df_result['IdPortada']))

        #validate if IdPortada exist in Portada table
        df_filter = self.__validatePortada(df_filter)
        return df_filter
    
    def __validatePortada(self, df: pl.DataFrame) -> pl.DataFrame:
        query = """
            select * from Portada
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=query, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df.join(df_sql_server, on="IdPortada", how="semi")

        return df_result
