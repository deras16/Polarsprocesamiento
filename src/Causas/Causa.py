import polars as pl
from utils.Model import Model

class Causa(Model):
    def __init__(self):
        super().__init__(table_name="Causas")

    
    def extract(self) -> pl.DataFrame:
        query = """      
            select r.value idcausa, r."text" causa  from ws_dea.reusablecategoricaloptions r where 
                categoriesid = 'b6a40e0c-4b1e-48fe-8313-6b3c35b35925' and questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf'
        """
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        df = df.sort("idcausa")
        return df
    

    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        #convert to sql server
        df_extract  = df_extract.with_columns(df_extract['idcausa'].cast(pl.Int32), df_extract['causa'].cast(pl.Utf8))
        df_extract  = df_extract.rename({ "idcausa": "IdCausa", "causa": "Causa" })
        
        #add TipoCausa TODO check how to add the final production value to this column
        df_extract = df_extract.with_columns(pl.lit("Causa").alias("TipoCausa"))
        return df_extract
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Causa Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Causas
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdCausa", how="left")

        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Causa_right").is_null()).select(pl.col("IdCausa"), pl.col("Causa"), pl.col("TipoCausa"))
        return df_diff
    