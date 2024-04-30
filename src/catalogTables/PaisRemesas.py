import polars as pl
from utils.Model import Model

class Pais(Model):

    def __init__(self):
        super().__init__(table_name="PaisRemesas")

    def extract(self) -> pl.DataFrame:
        query = """      
            select r.value idpais , r."text" Pais from ws_dea.reusablecategoricaloptions r 
                where categoriesid  = '84136944-2924-7c08-11ae-491e13f348b6' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf'
        """
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        df = df.sort("idpais")
        return df
    
    def transform(self) -> pl.DataFrame:
        #sql server two columns idPais and Pais
        df_extract = self.extract()

        #convert to sql server
        df_extract  = df_extract.with_columns(df_extract['idpais'].cast(pl.Int32), df_extract['pais'].cast(pl.Utf8))
        df_extract  = df_extract.rename({ "idpais": "IdPais", "pais": "Pais" })

        return df_extract
    
    def load(self):
        #load on sql server
        df_load = self.__validateData(self.transform())

        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('PaisesRemesas Data loaded')
        else:
            print('No data to load')


    #private method
    def __validateData(self, df_transform) -> pl.DataFrame: 
        
        querySQLServer = """
            select * from PaisRemesas
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdPais", how="left")
    
        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Pais_right").is_null()).select(pl.col("IdPais"), pl.col("Pais"))
        return df_diff
    