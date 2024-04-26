import polars as pl
from utils.Model import Model


class Inspractmaq(Model):
    def __init__(self):
        super().__init__(table_name="INSRACTMAQ")
    
    #TODO: check if the query is correct AND tipo column fill with the correct values
    def extract(self) -> pl.DataFrame:
        query_INSPRACTMAQ = """      
            select 
                value as IdInsPracMaq, 
                text as InsPracMaq, 
            case 
                when r.categoriesid = '573c5634-9e89-4d6c-b9df-b8ee6f59c93a' then 'Practica'
                when r.categoriesid = 'a117f4fa-973c-417c-9730-a67794f7a732' then 'Insumo'
                when r.categoriesid = '71f41838-6868-4d2f-ad31-c830032893b4' then 'Maquina_Equipo'
                else 'Otro' 
            end as Tipo 
            from 
                ws_dea.reusablecategoricaloptions r 
            where 
                r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                and (r.categoriesid = '573c5634-9e89-4d6c-b9df-b8ee6f59c93a' 
                    or r.categoriesid = 'a117f4fa-973c-417c-9730-a67794f7a732' 
                    or r.categoriesid = '71f41838-6868-4d2f-ad31-c830032893b4')
        """

        df = pl.DataFrame(pl.read_database_uri(query=query_INSPRACTMAQ, uri=self.postgres_connection, engine='connectorx'))

        #make unique index id for each row
        ids = [ i for i in range(1, df.shape[0] + 1)]
        df = df.with_columns(pl.Series("IdInsPracMaq", ids))
        return df
    
    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract  = df_extract.with_columns(df_extract['IdInsPracMaq'].cast(pl.Int32), df_extract['inspracmaq'].cast(pl.Utf8), df_extract['tipo'].cast(pl.Utf8))
        df_extract  = df_extract.rename({ "IdInsPracMaq": "IdInsPracMaq", "inspracmaq": "InsPracMaq", "tipo":"Tipo" })
        
        return df_extract
    
    def load(self):
        #load on sql server
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name="Inspractmaq", connection=self.mssql_connection, if_table_exists="append")
            print('Inspractmaq Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from INSPRACTMAQ
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdInsPracMaq", how="left")

        #select only rows that are different
        df_diff = df_diff.filter(pl.col("InsPracMaq_right").is_null()).select(pl.col("IdInsPracMaq"), pl.col("InsPracMaq"), pl.col("Tipo"))
        return df_diff
        