import polars as pl
from utils.Model import Model


class Inspractmaq(Model):
    def __init__(self):
        super().__init__(table_name="INSRACTMAQ")
    
    #TODO: check if the query is correct AND tipo column fill with the correct values
    def extract(self) -> pl.DataFrame:
        query_insumos = """      
            select  text as InsPracMaq, 'Insumo' Tipo from ws_dea.reusablecategoricaloptions r 
                where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'a117f4fa-973c-417c-9730-a67794f7a732'
        """
        query_maq_equipo = """
            select text as InsPracMaq, 'Revisar' Tipo from ws_dea.reusablecategoricaloptions r 
                where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '71f41838-6868-4d2f-ad31-c830032893b4'
        """
        df_insumos = pl.DataFrame(pl.read_database_uri(query=query_insumos, uri=self.postgres_connection, engine='connectorx'))
        df_maq_equipo = pl.DataFrame(pl.read_database_uri(query=query_maq_equipo, uri=self.postgres_connection, engine='connectorx'))
        
        df = df_insumos.extend(df_maq_equipo)
        #make unique index id for each row
        ids = [ i for i in range(1, df.shape[0] + 1)]
        df = df.with_columns(pl.Series("IdInsPracMaq", ids))
        print(df)
        return df
        