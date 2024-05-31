import polars as pl
from utils.Model import Model
from tqdm import tqdm
import re
class PortadaInspracmaq(Model):
    def __init__(self):
        super().__init__(table_name="INSPRACTMAQSiembra", id_column="IdPortada")
    
    def extract_query(self) -> str:
        return """
            with cteInsumos(interviewid, idinsumo) as(
            select e.interview__id, unnest(e.insumos) idinsumo 
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            where e.insumos is not null and (e.resultado = 1 or e.resultadost = 1)
            ), ctePracticas(interviewid, idpractica) as(
            select e2.interview__id, unnest(e2.practicas) idpractica 
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2
            where e2.practicas is not null and (e2.resultado = 1 or e2.resultadost = 1)
            ), cteEquipoMaquinaria(interviewid, idequipomaq) as (
            select e3.interview__id, unnest(e3.equipo_y_maquinaria) id 
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e3
            where e3.equipo_y_maquinaria is not null and (e3.resultado = 1 or e3.resultadost = 1)
            )
            select ctp.interviewid, r."text" inspracmaq, 'Practicas' Tipo from ctePracticas ctp
            inner join ws_dea.reusablecategoricaloptions r on r.value = ctp.idpractica and 
            r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '573c5634-9e89-4d6c-b9df-b8ee6f59c93a'
            union all
            select cti.interviewid, r2."text", 'Insumos'  from cteInsumos cti
            inner join ws_dea.reusablecategoricaloptions r2 on r2.value = cti.idinsumo and  
            r2.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r2.categoriesid = 'a117f4fa-973c-417c-9730-a67794f7a732'
            union all
            select ctem.interviewid, r3."text", 'Maquinaria y equipo'  from cteEquipoMaquinaria ctem
            inner join ws_dea.reusablecategoricaloptions r3 on r3.value = ctem.idequipomaq and
            r3.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r3.categoriesid = '71f41838-6868-4d2f-ad31-c830032893b4'
        """
    
    def transform_mappings(self) -> dict:
        return {
            "interviewid": ("IdPortada", pl.Utf8),
            "inspracmaq": ("InsPracMaq", pl.Utf8),
            "tipo": ("Tipo", pl.Utf8)
        }
    
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transformed = super().transform(df)

        df_transformed = df_transformed.with_columns(df_transformed["InsPracMaq"].str.replace("\n", " "))

        query_inspractmaq = """
            select * from INSPRACTMAQ
        """
        df_inspractmaq = pl.DataFrame(pl.read_database_uri(query=query_inspractmaq, uri=self.mssql_connection, engine='connectorx'))
        df_transformed = df_transformed.join(df_inspractmaq, left_on=["InsPracMaq", "Tipo"], right_on=["InsPracMaq", "Tipo"], how='left')
        df_transformed = df_transformed.select(["IdPortada", "IdInsPracMaq"])

        return df_transformed
    
    def load(self, df: pl.DataFrame):
        df_load = super()._check_different_rows(df)
        df_load = self.__validatePortada(df_load)
        total_rows = df_load.shape[0]

        if total_rows > 0:
            with tqdm(total=1, desc=f"Loading {self.table_name} data") as pbar:
                df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
                pbar.update(1)
            tqdm.write(f"{self.table_name} Data Loading Completed.") 
        else:
            tqdm.write(f'No data to load for {self.table_name} table.')
       
    def __validatePortada(self, df: pl.DataFrame) -> pl.DataFrame:
        query = """
            select * from Portada
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=query, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df.join(df_sql_server, on="IdPortada", how="semi")
        
        return df_result

