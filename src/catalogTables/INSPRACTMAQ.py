import polars as pl
from utils.Model import Model


class INSPRACTMAQ(Model):
    def __init__(self):
        super().__init__(table_name="INSPRACTMAQ", id_column="IdInsPracMaq")
    
    def extract_query(self) -> str:
        return """      
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
    
    #Override
    def extract(self) -> pl.DataFrame:
        df = super().extract()
        #make unique index id for each row
        ids = [ i for i in range(1, df.shape[0] + 1)]
        df = df.with_columns(pl.Series("idinspracmaq", ids))
        return df
    
    def transform_mappings(self) -> dict:
        return {
            "idinspracmaq": ("IdInsPracMaq", pl.Int32),
            "inspracmaq": ("InsPracMaq", pl.Utf8),
            "tipo": ("Tipo", pl.Utf8)
        }