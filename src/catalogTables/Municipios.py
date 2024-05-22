import os; import polars as pl; import json
from utils.Model import Model

class Municipio(Model):
    def __init__(self):
        super().__init__(table_name="Municipio", id_column="IdMunicipio")

    def extract_query(self) -> str:
        return """
            select r.value IdMunicipio, r.text Municipio, r.parentvalue IdDepto from ws_dea.reusablecategoricaloptions r 
            where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
        """

    def transform_mappings(self) -> dict:
        return {
            "idmunicipio": ("IdMunicipio", pl.Int32),
            "municipio": ("Municipio", pl.Utf8),
            "iddepto": ("IdDepto", pl.Int32)
        }
    
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transform = super().transform(df)

        #check for special characters
        df_transform = df_transform.with_columns(
            pl.when(pl.col("Municipio") == "MERCEDES UMA�A").then(pl.lit("MERCEDES UMAÑA")).otherwise(pl.col("Municipio")).alias("Municipio"),
        )      
        return df_transform