import polars as pl
from utils.Model import Model

class Pais(Model):

    def __init__(self):
        super().__init__(table_name="PaisRemesas", id_column="IdPais")

    def extract_query(self) -> str:
        return """
            select r.value idpais , r."text" Pais from ws_dea.reusablecategoricaloptions r 
            where categoriesid  = '84136944-2924-7c08-11ae-491e13f348b6' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf' order by idpais
        """

    def transform_mappings(self) -> dict:
        return {
            "idpais": ("IdPais", pl.Int32),
            "pais": ("Pais", pl.Utf8)
        }