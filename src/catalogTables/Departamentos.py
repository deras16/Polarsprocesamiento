import os
import polars as pl
import config.connections as conn
from utils.Model import Model 
import json

class Departamento(Model):
    def __init__(self):
        super().__init__(table_name="Departamento", id_column="IdDepto")
    
    def extract_query(self):
        return """ 
            select r.value IdDepartamento,r."text" departamento  from ws_dea.reusablecategoricaloptions r 
            where categoriesid  = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf' 
        """
    
    def transform_mappings(self):
        return {
            "iddepartamento": ("IdDepto", pl.Int32),
            "departamento": ("Departamento", pl.Utf8)
        }

    