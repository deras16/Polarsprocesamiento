import json
import os
import polars as pl 
from utils.Model import Model

class OrigenCredito(Model):
    def __init__(self):
        super().__init__(table_name="OrigenCredito", id_column="IdOrigenCredito")

    def extract_query(self) -> str:
        return ""
    #Override
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/creditos.json')
        data = json.load(open(path))

        df = pl.DataFrame(data)
        return df
    
    def transform_mappings(self) -> dict:
        return {
            "id": ("IdOrigenCredito", pl.Int32),
            "name": ("NombOrigen", pl.Utf8)
        }
