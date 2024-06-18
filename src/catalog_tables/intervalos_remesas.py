import json
import os
import polars as pl 
from utils.Model import Model

class IntervaloRemesa(Model):
    def __init__(self):
        super().__init__(table_name="IntervalosRemesas", id_column="IdIntervalo")

    def extract_query(self) -> str:
        return ""
    #Override
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/intervalos.json')
        data = json.load(open(path))
        df = pl.DataFrame(data)
        return df
    
    def transform_mappings(self) -> dict:
        return {
            "id": ("IdIntervalo", pl.Int32),
            "name": ("Intervalo", pl.Utf8)
        }