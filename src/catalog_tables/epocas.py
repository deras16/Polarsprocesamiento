import json
import os
import polars as pl 
from utils.Model import Model

class Epoca(Model):
    def __init__(self):
        super().__init__(table_name="Epocas", id_column="IdEpoca")

    def extract_query(self) -> str:
        return ""
    #Override
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/epocas.json')
        
        with open(path, 'r') as file:
            data = json.load(file)
        
        df = pl.DataFrame(data)
        return df
    
    def transform_mappings(self) -> dict:
        return {
            "idepoca": ("IdEpoca", pl.Int32),
            "epoca": ("Epoca", pl.Utf8)
        }