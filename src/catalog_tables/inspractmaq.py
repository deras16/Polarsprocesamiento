import json
import os
import polars as pl
from utils.Model import Model


class INSPRACTMAQ(Model):
    def __init__(self):
        super().__init__(table_name="INSPRACTMAQ", id_column="IdInsPracMaq")
    
    def extract_query(self) -> str:
        return ""
    
    #Override
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/INSPRACTMAQ.json')
        data = json.load(open(path, encoding='utf-8'))
        df = pl.DataFrame(data)
        return df
    
    def transform_mappings(self) -> dict:
        return {
            "IdInsPracMaq": ("IdInsPracMaq", pl.Int32),
            "InsPracMaq": ("InsPracMaq", pl.Utf8),
            "Tipo": ("Tipo", pl.Utf8)
        }