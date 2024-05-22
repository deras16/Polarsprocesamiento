import os
import json
import polars as pl 
from utils.Model import Model

class Grano(Model):
    def __init__(self):
        super().__init__(table_name="GranosBasicos" ,id_column="IdGrano")

    def extract_query(self) -> str:
            return ""
    #Override
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/granos.json')
        data = json.load(open(path))

        df = pl.DataFrame(data)
        return df

    def transform_mappings(self) -> dict:
        return {
            "id": ("IdGrano", pl.Int32),
            "name": ("Grano", pl.Utf8)
        }