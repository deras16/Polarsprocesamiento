import json
import os
import polars as pl 
from utils.Model import Model


class Semilla(Model):
    def __init__(self):
        super().__init__(table_name="Semilla", id_column="Idsemilla")

    def extract_query(self) -> str:
        return ""
    #Override
    def extract(self) -> pl.DataFrame:
        path = os.path.join(os.path.dirname(__file__), 'data/semillas.json')
        data = json.load(open(path))

        df = pl.DataFrame(data)
        return df

    def transform_mappings(self) -> dict:
        return {
            "id": ("Idsemilla", pl.Int32),
            "name": ("Semilla", pl.Utf8)
        }