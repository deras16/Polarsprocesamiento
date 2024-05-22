import polars as pl
from utils.Model import Model
from tqdm import tqdm

class PortadaCausaSiembra(Model):
    def __init__(self):
        super().__init__(table_name="CausaSiembra", id_column="IdPortada")

    
    def extract_query(self) -> str:
        return """
            select interview__id, unnest(resul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            where e.resultado = 1 or e.resultadost = 1 order by fecha_entr    
        """

    def transform_mappings(self) -> dict:
        return {
            "interview__id": ("IdPortada", pl.Utf8),
            "unnest": ("IdCausa", pl.Int32)
        }
    
    def load(self, df: pl.DataFrame):
        df_load = super()._check_different_rows(df)
        df_load = self.__validatePortada(df_load)
        total_rows = df_load.shape[0]

        if total_rows > 0:
            with tqdm(total=1, desc=f"Loading {self.table_name} data") as pbar:
                df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
                pbar.update(1)
            tqdm.write(f"{self.table_name} Data Loading Completed.") 
        else:
            tqdm.write(f'No data to load for {self.table_name} table.')

    
    def __validatePortada(self, df: pl.DataFrame) -> pl.DataFrame:
        query = """
            select * from Portada
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=query, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df.join(df_sql_server, on="IdPortada", how="semi")

        return df_result
