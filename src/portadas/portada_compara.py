import polars as pl
from utils.Model import Model
from tqdm import tqdm

class PortadaCompara(Model):
    def __init__(self):
        super().__init__(table_name="Compara", id_column="IdPortada")
    
    def extract_query(self) -> str:
        return """
            with CteGranosBasicos(interviewid,idgrano) AS(
                select interview__id, unnest(tipo_cul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2 
            )
            select e.interview__id, e.fecha_entr, ct.idgrano,e.areamaiz AreaCiclAnt, produccionmaiz produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 1 and (e.resultado = 1 or e.resultadost = 1)
            union all 
            select e.interview__id, e.fecha_entr, ct.idgrano,e.areafrijol AreaCiclAnt, produccionfrijol produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 2 and (e.resultado = 1 or e.resultadost = 1)
            union all 
            select e.interview__id, e.fecha_entr, ct.idgrano,e.areasorgo AreaCiclAnt, produccionsorgo produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 3 and (e.resultado = 1 or e.resultadost = 1)
            union all 
            select e.interview__id, e.fecha_entr, ct.idgrano,e.areaarroz AreaCiclAnt, produccionarroz produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 4 and (e.resultado = 1 or e.resultadost = 1)
        """

    def transform_mappings(self) -> dict:
        return {
            "interview__id": ("IdPortada", pl.Utf8),
            "fecha_entr": ("Fecha", pl.Datetime),
            "idgrano": ("IdGrano", pl.Int32),
            "areaciclant": ("AreaCicloAnt", pl.Float32),
            "produccionciclant": ("ProduccionCicloAnt", pl.Float32),
            "compareapc": ("Resultado", pl.Utf8)
        }
    
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transformed = super().transform(df)
        #validate nulls
        df_transformed = df_transformed.with_columns(
            pl.when(pl.col("AreaCicloAnt").is_null()).then(pl.lit(0)).otherwise(pl.col("AreaCicloAnt")).alias('AreaCicloAnt'),
            pl.when(pl.col("ProduccionCicloAnt").is_null()).then(pl.lit(0)).otherwise(pl.col("ProduccionCicloAnt")).alias('ProduccionCicloAnt')
        )
        return df_transformed
        
    #Override
    def load(self, df: pl.DataFrame):
        df_load = super().check_different_rows(df)
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
        """
            Validate if the IdPortada exists in the Portada table
            Args:
                df (pl.DataFrame): The data to be validated
            Returns:
                pl.DataFrame: The validated data
        """
        query = """
            select * from Portada
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=query, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df.join(df_sql_server, on="IdPortada", how="semi")
        return df_result