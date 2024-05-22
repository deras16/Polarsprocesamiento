import polars as pl
from utils.Model import Model
import os
class Productor(Model):
    def __init__(self):
        super().__init__(table_name="Productores" , id_column="IdFolio")

    def extract_query(self) -> str:
        return """
            select folio, departamento, municipio,extract(YEAR from fecha_entr ) anio,tipo_prod, geo_est ->> 'Altitude' as Altitude, 
            geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            where e.resultado = 1 or e.resultadost = 1 order by folio
        """   
    
    def transform_mappings(self) -> dict:
        return {
            "folio": ("IdFolio", pl.Int32),
            "departamento": ("Departamento", pl.Utf8),
            "municipio": ("Municipio", pl.Utf8),
            "anio": ("Anio", pl.Int32),
            "tipo_prod": ("TipologiaProd", pl.Utf8),
            "altitude": ("Altitud", pl.Float32),
            "latitude": ("Latitud", pl.Float32),
            "longitude": ("Longitud", pl.Float32)
        }
    
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transform = super().transform(df)
        df_transform = self.transfomation_validations(df_transform)
        return df_transform
        

    def transfomation_validations(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.when(pl.col("Departamento") == "CABA�AS").then(pl.lit("CABAÑAS")).otherwise(pl.col("Departamento")).alias("Departamento"),
            pl.when(pl.col("Municipio") == "MERCEDES UMA�A").then(pl.lit("MERCEDES UMAÑA")).otherwise(pl.col("Municipio")).alias("Municipio"),
            pl.when(pl.col("TipologiaProd") == '1').then(pl.lit("Productor Comercial"))
                .otherwise(
                    pl.when(pl.col("TipologiaProd") == '2').then(pl.lit("Productor de Subsistencia"))
                    .otherwise(pl.lit("No Definido"))
            ).alias("TipologiaProd")
        )

        query_departamentos = """
            select * from Departamento
        """
        query_municipios = """
            select * from Municipio
        """
        df_departamentos = pl.DataFrame(pl.read_database_uri(query=query_departamentos, uri=self.mssql_connection, engine='connectorx'))
        df_municipios = pl.DataFrame(pl.read_database_uri(query=query_municipios, uri=self.mssql_connection, engine='connectorx'))

        df = df.join(df_departamentos,left_on='Departamento', right_on='Departamento', how='left')
        df = df.join(df_municipios, left_on='Municipio', right_on='Municipio' , how='left').select(["IdFolio", "IdDepto", "IdMunicipio", "Anio", "TipologiaProd", "Altitud", "Latitud", "Longitud"])
        df = df.unique(subset=["IdFolio"])
        df = df.sort("IdFolio")
        return df

