import polars as pl
from utils.Model import Model
import os
class Productor(Model):
    def __init__(self):
        super().__init__(table_name="Productores")

    
    def extract(self) -> pl.DataFrame:
        query_productores = """
            select folio, departamento, municipio,extract(YEAR from fecha_entr ) anio,tipo_prod, geo_est ->> 'Altitude' as Altitude, 
                geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                where e.resultado = 1
        """
        df = pl.DataFrame(pl.read_database_uri(query=query_productores, uri=self.postgres_connection, engine='connectorx'))
        df = df.sort("folio")
        return df

    def transform(self) -> pl.DataFrame:
        df_transformed = self.transfomation_validations(self.extract())
        return df_transformed
        
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Productores Data loaded')
        else:
            print('No data to load') 

    def __validateData(self, df_transform: pl.DataFrame) -> pl.DataFrame:
        querySQLServer = """
            select * from Productores
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        df_diff = df_transform.join(df_sql_server, on="IdFolio", how="left")

        df_diff = df_diff.filter(pl.col("IdDepto_right").is_null()).select(pl.col("IdFolio"), pl.col("IdDepto"), pl.col("IdMunicipio"), pl.col("Anio"),pl.col("TipologiaProd"), pl.col("Altitud"), pl.col("Latitud"), pl.col("Longitud"))

        return df_diff

    def transfomation_validations(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.rename({"folio":"IdFolio","departamento": "Departamento", "municipio": "Municipio", "anio": "Anio",
                        "tipo_prod": "TipologiaProd", "altitude": "Altitud", "latitude": "Latitud", "longitude": "Longitud"})
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

        df = df.with_columns(
            df['IdFolio'].cast(pl.Int32), df['IdDepto'].cast(pl.Int32), df['Anio'].cast(pl.Int32),
            df['IdMunicipio'].cast(pl.Int32), df['TipologiaProd'].cast(pl.Utf8), 
            df['Altitud'].cast(pl.Decimal(9,6)), df['Latitud'].cast(pl.Decimal(9,6)), 
            df['Longitud'].cast(pl.Decimal(9,6))
        )
        return df

