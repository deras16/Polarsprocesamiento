import polars as pl
from utils.Model import Model

class Portada(Model):
    def __init__(self):
        super().__init__(table_name="Portada" ,id_column="IdPortada")

    def extract_query(self) -> str:
        return """
            select interview__id, extract(YEAR from fecha_entr ) anio, e.folio,e.fecha_entr, geo_est ->> 'Altitude' as Altitude, 
            geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude, geo_est ->> 'Accuracy' as Precisiongps ,
            case 
                when e.paquete = 1 then 1
                when e.paquete = 2 then 0
                else null
            end as paquete
            ,e.departamento,e.municipio ,e.resultado, e.otros_robros, 
            case 
                when e.tipo_pro is null and e.tipo_prost is not null then e.tipo_prost
                when e.tipo_prost is null and e.tipo_pro is not null then e.tipo_pro
                when e.tipo_pro is not null and e.tipo_prost is not null and e.tipo_pro = e.tipo_prost then e.tipo_pro
                else 0
            end as TipologiaProd, 
            case
                when e.dirigida is null and e.dirigidast is not null then e.dirigidast
                when e.dirigidast is null and e.dirigida is not null then e.dirigida
                when e.dirigida is not null and e.dirigidast is not null and e.dirigida = e.dirigidast then e.dirigida
                else 0
            end as dirigida 
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            inner join ws_dea.interviewsummaries i on i.interviewid = e.interview__id 
            where e.resultado = 1 or resultadost = 1
        """
    def transform_mappings(self) -> dict:
        return {
            "interview__id": ("IdPortada", pl.Utf8),
            "anio": ("Anio", pl.Int32),
            "folio": ("IdFolio", pl.Int32),
            "fecha_entr": ("FechaEntrevista", pl.Datetime),
            "altitude": ("Altitud", pl.Float32),
            "latitude": ("Latitud", pl.Float32),
            "longitude": ("Longitud", pl.Float32),
            "precisiongps": ("Precision", pl.Float32),
            "paquete": ("Recibepqtmag", pl.Boolean),
            "departamento": ("Departamento", pl.Utf8),
            "municipio": ("Municipio", pl.Utf8),
            "resultado": ("ResultadoEntrevista", pl.Int32),
            "otros_robros": ("OtrosRubros", pl.List(pl.Utf8)),
            "tipologiaprod": ("TipologiaProductor", pl.Utf8),
            "dirigida": ("EncRealizadaA", pl.Utf8)
        }
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transformed = super().transform(df)
        df_transformed = self.__transformationValidations(df_transformed)
        return df_transformed 
  
    def __transformationValidations(self, df: pl.DataFrame) -> pl.DataFrame:
        
        df = df.with_columns(
            pl.format("[{}]",
                pl.col("OtrosRubros").cast(pl.List(pl.Utf8)).list.join(", ")).alias('OtrosRubros'),
        )

        df = df.with_columns(
            pl.when(pl.col("Departamento") == "CABA�AS").then(pl.lit("CABAÑAS")).otherwise(pl.col("Departamento")).alias("Departamento"),
            pl.when(pl.col("Municipio") == "MERCEDES UMA�A").then(pl.lit("MERCEDES UMAÑA")).otherwise(pl.col("Municipio")).alias("Municipio"),
            pl.when(pl.col("OtrosRubros").is_null()).then(pl.lit("No Definido")).otherwise(pl.col("OtrosRubros")).alias('OtrosRubros'),
            pl.when(pl.col("TipologiaProductor") == '1').then(pl.lit("Productor Comercial"))
                .otherwise(
                    pl.when(pl.col("TipologiaProductor") == '2').then(pl.lit("Productor de Subsistencia"))
                    .otherwise(pl.lit("No Definido"))
            ).alias("TipologiaProductor")
        )
        #replacing Departamento and Municipio with their respective ids
        query_departamentos = """
            select * from Departamento
        """
        query_municipios = """
            select * from Municipio
        """
        df_departamentos = pl.DataFrame(pl.read_database_uri(query=query_departamentos, uri=self.mssql_connection, engine='connectorx'))
        df_municipios = pl.DataFrame(pl.read_database_uri(query=query_municipios, uri=self.mssql_connection, engine='connectorx'))

        df = df.join(df_departamentos, left_on='Departamento', right_on='Departamento', how='left')
        df = df.join(df_municipios, left_on='Municipio', right_on='Municipio' , how='left').select(
            ["IdPortada", "Anio", "IdFolio", "FechaEntrevista", "Altitud", "Latitud", "Longitud", "Precision", 
             "Recibepqtmag", "IdDepto", "IdMunicipio", "ResultadoEntrevista", "OtrosRubros", "TipologiaProductor", "EncRealizadaA"])
        df = df.unique(subset=["IdPortada"])
        
        df = df.rename({"IdDepto": "IdDeptoexplt", "IdMunicipio": "IdMunicipioexp"})
        return df
 