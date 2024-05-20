import polars as pl
from utils.Model import Model

from src.Portadas.CausaSiembras import Portadas_Causas
from src.Portadas.Compara import Portada_Compara
from src.Portadas.FondosAgricolas import Portadas_FondosAgriculas
from src.Portadas.SiembraExpectativa import Portadas_SiembraExpectativas

class Portada(Model):
    def __init__(self):
        super().__init__(table_name="Portada")

    def extract(self) -> pl.DataFrame:
        query = """
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
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        return df

    def transform(self) -> pl.DataFrame:
        df = self.extract()
        df_transformed = self.__transormationValidations(df)
        return df_transformed 
    
    def load(self) -> pl.DataFrame:
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Portada Data loaded')
        else:
            print('No data to load')

    def __transormationValidations(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.rename({"interview__id": "IdPortada", "anio": "Anio", "folio": "IdFolio", "fecha_entr": "FechaEntrevista", 
                        "altitude": "Altitud", "latitude": "Latitud", "longitude": "Longitud", "precisiongps": "Precision", 
                        "paquete": "Recibepqtmag", "departamento": "Departamento", "municipio": "Municipio", "resultado": "ResultadoEntrevista", 
                        "otros_robros": "OtrosRubros", "tipologiaprod": "TipologiaProductor", "dirigida": "EncRealizadaA"})

        
        df = df.with_columns(df['IdPortada'].cast(pl.Utf8), df['Anio'].cast(pl.Int32), df['IdFolio'].cast(pl.Int32), 
                             df['FechaEntrevista'].cast(pl.Datetime), df['Altitud'].cast(pl.Float32), df['Latitud'].cast(pl.Float32), 
                             df['Longitud'].cast(pl.Float32), df['Precision'].cast(pl.Float32), df['Recibepqtmag'].cast(pl.Boolean), 
                             df['Departamento'].cast(pl.Utf8), df['Municipio'].cast(pl.Utf8), df['ResultadoEntrevista'].cast(pl.Int32), 
                             df['OtrosRubros'].cast(pl.List(pl.Utf8)), df['TipologiaProductor'].cast(pl.Utf8), df['EncRealizadaA'].cast(pl.Utf8))
        

        #convert the list to string TODO: check if this is the correct way to do it
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

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Portada
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df_transform.join(df_sql_server, on="IdPortada", how="semi")

        #delete existing rows on df
        df_filter = df_transform.filter(~df_transform["IdPortada"].is_in(df_result['IdPortada']))

        return df_filter

    #belongsToMany relationships
    def loadCausas(self):
        Portadas_Causas.PortadaCausaSiembra().load()
    def loadCompara(self):
        Portada_Compara.PortadaCompara().load()
    def loadFondosAgricolas(self):
        Portadas_FondosAgriculas.PortadaFondosAgricolas().load()
    def loadSiembraExpectativas(self):
        Portadas_SiembraExpectativas.PortadaSiembraExpectativas().load() 
 