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
            e.paquete, e.depto_sede,e.mun_sede, e.resultado, e.otros_robros, e.tipo_pro, e.dirigida 
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            where e.resultado = 1
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
                        "paquete": "Recibepqtmag", "depto_sede": "IdDeptoexplt", "mun_sede": "IdMunicipioexp", "resultado": "ResultadoEntrevista", 
                        "otros_robros": "OtrosRubros", "tipo_pro": "TipologiaProductor", "dirigida": "EncRealizadaA"})

        
        df = df.with_columns(df['IdPortada'].cast(pl.Utf8), df['Anio'].cast(pl.Int32), df['IdFolio'].cast(pl.Int32), 
                             df['FechaEntrevista'].cast(pl.Datetime), df['Altitud'].cast(pl.Float32), df['Latitud'].cast(pl.Float32), 
                             df['Longitud'].cast(pl.Float32), df['Precision'].cast(pl.Float32), df['Recibepqtmag'].cast(pl.Boolean), 
                             df['IdDeptoexplt'].cast(pl.Int32), df['IdMunicipioexp'].cast(pl.Int32), df['ResultadoEntrevista'].cast(pl.Int32), 
                             df['OtrosRubros'].cast(pl.List(pl.Utf8)), df['TipologiaProductor'].cast(pl.Utf8), df['EncRealizadaA'].cast(pl.Utf8))
        
        #validate data
        df = df.filter(pl.col("IdDeptoexplt").is_not_null())
        df = df.filter(pl.col("IdMunicipioexp").is_not_null())

        #convert the list to string TODO: check if this is the correct way to do it
        df = df.with_columns(
            pl.format("[{}]",
                pl.col("OtrosRubros").cast(pl.List(pl.Utf8)).list.join(", ")).alias('OtrosRubros'),
        )

        df = df.with_columns(
            pl.when(pl.col("OtrosRubros").is_null()).then(pl.lit("No Definido")).otherwise(pl.col("OtrosRubros")).alias('OtrosRubros'),
            pl.when(pl.col("TipologiaProductor") == '1').then(pl.lit("Productor Comercial"))
                .otherwise(
                    pl.when(pl.col("TipologiaProductor") == '2').then(pl.lit("Productor de Subsistencia"))
                    .otherwise(pl.lit("No Definido"))
            ).alias("TipologiaProductor")
        )
        return df

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Portada
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdPortada", how="left")
    
        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Anio_right").is_null()).select(pl.col("IdPortada"), pl.col("Anio"), pl.col("IdFolio"), 
                                                                      pl.col("FechaEntrevista"), pl.col("Altitud"), pl.col("Latitud"), 
                                                                      pl.col("Longitud"), pl.col("Precision"), pl.col("Recibepqtmag"), 
                                                                      pl.col("IdDeptoexplt"), pl.col("IdMunicipioexp"), pl.col("ResultadoEntrevista"), 
                                                                      pl.col("OtrosRubros"), pl.col("TipologiaProductor"), pl.col("EncRealizadaA"))

        return df_diff

    #belongsToMany relationships
    def loadCausas(self):
        Portadas_Causas.PortadaCausaSiembra().load()
    def loadCompara(self):
        Portada_Compara.PortadaCompara().load()
    def loadFondosAgricolas(self):
        Portadas_FondosAgriculas.PortadaFondosAgricolas().load()
    def loadSiembraExpectativas(self):
        Portadas_SiembraExpectativas.PortadaSiembraExpectativas().load() 
 