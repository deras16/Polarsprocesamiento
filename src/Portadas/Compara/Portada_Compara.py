import polars as pl
from utils.Model import Model

class PortadaCompara(Model):
    def __init__(self):
        super().__init__(table_name="Compara")
    
    def extract(self) -> pl.DataFrame:
        query = """
            with CteGranosBasicos(interviewid,idgrano) AS(
                select interview__id, unnest(tipo_cul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2 
            )
            select e.interview__id, extract(YEAR from e.fecha_entr ) anio, ct.idgrano,e.areamaiz AreaCiclAnt, produccionmaiz produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 1 and e.resultado = 1
            union all 
            select e.interview__id, extract(YEAR from e.fecha_entr ) anio, ct.idgrano,e.areafrijol AreaCiclAnt, produccionfrijol produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 2 and e.resultado = 1
            union all 
            select e.interview__id, extract(YEAR from e.fecha_entr ) anio, ct.idgrano,e.areasorgo AreaCiclAnt, produccionsorgo produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 3 and e.resultado = 1
            union all 
            select e.interview__id, extract(YEAR from e.fecha_entr ) anio, ct.idgrano,e.areaarroz AreaCiclAnt, produccionarroz produccionciclAnt, 
            compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
            where ct.idgrano = 4 and e.resultado = 1
        """
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        return df

    def transform(self) -> pl.DataFrame:
        df = self.extract()
        df = df.rename({"interview__id": "IdPortada", "anio": "AnioCicloAnt", "idgrano": "IdGrano", 
                        "areaciclant": "AreaCicloAnt", "produccionciclant": "ProduccionCicloAnt", "compareapc": "Resultado"})
        df = df.with_columns(df['IdPortada'].cast(pl.Utf8), df['AnioCicloAnt'].cast(pl.Int32), df['IdGrano'].cast(pl.Int32), 
                             df['AreaCicloAnt'].cast(pl.Float32), df['ProduccionCicloAnt'].cast(pl.Float32), df['Resultado'].cast(pl.Utf8))
        
        #validate nulls
        df = df.with_columns(
            pl.when(pl.col("AreaCicloAnt").is_null()).then(pl.lit(0)).otherwise(pl.col("AreaCicloAnt")).alias('AreaCicloAnt'),
            pl.when(pl.col("ProduccionCicloAnt").is_null()).then(pl.lit(0)).otherwise(pl.col("ProduccionCicloAnt")).alias('ProduccionCicloAnt')
        )
        return df
        

    def load(self) -> pl.DataFrame:
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Compara Data loaded')
        else:
            print('No data to load')
    
    def __validateData(self, df_transform: pl.DataFrame) -> pl.DataFrame:
        querySQLServer = """
            select * from Compara
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))

        df_result = df_transform.join(df_sql_server, on="IdPortada", how="semi")

        df_filter = df_transform.filter(~df_transform["IdPortada"].is_in(df_result['IdPortada']))

        df = self.__validatePortada(df_filter)
        return df
    
    def __validatePortada(self, df: pl.DataFrame) -> pl.DataFrame:
        query = """
            select * from Portada
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=query, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df.join(df_sql_server, on="IdPortada", how="semi")
        return df_result