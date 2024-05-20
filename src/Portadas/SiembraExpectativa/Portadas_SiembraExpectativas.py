import polars as pl
from utils.Model import Model

class PortadaSiembraExpectativas(Model):
    def __init__(self):
        super().__init__(table_name="SiembraExpectativa")
    
    def extract(self) -> pl.DataFrame:
        query = """
            with cteGrano(interviewid, idgrano) as(
                select e.interview__id, unnest(e.expec) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
            )
            select er.interview__id, ct.idgrano, e2.num_expl_agricm, er2.depto_explosm iddepto, er2.munic_explosm idmuni, 
            er.roster__vector[2] idepoca, er.roster__vector[3] idsemilla, er.aream areaprod, er.produccionm produccion
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAM" er
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RMAIZ" er2 on er2.interview__id = er.interview__id 
            inner join cteGrano ct on ct.interviewid = er.interview__id
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2 on e2.interview__id = er.interview__id 
            where ct.idgrano = 1 and (e2.resultado = 1 or e2.resultadost = 1)
            union all 
            select er3.interview__id, ct.idgrano, e3.num_expl_agricf, er4.depto_explosf, er4.munic_explosf, er3.roster__vector[2] idepoca,
            er3.roster__vector[3] idsemilla, er3.areaf  areaprod, er3.produccionf produccion
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAF" er3 
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RFRIJOL" er4 on er4.interview__id = er3.interview__id 
            inner join cteGrano ct on ct.interviewid = er3.interview__id
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e3 on e3.interview__id = er3.interview__id 
            where ct.idgrano = 2 and (e3.resultado = 1 or e3.resultadost = 1)
            union all
            select er5.interview__id, ct.idgrano, e4.num_expl_agrics, er6.depto_exploss, er6.munic_exploss, er5.roster__vector[2] idepoca,
            er5.roster__vector[3] idsemilla, er5.areas areaprod, er5.produccions produccion
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAS" er5
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSORGO" er6 on er6.interview__id = er5.interview__id 
            inner join cteGrano ct on ct.interviewid = er5.interview__id
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e4 on e4.interview__id = er5.interview__id 
            where ct.idgrano = 3 and (e4.resultado = 1 or e4.resultadost = 1)
            union all
            select er7.interview__id,ct.idgrano, e5.num_expl_agrica, er8.depto_explosa, er8.munic_explosa, er7.roster__vector[2],
            er7.roster__vector[3], er7.areaa, er7.producciona
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAA" er7 
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RARROZ" er8 on er8.interview__id = er7.interview__id
            inner join ctegrano ct on ct.interviewid = er7.interview__id
            inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e5 on e5.interview__id = er7.interview__id 
            where ct.idgrano = 4 and (e5.resultado = 1 or e5.resultadost = 1)
        """
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        return df

    def transform(self) -> pl.DataFrame:
        df = self.extract()
        df = df.rename({"interview__id": "IdPortada", "idgrano": "IdGrano", "num_expl_agricm": "numexp",
                        "idepoca": "IdEpoca", "idsemilla": "IdSemilla", "iddepto": "IdDeptoexp", "idmuni": "IdMuniexp", "areaprod": "Area", 
                        "produccion": "Produccion"})
        df = df.with_columns(df['IdPortada'].cast(pl.Utf8), df['IdGrano'].cast(pl.Int32),
                            df['numexp'].cast(pl.Int32), df['IdEpoca'].cast(pl.Int32), df['IdSemilla'].cast(pl.Int32),
                            df['IdDeptoexp'].cast(pl.Int32), df['IdMuniexp'].cast(pl.Int32), df['Area'].cast(pl.Float32),
                            df['Produccion'].cast(pl.Float32))
        
        #validate data
        df = df.filter(pl.col("IdSemilla").is_not_null())
        #validate IdEpoca and IdSemilla
        df = df.with_columns(
            # Modify IdEpoca based on IdSemilla ranges
            pl.when((pl.col("IdSemilla") >= 101) & (pl.col("IdSemilla") <= 109)).then(
                pl.when(pl.col("IdEpoca").is_null() | (pl.col("IdEpoca") != 1)).then(pl.lit(1)).otherwise(pl.col("IdEpoca"))
            ).when((pl.col("IdSemilla") >= 201) & (pl.col("IdSemilla") <= 209)).then(
                pl.when(pl.col("IdEpoca").is_null() | (pl.col("IdEpoca") != 2)).then(pl.lit(2)).otherwise(pl.col("IdEpoca"))
            ).when((pl.col("IdSemilla") >= 301) & (pl.col("IdSemilla") <= 309)).then(
                pl.when(pl.col("IdEpoca").is_null() | (pl.col("IdEpoca") != 3)).then(pl.lit(3)).otherwise(pl.col("IdEpoca"))
            ).when((pl.col("IdSemilla") == 410) | (pl.col("IdSemilla") == 411)).then(
                pl.when(pl.col("IdEpoca").is_null() | (pl.col("IdEpoca") != 4)).then(pl.lit(4)).otherwise(pl.col("IdEpoca"))
            ).when((pl.col("IdSemilla") == 510) | (pl.col("IdSemilla") == 511)).then(
                pl.when(pl.col("IdEpoca").is_null() | (pl.col("IdEpoca") != 5)).then(pl.lit(5)).otherwise(pl.col("IdEpoca"))
            ).when((pl.col("IdSemilla") == 610) | (pl.col("IdSemilla") == 611)).then(
                pl.when(pl.col("IdEpoca").is_null() | (pl.col("IdEpoca") != 6)).then(pl.lit(6)).otherwise(pl.col("IdEpoca"))
            ).otherwise(pl.col("IdEpoca")).alias('IdEpoca'),

            # Modify IdSemilla values based on ranges
            pl.when(pl.col("IdSemilla") == 201).then(pl.lit(101))
                .when(pl.col("IdSemilla") == 202).then(pl.lit(102))
                .when(pl.col("IdSemilla") == 203).then(pl.lit(103))
                .when(pl.col("IdSemilla") == 204).then(pl.lit(104))
                .when(pl.col("IdSemilla") == 205).then(pl.lit(105))
                .when(pl.col("IdSemilla") == 206).then(pl.lit(106))
                .when(pl.col("IdSemilla") == 207).then(pl.lit(107))
                .when(pl.col("IdSemilla") == 208).then(pl.lit(108))
                .when(pl.col("IdSemilla") == 209).then(pl.lit(109))
                .when(pl.col("IdSemilla") == 301).then(pl.lit(101))
                .when(pl.col("IdSemilla") == 302).then(pl.lit(102))
                .when(pl.col("IdSemilla") == 303).then(pl.lit(103))
                .when(pl.col("IdSemilla") == 304).then(pl.lit(104))
                .when(pl.col("IdSemilla") == 305).then(pl.lit(105))
                .when(pl.col("IdSemilla") == 306).then(pl.lit(106))
                .when(pl.col("IdSemilla") == 307).then(pl.lit(107))
                .when(pl.col("IdSemilla") == 308).then(pl.lit(108))
                .when(pl.col("IdSemilla") == 309).then(pl.lit(109))
                .when(pl.col("IdSemilla") == 510).then(pl.lit(410))
                .when(pl.col("IdSemilla") == 511).then(pl.lit(411))
                .when(pl.col("IdSemilla") == 610).then(pl.lit(410))
                .when(pl.col("IdSemilla") == 611).then(pl.lit(411))
                .otherwise(pl.col("IdSemilla")).alias("IdSemilla")
        )
       
        return df


    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Portada Siembra Expectativa Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transformed) -> pl.DataFrame:
        query = f"""
            select * from {self.table_name}
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=query, uri=self.mssql_connection, engine='connectorx'))

        df_result = df_transformed.join(df_sql_server, on="IdPortada", how="semi")

        df_filter = df_transformed.filter(~df_transformed["IdPortada"].is_in(df_result['IdPortada']))

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