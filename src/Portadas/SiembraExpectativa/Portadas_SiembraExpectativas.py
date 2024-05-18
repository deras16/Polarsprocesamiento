import polars as pl
from utils.Model import Model

class PortadaSiembraExpectativas(Model):
    def __init__(self):
        super().__init__(table_name="SiembraExpectativa")
    
    def extract(self) -> pl.DataFrame:
        query = """
            --Nueva consulta para extraer expectativas de siembra      
                WITH ctesemilla(interviewid, idsemilla, idgrano) AS (
                    SELECT interviewid, array_agg(idsemilla), idgrano 
                    FROM (
                        SELECT er2.interview__id interviewid, unnest(er2.t_semillam) idsemilla, 1 idgrano 
                        FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAM" er2
                        UNION ALL
                        SELECT er3.interview__id, unnest(er3.t_semillaf), 2 idgrano
                        FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAF" er3
                        UNION ALL
                        SELECT er4.interview__id, unnest(er4.t_semillas), 3 idgrano
                        FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAS" er4
                        UNION ALL
                        SELECT er5.interview__id, unnest(er5.t_semillaa), 4 idgrano
                        FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAA" er5 
                    ) AS t
                    GROUP BY interviewid, idgrano
                ), cteGrano(interviewid, idgrano) AS (
                    SELECT interview__id, unnest(expec) 
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2
                ), cteareaProd(interviewid, areaSiembra, produccion, idgrano) AS (
                    SELECT interview__id, array_agg(aream) areas, array_agg(produccionm) prod, 1 idgrano 
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAM" er
                    GROUP BY interview__id
                    UNION ALL
                    SELECT er6.interview__id, array_agg(er6.areaf), array_agg(er6.produccionf), 2 idgrano 
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAF" er6
                    GROUP BY er6.interview__id 
                    UNION ALL
                    SELECT er7.interview__id, array_agg(er7.areas), array_agg(er7.produccions), 3 idgrano
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAS" er7
                    GROUP BY er7.interview__id
                    UNION ALL
                    SELECT er8.interview__id, array_agg(er8.areaa), array_agg(er8.producciona), 4 idgrano  
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAA" er8
                    GROUP BY er8.interview__id 
                )	  	  
                SELECT row_num, llave, ctg.idgrano, e.num_expl_agricm, unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                    t.depto, t.munici, unnest(t.areaSiembra) AS area, unnest(t.produccion) AS produccion 
                FROM (
                    SELECT 
                        row_number() OVER () AS row_num,
                        er.interview__id llave,
                        er.epoca_siembram epoca,
                        ct.idsemilla semilla,
                        er.depto_explosm depto,
                        er.munic_explosm munici,
                        ctp.areasiembra areaSiembra,
                        ctp.produccion produccion
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RMAIZ" er
                    INNER JOIN ctesemilla ct ON ct.interviewid = er.interview__id AND ct.idgrano = 1
                    INNER JOIN cteareaProd ctp ON ctp.interviewid = er.interview__id AND ctp.idgrano = 1
                ) AS t 
                INNER JOIN cteGrano ctg ON ctg.interviewid = t.llave
                INNER JOIN "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e ON e.interview__id = t.llave
                WHERE ctg.idgrano = 1 AND (e.resultado = 1 OR e.resultadost = 1)
                UNION ALL
                -- FRIJOL
                SELECT t.row_num, t.llave, ctg.idgrano, e.num_expl_agricf, unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                    t.depto, t.munici, unnest(t.areaSiembra) AS area, unnest(t.produccion) AS produccion 
                FROM (
                    SELECT 
                        row_number() OVER () AS row_num,
                        er2.interview__id llave,
                        er2.epoca_siembraf epoca,
                        ct.idsemilla semilla,
                        er2.depto_explosf depto,
                        er2.munic_explosf munici,
                        ctp.areasiembra areaSiembra,
                        ctp.produccion produccion
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RFRIJOL" er2
                    INNER JOIN ctesemilla ct ON ct.interviewid = er2.interview__id AND ct.idgrano = 2 
                    INNER JOIN cteareaProd ctp ON ctp.interviewid = er2.interview__id AND ctp.idgrano = 2
                ) AS t 
                INNER JOIN cteGrano ctg ON ctg.interviewid = t.llave
                INNER JOIN "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e ON e.interview__id = t.llave
                WHERE ctg.idgrano = 2 AND (e.resultado = 1 OR e.resultadost = 1)
                UNION ALL
                -- SORGO
                SELECT t.row_num, t.llave, ctg.idgrano, e.num_expl_agrics, unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                    t.depto, t.munici, unnest(t.areaSiembra) AS area, unnest(t.produccion) AS produccion 
                FROM (
                    SELECT 
                        row_number() OVER () AS row_num,
                        er9.interview__id llave,
                        er9.epoca_siembras epoca,
                        ct.idsemilla semilla,
                        er9.depto_exploss depto,
                        er9.munic_exploss munici,
                        ctp.areasiembra areaSiembra,
                        ctp.produccion produccion
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSORGO" er9 
                    INNER JOIN ctesemilla ct ON ct.interviewid = er9.interview__id AND ct.idgrano = 3 
                    INNER JOIN cteareaProd ctp ON ctp.interviewid = er9.interview__id AND ctp.idgrano = 3
                ) AS t 
                INNER JOIN cteGrano ctg ON ctg.interviewid = t.llave
                INNER JOIN "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e ON e.interview__id = t.llave
                WHERE ctg.idgrano = 3 AND (e.resultado = 1 OR e.resultadost = 1)
                UNION ALL
                -- ARROZ 
                SELECT t.row_num, t.llave, ctg.idgrano, e.num_expl_agrica, unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                    t.depto, t.munici, unnest(t.areaSiembra) AS area, unnest(t.produccion) AS produccion 
                FROM (
                    SELECT 
                        row_number() OVER () AS row_num,
                        er9.interview__id llave,
                        er9.epoca_siembraa epoca,
                        ct.idsemilla semilla,
                        er9.depto_explosa depto,
                        er9.munic_explosa munici,
                        ctp.areasiembra areaSiembra,
                        ctp.produccion produccion
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RARROZ" er9 
                    INNER JOIN ctesemilla ct ON ct.interviewid = er9.interview__id AND ct.idgrano = 4 
                    INNER JOIN cteareaProd ctp ON ctp.interviewid = er9.interview__id AND ctp.idgrano = 4
                ) AS t 
                INNER JOIN cteGrano ctg ON ctg.interviewid = t.llave
                INNER JOIN "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e ON e.interview__id = t.llave
                WHERE ctg.idgrano = 4 AND (e.resultado = 1 OR e.resultadost = 1)
        """
        
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        #no select row_num
        df = df.drop("row_num")
        return df

    def transform(self) -> pl.DataFrame:
        df = self.extract()
        df = df.rename({"llave": "IdPortada", "idgrano": "IdGrano", "num_expl_agricm": "numexp",
                        "epoca": "IdEpoca", "semilla": "IdSemilla", "depto": "IdDeptoexp", "munici": "IdMuniexp", "area": "Area", 
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