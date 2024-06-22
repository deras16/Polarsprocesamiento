import polars as pl
from tqdm import tqdm
from utils.Model import Model

class PortadaSiembraExpectativa(Model):
    def __init__(self):
        super().__init__(table_name="SiembraExpectativa", id_column="IdPortada")
    
    def extract_query(self) -> str:
        return """
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
            union all 
            select e.interview__id, 1 idgrano, 1 numexplt,case
                when e.lugar_sede = 1 then r3.value
                when e.lugar_sede is null then r3.value
                else r.value end as iddepto,
            case 
                when e.lugar_sede = 1 then r4.value
                when e.lugar_sede is null then r4.value
                else r2.value 
            end as idmunu,7 idepoca,120 idsemilla, e.semilla_maiz area, 0 produccion 
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            left join ws_dea.reusablecategoricaloptions r on r.value = e.depto_sede and r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
            left join ws_dea.reusablecategoricaloptions r3 on r3.text = replace(e.departamento,'�','Ñ') and r3.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r3.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
            left join ws_dea.reusablecategoricaloptions r2 on r2.value = e.mun_sede and r2.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r2.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
            left join ws_dea.reusablecategoricaloptions r4 on r4.text = e.municipio and r4.parentvalue = r3.value and r4.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r4.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
            where e.semilla_maiz > 0 and (e.resultado = 1 or e.resultadost = 1)                    
            union all
            select e.interview__id, 2 idgrano, 1 numexplt,case 
                when e.lugar_sede = 1 then r5.value 
                when lugar_sede is null then r5.value  
                else r.value
            end as iddepto, case 
                when e.lugar_sede = 1 then r6.value 
                when lugar_sede is null then r6.value  
                else r2.value 
            end as idmuni, 7 idepoca,121 idsemilla, e.semilla_frijol area, 0 produccion 
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            left join ws_dea.reusablecategoricaloptions r on r.value = e.depto_sede and r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
            left join ws_dea.reusablecategoricaloptions r2 on r2.value = e.mun_sede and r2.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r2.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
            left join ws_dea.reusablecategoricaloptions r5 on r5.text = replace(e.departamento,'�','Ñ') and r5.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r5.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
            left join ws_dea.reusablecategoricaloptions r6 on r6.text = e.municipio and r6.parentvalue = r5.value and r6.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
            and r6.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
            where e.semilla_frijol > 0 and (e.resultado = 1 or e.resultadost = 1)
        """

    def transform_mappings(self) -> dict:
        return {
            "interview__id": ("IdPortada", pl.Utf8),
            "idgrano": ("IdGrano", pl.Int32),
            "num_expl_agricm": ("numexp", pl.Int32),
            "idepoca": ("IdEpoca", pl.Int32),
            "idsemilla": ("IdSemilla", pl.Int32),
            "iddepto": ("IdDeptoexp", pl.Int32),
            "idmuni": ("IdMuniexp", pl.Int32),
            "areaprod": ("Area", pl.Float32),
            "produccion": ("Produccion", pl.Float32)
        }
    #Override
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        df_transformed = super().transform(df)
        #validate data
        df_transformed  = df_transformed .filter(pl.col("IdSemilla").is_not_null())
        #validate IdEpoca and IdSemilla
        df_transformed = df_transformed .with_columns(
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
            # Semilla
            pl.when(pl.col("IdSemilla") == 201).then(pl.lit(101)) #semilla nacional a semilla nacional
                .when(pl.col("IdSemilla") == 202).then(pl.lit(102)) #semilla hibrida a semilla hibrida
                .when(pl.col("IdSemilla") == 203).then(pl.lit(103)) #semilla segregada a semilla segregada
                .when(pl.col("IdSemilla") == 301).then(pl.lit(101)) #semilla nacional a semilla nacional
                .when(pl.col("IdSemilla") == 302).then(pl.lit(102)) #semilla hibrida a semilla hibrida
                .when(pl.col("IdSemilla") == 303).then(pl.lit(103)) #semilla segregada a semilla segregada
            #Frijol
                .when(pl.col("IdSemilla") == 304).then(pl.lit(104)) #frijol rojo a frijol rojo
                .when(pl.col("IdSemilla") == 305).then(pl.lit(105)) #frijol blanco a frijol blanco
                .when(pl.col("IdSemilla") == 306).then(pl.lit(106)) #frijol negro a frijol negro
                .when(pl.col("IdSemilla") == 204).then(pl.lit(104)) #frijol rojo a frijol rojo
                .when(pl.col("IdSemilla") == 205).then(pl.lit(105)) #frijol blanco a frijol blanco
                .when(pl.col("IdSemilla") == 206).then(pl.lit(106)) #frijol negro a frijol negro
            #Sorgo
                .when(pl.col("IdSemilla") == 107).then(pl.lit(109)) #sorgo mejorada a semilla mejorada
                .when(pl.col("IdSemilla") == 108).then(pl.lit(101)) #sorgo nacional a semilla nacional
                .when(pl.col("IdSemilla") == 109).then(pl.lit(103)) #sorgo segregada a semilla segregada
                .when(pl.col("IdSemilla") == 207).then(pl.lit(109)) #sorgo mejorado a semilla mejorada
                .when(pl.col("IdSemilla") == 208).then(pl.lit(101)) #sorgo nacional a semilla nacional
                .when(pl.col("IdSemilla") == 209).then(pl.lit(103)) #sorgo segregado a semilla segregada
                .when(pl.col("IdSemilla") == 307).then(pl.lit(109)) #sorgo mejorado a semilla mejorada
                .when(pl.col("IdSemilla") == 308).then(pl.lit(101)) #sorgo nacional a semilla nacional
                .when(pl.col("IdSemilla") == 309).then(pl.lit(103)) #sorgo segregada a semilla segregada
            #Arroz
                .when(pl.col("IdSemilla") == 410).then(pl.lit(109)) #arroz mejorado a semilla mejorada
                .when(pl.col("IdSemilla") == 411).then(pl.lit(103)) #arroz segregado a semilla segregada
                .when(pl.col("IdSemilla") == 510).then(pl.lit(109)) #arroz mejorado a semilla mejorada
                .when(pl.col("IdSemilla") == 511).then(pl.lit(103)) #arroz segregado a semilla segregada
                .when(pl.col("IdSemilla") == 610).then(pl.lit(109)) #arroz mejorado a semilla mejorada
                .when(pl.col("IdSemilla") == 611).then(pl.lit(103)) #arroz segregado a semilla segregada
                .otherwise(pl.col("IdSemilla")).alias("IdSemilla")
        )
        return df_transformed 

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