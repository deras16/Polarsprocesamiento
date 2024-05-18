import polars as pl
from utils.Model import Model

class PortadaFondosAgricolas(Model):
    def __init__(self):
        super().__init__(table_name="FondosAgricolas")
    
    def extract(self) -> pl.DataFrame:
        query = """
            select e.interview__id, e.credito, unnest(e.lcredito), e.inversionagricolaactual,e.remesa, e.porc_rem, e.pais  
            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
            where e.resultado = 1 
        """
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        return df


    def transform(self) -> pl.DataFrame:
        df = self.extract()
        df = df.rename({"interview__id": "IdPortada", "credito": "SolicitoCredito", "unnest": "IdOrigenCredito", 
                        "inversionagricolaactual": "InversionCredito", "remesa": "RecibeRemesa", "porc_rem": "IdIntervalo", "pais": "IdPais"})
        
        #validate booleans
        df = df.with_columns(
            pl.when(pl.col("SolicitoCredito") == 1).then(pl.lit(True)).otherwise(
                pl.when(pl.col("SolicitoCredito") == 2).then(pl.lit(False)).otherwise(pl.lit(None))
            ).alias('SolicitoCredito'),
            
            pl.when(pl.col("RecibeRemesa") == 1).then(pl.lit(True)).otherwise(
                pl.when(pl.col("RecibeRemesa") == 2).then(pl.lit(False)).otherwise(pl.lit(None))
            ).alias('RecibeRemesa')
        )

        df = df.with_columns(df['IdPortada'].cast(pl.Utf8), df['SolicitoCredito'].cast(pl.Boolean), df['IdOrigenCredito'].cast(pl.Int32), 
                             df['InversionCredito'].cast(pl.Float32), df['RecibeRemesa'].cast(pl.Boolean), df['IdIntervalo'].cast(pl.Int32), 
                             df['IdPais'].cast(pl.Int32))
        
        return df
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Fondos Agricolas Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transformed: pl.DataFrame) -> pl.DataFrame:
        query = """
            select * from FondosAgricolas
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