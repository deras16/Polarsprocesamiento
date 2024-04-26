import polars as pl
from utils.Model import Model

class Causa(Model):
    def __init__(self):
        super().__init__(table_name="Causas")

    
    def extract(self) -> pl.DataFrame:
        query = """      
            select 
                r.value as IdCausa, 
                r.text as Causas, 
            case 
                when r.text = 'Precios altos de venta de la producción' then 'Mayor'
                when r.text = 'Precios bajos de los insumos agrícolas' then 'Mayor'
                when r.text = 'Precios bajos del alquiler de la tierra' then 'Mayor'
                when r.text = 'Mayor acceso a tierra' then 'Mayor'
                when r.text = 'Recibe paquete agrícola del MAG' then 'Mayor'
                when r.text = 'Accesibilidad de mano de obra' then 'Mayor'
                when r.text = 'Buen acceso a crédito' then 'Mayor'
                when r.text = 'Buenas expectativas de las condiciones climáticas' then 'Mayor'
                when r.text = 'Precios bajos de venta de la producción' then 'Menor'
                when r.text = 'Precios altos de los insumos agrícolas' then 'Menor'
                when r.text = 'Precios altos del alquiler de la tierra' then 'Menor'
                when r.text = 'Poco acceso a tierra' then 'Menor'
                when r.text = 'No recibe paquete agrícola del MAG' then 'Menor'
                when r.text = 'Mano de obra cara o escasa' then 'Menor'
                when r.text = 'Falta de acceso a crédito' then 'Menor'
                when r.text = 'Malas expectativas de las condiciones climáticas' then 'Menor'
                when r.text = 'Precios estables de venta de la producción' then 'Igual'
                when r.text = 'Precios estables de los insumos agrícolas' then 'Igual'
                when r.text = 'Precios estables del alquiler de la tierra' then 'Igual'
                when r.text = 'Mantiene la misma área para la siembra' then 'Igual'
                when r.text = 'Mantiene los mismos recursos para producir' then 'Igual'
                when r.text = 'Acceso a crédito' then 'Igual'
                when r.text = 'Similares expectativas de las condiciones climáticas' then 'Igual'
                when r.text = 'Acceso amano de obra estable' then 'Igual'
                else 'Otro' 
            end as TipoCausa 
            from ws_dea.reusablecategoricaloptions r 
            where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'b6a40e0c-4b1e-48fe-8313-6b3c35b35925'
        """
        df = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgres_connection, engine='connectorx'))
        df = df.sort("idcausa")
        return df
    

    def transform(self) -> pl.DataFrame:
        df_extract = self.extract()
        df_extract  = df_extract.with_columns(df_extract['idcausa'].cast(pl.Int32), df_extract['causas'].cast(pl.Utf8), df_extract['tipocausa'].cast(pl.Utf8))
        df_extract  = df_extract.rename({ "idcausa": "IdCausa", "causas": "Causa", "tipocausa": "TipoCausa" })
        
        return df_extract
    
    def load(self):
        df_load = self.__validateData(self.transform())
        if df_load.shape[0] > 0:
            df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
            print('Causa Data loaded')
        else:
            print('No data to load')

    def __validateData(self, df_transform) -> pl.DataFrame:
        querySQLServer = """
            select * from Causas
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return only different rows on equal dataframes
        df_diff = df_transform.join(df_sql_server, on="IdCausa", how="left")

        #select only rows that are different
        df_diff = df_diff.filter(pl.col("Causa_right").is_null()).select(pl.col("IdCausa"), pl.col("Causa"), pl.col("TipoCausa"))
        return df_diff
    