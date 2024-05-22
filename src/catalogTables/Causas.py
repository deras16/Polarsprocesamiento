import polars as pl
from utils.Model import Model

class Causa(Model):
    def __init__(self):
        super().__init__(table_name="Causas", id_column="IdCausa")
    
    def extract_query(self) -> str:
        return """            
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
    
    def transform_mappings(self) -> dict:
        return {
            "idcausa": ("IdCausa", pl.Int32),
            "causas": ("Causa", pl.Utf8),
            "tipocausa": ("TipoCausa", pl.Utf8)
        }

    