import polars as pl
import config.connections as conn
from src.PaisRemesas import Pais


postgres_connection = conn.postgresql_connection()
mssql_connection = conn.msserver_connection()

def get_departamentos():
    query = """
       select r.value IdDepartamento,r."text" departamento  from ws_dea.reusablecategoricaloptions r 
       where categoriesid  = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf'
    """
    df = pl.DataFrame(pl.read_database_uri(query=query, uri=postgres_connection, engine='connectorx'))

    df_filter = df.unique(subset=["departamento"]).sort("iddepartamento")

    print(df_filter)    

def get_municipios():
    query = """
       select r.value IdMunicipio,r."text" municipio  from ws_dea.reusablecategoricaloptions r 
       where categoriesid  = 'c0eac36c-1598-dd17-53ed-9fb351d194dd' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf' 
    """
    df = pl.DataFrame(pl.read_database_uri(query=query, uri=postgres_connection, engine='connectorx'))

    #TODO remove duplicates but is removing 6 record of 262 unique text values
    #df_filter = df.unique(subset=["municipio"]).sort("idmunicipio")

    print(df)

def get_cantones():
    query = """
       select r.value IdCanton, r."text" canton  from ws_dea.reusablecategoricaloptions r 
       where categoriesid  = '4bc2d4f4-b202-5279-b1b4-b1758e11033c' and  questionnaireid ='f3be9695-9847-4dfc-9f7d-b64790b029cf'
    """
    df = pl.DataFrame(pl.read_database_uri(query=query, uri=postgres_connection, engine='connectorx'))
    
    #remove duplicates text
    #df_filter = df.unique(subset=["canton"]).sort("idcanton")
    
    print(df)


if __name__ == '__main__' :   
    #Pais.Pais().load()
    pass