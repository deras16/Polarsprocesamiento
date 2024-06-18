# This file is used to run the main functions of the project
from config.config_file import load_config, save_config
#Catalog Tables
from src.catalog_tables.pais_remesas import PaisRemesa
from src.catalog_tables.departamentos import Departamento
from src.catalog_tables.municipios import Municipio
from src.catalog_tables.causas import Causa
from src.catalog_tables.inspractmaq import INSPRACTMAQ
from src.catalog_tables.granos_basicos import GranoBasico
from src.catalog_tables.epocas import Epoca
from src.catalog_tables.origen_credito import OrigenCredito
from src.catalog_tables.intervalos_remesas import IntervaloRemesa
from src.catalog_tables.semillas import Semilla

#Productores
from src.productores.productores import Productor

#Portadas
from src.portadas.portadas import Portada
from src.portadas.portada_causa import PortadaCausa
from src.portadas.portada_compara import PortadaCompara
from src.portadas.portada_fondo_agricola import PortadaFondoAgricola
from src.portadas.portada_siembra_expectativa import PortadaSiembraExpectativa
from src.portadas.portada_inspracmaq import PortadaInspracmaq


def load_catalog_tables():
    """
        Run ETL process for catalog tables
    """
    config = load_config()
    if config.getboolean('CONFIG_VARIABLES', 'CATALOG_TABLES_LOADED'):
        print('Catalog tables already loaded, Skipping...')
        return
  
    Departamento().run()
    Municipio().run()
    PaisRemesa().run()
    
    Causa().run()
    INSPRACTMAQ().run()
    
    IntervaloRemesa().run()
    OrigenCredito().run()
    
    Epoca().run()
    Semilla().run()
    GranoBasico().run()
    
    config.set('CONFIG_VARIABLES', 'CATALOG_TABLES_LOADED', 'True')
    save_config(config)

def load_productores():
    """
        Run ETL process for productores
    """
    Productor().run()

def load_portadas():
    """
        Run ETL process for portadas
    """
    Portada().run()
    PortadaCausa().run()
    PortadaCompara().run()
    PortadaFondoAgricola().run()
    PortadaSiembraExpectativa().run()
    PortadaInspracmaq().run()

def main():
    """
        Run the main functions of the project
    """
    load_catalog_tables()
    load_productores()
    load_portadas()
