# This file is used to run the main functions of the project
from config.config_file import load_config, save_config
#Catalog Tables
from src.catalogTables.PaisRemesas import Pais
from src.catalogTables.Departamentos import Departamento
from src.catalogTables.Municipios import Municipio
from src.catalogTables.Causas import Causa
from src.catalogTables.INSPRACTMAQ import INSPRACTMAQ
from src.catalogTables.GranosBasicos import Grano
from src.catalogTables.Epocas import Epoca
from src.catalogTables.OrigenCredito import Credito
from src.catalogTables.IntervalosRemesas import IntervaloRemesa
from src.catalogTables.Semillas import Semilla

#Productores
from src.Productores.Productor import Productor 

#Portadas
from src.Portadas.Portada import Portada
from src.Portadas.CausaSiembras.Portadas_Causas import PortadaCausaSiembra
from src.Portadas.Compara.Portada_Compara import PortadaCompara
from src.Portadas.FondosAgricolas.Portadas_FondosAgriculas import PortadaFondosAgricolas
from src.Portadas.SiembraExpectativa.Portadas_SiembraExpectativas import PortadaSiembraExpectativas
from src.Portadas.INSPRACTMACSiembra.Portada_Inspracmaq import PortadaInspracmaq

def load_catalog_tables():
    config = load_config()
    if config.getboolean('CONFIG_VARIABLES', 'CATALOG_TABLES_LOADED'):
        print('Catalog tables already loaded, Skipping...')
        return
  
    Departamento().run()
    Municipio().run()
    Pais().run()
    
    Causa().run()
    INSPRACTMAQ().run()
    
    IntervaloRemesa().run()
    Credito().run()
    
    Epoca().run()
    Semilla().run()
    Grano().run()
    
    config.set('CONFIG_VARIABLES', 'CATALOG_TABLES_LOADED', 'True')
    save_config(config)

def load_productores():
    Productor().run()

def load_portadas():
    Portada().run()
    PortadaCausaSiembra().run()
    PortadaCompara().run()
    PortadaFondosAgricolas().run()
    PortadaSiembraExpectativas().run()
    PortadaInspracmaq().run()

def main():
    load_catalog_tables()
    load_productores()
    load_portadas()
