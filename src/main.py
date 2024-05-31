# This file is used to run the main functions of the project
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
