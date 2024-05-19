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
from src.Productores.Productor import Productor 
from src.Portadas.Portada import Portada

def load_catalog_tables():
    Pais().load()
    Departamento().load()
    Municipio().load()
    Causa().load()
    INSPRACTMAQ().load()
    Grano().load()
    Epoca().load()
    Credito().load()
    IntervaloRemesa().load()
    Semilla().load()

def load_productores():
    Productor().load()

def load_portadas():
    Portada().load()
    Portada().loadCausas()
    Portada().loadCompara()
    Portada().loadFondosAgricolas()
    Portada().loadSiembraExpectativas()
    
def main():
    load_catalog_tables()
    load_productores()
    load_portadas()
