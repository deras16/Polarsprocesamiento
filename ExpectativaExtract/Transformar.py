import polars as pl
import conn 
from Extract import *

class Transformar():

    def __init__(self):
        self.postgreConn = conn.postgreConn()

    def TransformProductor(self, df):
        #La columna tipo prod deberia ser entero pero tiene valores string ingresados en survey, revisar
        df = df.rename({'folio':'IdFolio','departamento':'IdDepartamento', 'municipio':'IdMunicipio'}
                       ).cast({'latitude':pl.Float32, 'altitude':pl.Float32, 'longitude':pl.Float32})
        return df
    def TransPortada(self, df):
        df = df.rename({'interview__id':'IdPortada', 'folio':'IdFolio','fecha_entr':'FechaEntrevista','altitude':'Altitud',
                        'latitude':'Latitud', 'longitude':'Longitud', 'paquete':'Recibepqtmag','semilla':'IdGrano',
                        'semilla_maiz':'Areapqtmag','depto_sede':'IdDeptoexplt', 'mun_sede':'IdMunicipioexp', 'resultado':'ResultadoEntrevista',
                        'otros_robros':'OtrosRubros','tipo_pro':'TipologiaProductor', 'dirigida':'EncRealizadaA'}
                #Revisar tipo de datos de columnas en base destino
                ).cast({'IdPortada':pl.String,'anio':pl.Int32,'Altitud':pl.Float32,'Latitud':pl.Float32,'Longitud':pl.Float32})
    
    def TSiembraExpectativa(self, df):
        df = df.

extrac = Extract()    
dfprodex = extrac.ExtProductor()
trans = Transformar()
dfprodt = trans.TransformProductor(dfprodex)
print(dfprodt)


        