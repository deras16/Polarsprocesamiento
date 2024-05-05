USE ExpectativaSiembra
GO
CREATE TABLE [Departamento] (
  [IdDepto] integer PRIMARY KEY,
  [Departamento] nvarchar(255)
)
GO
CREATE TABLE [Municipio] (
  [IdMunicipio] integer PRIMARY KEY,
  [Municipio] nvarchar(255) not null,
  [IdDepto] integer not null
)
GO
CREATE TABLE [Semilla] (
	[Idsemilla] int PRIMARY KEY not null,	
	[Semilla] nvarchar(50)
)
GO
CREATE TABLE [GranosBasicos] (
	[IdGrano] int PRIMARY KEY not null,
	[Grano] nvarchar(50)
)
GO
CREATE TABLE [Epocas] (
	[IdEpoca] int PRIMARY KEY not null,
	[Epoca] nvarchar(50) 
)
GO
CREATE TABLE [Causas] (
	[IdCausa] int PRIMARY KEY not null, 
	[Causa] nvarchar(250) not null,
	[TipoCausa] nvarchar(100) not null
)
GO
CREATE TABLE PaisRemesas (
	[IdPais] int PRIMARY KEY not null,
	[Pais] nvarchar(100)
)
GO
CREATE TABLE Productores (
	[IdFolio] int not null,  
	[IdDepto] int not null, 
	[IdMunicipio] int not null,
	[Anio] int not null,
	[TipologiaProd] nvarchar(50) not null,
	[Altitud] decimal(9,6) null,
	[Latitud] decimal(9,6) null,
	[Longitud] decimal(9,6) null
)
GO
CREATE TABLE Portada (
	[IdPortada] varchar(50) PRIMARY KEY not null, 
	[Anio] int not null, 
	[IdFolio] int not null, 
	[FechaEntrevista] datetime null, 
	[Altitud] decimal(9,6) null,
	[Latitud] decimal(9,6) null,
	[Longitud] decimal(9,6) null,
	[Precision] decimal(9,6), 
	[Recibepqtmag] bit,
	[IdGrano] int,
	[Areapqtmag] decimal(6,2),
	[IdDeptoexplt] int not null,
	[IdMunicipioexp] int not null, 
	[ResultadoEntrevista] int not null,
	[OtrosRubros] varchar(500),
	[TipologiaProductor] int,
	[EncRealizadaA] nvarchar(100)  
)
GO
CREATE TABLE SiembraExpectativa(
	[Idportada] varchar(50) not null,
	[IdGrano] int not null, 
	[numexp] int not null,
	[IdDeptoexp] int not null,
	[IdMuniexp] int not null,
	[IdEpoca] int not null,
	[IdSemilla] int not null,
	[Area] decimal(6,2) not null,
	[Produccion] decimal(6,2)
)
GO
CREATE TABLE Compara(
	[IdPortada] varchar(50) not null,
	[AnioCicloAnt] int not null,
	[IdGrano] int not null,
	[AreaCicloAnt] decimal(6,2) not null,
	[ProduccionCicloAnt] decimal(6,2),
	[Resultado] nvarchar(100)
)
GO
CREATE TABLE CausaSiembra (
	[IdPortada] varchar(50) not null,
	[IdCausa] int not null 
)
GO
CREATE TABLE IntervalosRemesas (
	[IdIntervalo] int PRIMARY KEY not null,
	[Intervalo] nvarchar(50) not null
)
GO
CREATE TABLE OrigenCredito (
	[IdOrigenCredito] int PRIMARY KEY not null,
	[NombOrigen] nvarchar(100) not null
)
GO
CREATE TABLE FondosAgricolas (
	[IdPortada] varchar(50) not null,
	[SolicitoCredito] bit,
	[IdOrigenCredito] int,
	[InversionCredito] decimal(10,2),
	[RecibeRemesa] bit,
	[IdIntervalo] int not null,
	[IdPais] int not null
)
GO
CREATE TABLE INSPRACTMAQ (
	[IdInsPracMaq] int PRIMARY KEY not null,
	[InsPracMaq] nvarchar(500) not null,
	[Tipo] nvarchar(15)
)
GO
CREATE TABLE INSPRACTMAQSiembra (
	[IdPortada] varchar(50) not null,
	[IdInsPracMaq] int not null
)
GO
CREATE TABLE EncuestasContestadas (

	[Idportada] varchar(50) not null,

	[creado_el] datetime default getdate() not null,
)
GO
ALTER TABLE Productores
ADD CONSTRAINT FK_DeptoProductores
FOREIGN KEY(IdDepto) REFERENCES Departamento(IdDepto)

ALTER TABLE Productores
ADD CONSTRAINT Fk_MuniProductores
FOREIGN KEY(IdMunicipio) REFERENCES Municipio(IdMunicipio)

-- ALTER TABLE Portada
-- ADD CONSTRAINT FK_ProductPortada
-- FOREIGN KEY(IdFolio) REFERENCES Productores(IdFolio)  se comenta debido a que la tabla productores deja de tener una llave primaria

ALTER TABLE Portada
ADD CONSTRAINT FK_GranoPortada
FOREIGN KEY(IdGrano) REFERENCES GranosBasicos(IdGrano)

ALTER TABLE Portada
ADD CONSTRAINT FK_DeptoPortada
FOREIGN KEY(IdDeptoexplt) REFERENCES Departamento(IdDepto)

ALTER TABLE Portada 
ADD CONSTRAINT FK_MuniPortada
FOREIGN KEY(IdMunicipioexp) REFERENCES Municipio(IdMunicipio)

ALTER TABLE SiembraExpectativa
ADD CONSTRAINT FK_GranoSiembra
FOREIGN KEY(IdGrano) REFERENCES GranosBasicos(IdGrano)

ALTER TABLE SiembraExpectativa
ADD CONSTRAINT FK_DeptoSiembra
FOREIGN KEY(IdDeptoexp) REFERENCES Departamento(IdDepto)

ALTER TABLE SiembraExpectativa
ADD CONSTRAINT FK_MuniSiembra
FOREIGN KEY(IdMuniexp) REFERENCES Municipio(IdMunicipio)

ALTER TABLE SiembraExpectativa
ADD CONSTRAINT FK_EpocaSiembra
FOREIGN KEY(IdEpoca) REFERENCES Epocas(IdEpoca)

ALTER TABLE SiembraExpectativa
ADD CONSTRAINT FK_SemillaSiembra
FOREIGN KEY(IdSemilla) REFERENCES Semilla(IdSemilla)

ALTER TABLE SiembraExpectativa
ADD CONSTRAINT FK_SiembraPortada
FOREIGN KEY(IdPortada) REFERENCES Portada(IdPortada)

ALTER TABLE Compara
ADD CONSTRAINT FK_PortadaCompara
FOREIGN KEY(IdPortada) REFERENCES Portada(IdPortada)

ALTER TABLE Compara 
ADD CONSTRAINT FK_GranoCompara 
FOREIGN KEY(IdGrano) REFERENCES GranosBasicos(IdGrano)

ALTER TABLE Municipio
ADD CONSTRAINT FK_DeptoMunicipio
FOREIGN KEY(IdDepto) REFERENCES Departamento(IdDepto)

ALTER TABLE CausaSiembra
ADD CONSTRAINT FK_CausasCausaSiembra
FOREIGN KEY(IdCausa) REFERENCES Causas(IdCausa)

ALTER TABLE CausaSiembra
ADD CONSTRAINT FK_PortadaCausasSiembra
FOREIGN KEY(IdPortada) REFERENCES Portada(IdPortada)

ALTER TABLE FondosAgricolas
ADD CONSTRAINT FK_PortadaFondoAgricola
FOREIGN KEY(IdPortada) REFERENCES Portada(IdPortada)

ALTER TABLE FondosAgricolas
ADD CONSTRAINT FK_IntervaloRemeFondos
FOREIGN KEY(IdIntervalo) REFERENCES IntervalosRemesas(IdIntervalo)

ALTER TABLE FondosAgricolas
ADD CONSTRAINT FK_PaisRemeFondoAgricola
FOREIGN KEY(IdPais) REFERENCES PaisRemesas(IdPais)

ALTER TABLE FondosAgricolas
ADD CONSTRAINT FK_OrigenCredFondos
FOREIGN KEY(IdOrigenCredito) REFERENCES OrigenCredito(IdOrigenCredito)

ALTER TABLE INSPRACTMAQSiembra
ADD CONSTRAINT FK_INSPRACTMAQSiembra
FOREIGN KEY(IdInsPracMaq) REFERENCES INSPRACTMAQ(IdInsPracMaq)

ALTER TABLE INSPRACTMAQSiembra
ADD CONSTRAINT FK_PortadaINSPRACTMAQSiembra
FOREIGN KEY(IdPortada) REFERENCES Portada(IdPortada)
