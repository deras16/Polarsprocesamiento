import polars as pl
import conn 

class Extract():


    def __init__(self):
        self.postgreConn = conn.postgreConn()
        self.mssqlConn = conn.MssqlConn()

    def Extrae_Tblcatalogos_Survey(self):
        queries = {
            'Departamento':"""select r.value IdDepto, r.text from ws_dea.reusablecategoricaloptions r where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and 
                            r.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'""",
            'Municipio':"""select r.value IdMunicipio, r.text Municipio, r.parentvalue IdDepto from ws_dea.reusablecategoricaloptions r 
                       where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'""",
            'Causas':"""select r.value IdCausa, r.text Causas, 'Revisar' TipoCausa from ws_dea.reusablecategoricaloptions r 
                        where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'b6a40e0c-4b1e-48fe-8313-6b3c35b35925'""",
            'Paises':"""select r.value IdPais, r.text Pais from ws_dea.reusablecategoricaloptions r 
                        where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '84136944-2924-7c08-11ae-491e13f348b6'""",
            'Insumos':"""select value as IdInsPracMaq, text as InsPracMaq, 'Insumo' Tipo from ws_dea.reusablecategoricaloptions r 
                        where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'a117f4fa-973c-417c-9730-a67794f7a732'""",
            'Maquinaria':"""select value as IdInsPracMaq, text as InsPracMaq, 'Revisar' Tipo from ws_dea.reusablecategoricaloptions r 
                        where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '71f41838-6868-4d2f-ad31-c830032893b4'""",
            'Practicas': """select value as IdInsPracMaq, text as InsPracMaq, 'Practica' Tipo from ws_dea.reusablecategoricaloptions r 
                        where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '573c5634-9e89-4d6c-b9df-b8ee6f59c93a'""",
            'Semillas': """select value as Idsemilla, text as Semilla from ws_dea.reusablecategoricaloptions r where r.categoriesid = 'f3c93701-b8e5-4d86-b7bc-e1836fe9fdb3'"""
        }
        results = {}

        for table_name, consulta in queries.items():
            df = pl.DataFrame(pl.read_database_uri(query=consulta, uri=self.postgreConn, engine='connectorx'))
            results[table_name] = df
        return results

    def ExtProductor(self):
        query = """
                select folio, departamento, municipio, tipo_prod, geo_est ->> 'Altitude' as Altitude, 
                geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                """
        dfProd = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfProd

    def ExtPortada(self):
        query = """ 
                select interview__id, extract(YEAR from fecha_entr ) anio, e.folio,e.fecha_entr, geo_est ->> 'Altitude' as Altitude, 
                geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude,e.paquete, e.semilla,e.semilla_maiz, e.semilla_frijol,
                e.depto_sede,e.mun_sede, e.resultado, e.otros_robros, e.tipo_pro, e.dirigida 
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                """
        dfPortada = pl.DataFrame(pl.read_database_uri(uri=self.postgreConn, query=query, engine='connectorx'))
        return dfPortada

    def ExtSiembraExpectativa(self):
        query = """
                with CteGranosBasicos(interviewid,idgrano) AS(
	                select interview__id, unnest(expec) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2 
                )
                select e.interview__id, ct.idgrano,num_expl_agricm, er.depto_explosm, er.munic_explosm, unnest(er.epoca_siembram) EPOCA,
                unnest(er3.t_semillam) IdSemilla, er2.aream, er2.produccionm  
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RMAIZ" er on e.interview__id = er.interview__id
                inner join CteGranosBasicos ct on ct.interviewid = e.interview__id 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAM" er2 on er2.interview__id = e.interview__id
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAM" er3 on er3.interview__id  = e.interview__id 
                where ct.idgrano = 1
                union all 
                select e.interview__id, ct.idgrano,num_expl_agricf,er4.depto_explosf,er4.munic_explosf, unnest(er4.epoca_siembraf) EPOCA,
                unnest(er5.t_semillaf) IdSemilla, er6.areaf, er6.produccionf 
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RFRIJOL" er4  on e.interview__id = er4.interview__id
                inner join CteGranosBasicos ct on ct.interviewid = e.interview__id 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAF" er6 on er6.interview__id = e.interview__id
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAF" er5 on er5.interview__id = e.interview__id 
                where ct.idgrano = 2
                union all 
                select e.interview__id, ct.idgrano,num_expl_agrics, er7.depto_exploss, er7.munic_exploss, unnest(er7.epoca_siembras) EPOCA,
                unnest(er8.t_semillas) IdSemilla, er9.areas, er9.produccions 
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSORGO" er7  on e.interview__id = er7.interview__id
                inner join CteGranosBasicos ct on ct.interviewid = e.interview__id 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAS" er9 on er9.interview__id = e.interview__id
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAS" er8  on er8.interview__id  = e.interview__id 
                where ct.idgrano = 3
                union all 
                select e.interview__id, ct.idgrano,num_expl_agrica, er10.depto_explosa, er10.munic_explosa,CASE
                when epoca = 1 then 4
                when epoca = 2 then 5
                else 6 end as EpocaSiembra,
                unnest(er12.t_semillaa) IdSemilla, er11.areaa, er11.producciona 
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RARROZ" er10 on e.interview__id = er10.interview__id
                inner join CteGranosBasicos ct on ct.interviewid = e.interview__id 
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAA" er11 on er11.interview__id = e.interview__id
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAA" er12 on er12.interview__id  = e.interview__id 
                cross join unnest(er10.epoca_siembraa) as epoca
                where ct.idgrano = 4
                """
        dfSiembraExpec = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfSiembraExpec

    def ExtEpoca():
        epoca = pl.DataFrame(
            {'IdEpoca':[1,2,3,4,5,6],'Epoca':['Invierno','Postrera','Apante','Secano','Primera Distrito de Riego','Segunda Distrito de Riego']}
        )
        return epoca
    
    def ExtCompara(self):
        query = """ 
                    with CteGranosBasicos(interviewid,idgrano) AS(
                        select interview__id, unnest(tipo_cul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2 
                    )

                    select e.interview__id, e.fecha_entr, ct.idgrano,e.areamaiz AreaCiclAnt, produccionmaiz produccionciclAnt, 
                    compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
                    where ct.idgrano = 1
                    union all 
                    select e.interview__id, e.fecha_entr, ct.idgrano,e.areafrijol AreaCiclAnt, produccionfrijol produccionciclAnt, 
                    compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
                    where ct.idgrano = 2
                    union all 
                    select e.interview__id, e.fecha_entr, ct.idgrano,e.areasorgo AreaCiclAnt, produccionsorgo produccionciclAnt, 
                    compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
                    where ct.idgrano = 3
                    union all 
                    select e.interview__id, e.fecha_entr, ct.idgrano,e.areaarroz AreaCiclAnt, produccionarroz produccionciclAnt, 
                    compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
                    where ct.idgrano = 4
                """
        dfcompara = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfcompara

    def ExtFondosAgricolas(self):
        query = """ 
                    select e.interview__id, e.credito, unnest(e.lcredito), e.inversionagricolaactual,e.remesa, e.porc_rem, e.pais  
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                """
        dfFondos = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfFondos

    def ExtCausasSiembra(self):
        query = """ 
                    select interview__id, unnest(resul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    order by fecha_entr 
                """
        dfCausasSiembra = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfCausasSiembra
    
    def INSPRACTMAQ(self):
        query = """
                    select value as IdInsPracMaq, text as InsPracMaq, 'Practica' as Tipo from ws_dea.reusablecategoricaloptions r 
                    where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '573c5634-9e89-4d6c-b9df-b8ee6f59c93a'
                    union all select value as IdInsPracMaq, text as InsPracMaq, 'Insumo' as Tipo from ws_dea.reusablecategoricaloptions r 
                    where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'a117f4fa-973c-417c-9730-a67794f7a732'
                    union all select value as IdInsPracMaq, text as InsPracMaq, 'Revisar' as Tipo 
                    from ws_dea.reusablecategoricaloptions r where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '71f41838-6868-4d2f-ad31-c830032893b4'
                """
        dfINSPRACTMAQ = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfINSPRACTMAQ

    def ExtGranoBasico():
        gb = pl.DataFrame(
            {'IdGrano': [1,2,3,4], 'Grano':['Maiz','Frijol','Sorgo','Arroz']}
        )
        return gb
    
    def ExtSemilla():
        semilla = pl.DataFrame(
            {'IdSemilla':[101,102,103],'Semilla':['Semilla Nacional', 'Semilla Hibrida','Semilla Segregada']}

        )
        return semilla
    
    def ExtEpoca():
        epoca = pl.DataFrame(
            {'IdEpoca':[1,2,3,4,5,6],'Epoca':['Invierno','Postrera','Apante','Secano','Primera Distrito de Riego','Segunda Distrito de Riego']}
        )
        return epoca
    
    def ExtIntervaloRemesa():
        intervalo = pl.DataFrame(
            {'IdIntervalo':[1,2,3,4,5],'Intervalo':['50-250','251-500','501-750','751-1000','1001-o mas']}
        )
        return intervalo
    
    def ExtOrigenCredito():
        OrgCredito = pl.DataFrame(
            {'IdOrigenCredito':[1,2,3,4,5,6,7,8,9],'NombOrigen':['Banca privada','Banco de Fomento Agropecuario','Banco Hipotecario',
                                                                 'Bandesal','Cooperativa',"ONG'S",'Otras financieras',
                                                                 'Prestamista local (Usurero)','Otro']}
        )
        return OrgCredito
