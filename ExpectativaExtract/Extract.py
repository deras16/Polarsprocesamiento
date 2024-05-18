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
                select folio, departamento, municipio,extract(YEAR from fecha_entr ) anio,tipo_prod, geo_est ->> 'Altitude' as Altitude, 
                geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude
                from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                where e.resultado = 1
                """
        dfProd = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfProd

    def ExtPortada(self):
        query = """ 
                    select interview__id, extract(YEAR from fecha_entr ) anio, e.folio,e.fecha_entr, geo_est ->> 'Altitude' as Altitude, 
                    geo_est ->> 'Latitude' as Latitude, geo_est ->> 'Longitude' as Longitude, geo_est ->> 'Accuracy' as Precisiongps ,
                    case 
                    	when e.paquete = 1 then 1
                    	when e.paquete = 2 then 0
                    	else null
                    end as paquete
                    ,e.departamento,e.municipio ,e.resultado, e.otros_robros, 
                    case 
                    	when e.tipo_pro is null and e.tipo_prost is not null then e.tipo_prost
                    	when e.tipo_prost is null and e.tipo_pro is not null then e.tipo_pro
                    	when e.tipo_pro is not null and e.tipo_prost is not null and e.tipo_pro = e.tipo_prost then e.tipo_pro
                    	else 0
                    end as TipologiaProd, 
                    case
                    	when e.dirigida is null and e.dirigidast is not null then e.dirigidast
                    	when e.dirigidast is null and e.dirigida is not null then e.dirigida
                    	when e.dirigida is not null and e.dirigidast is not null and e.dirigida = e.dirigidast then e.dirigida
                    	else 0
                    end as dirigida 
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
                    inner join ws_dea.interviewsummaries i on i.interviewid = e.interview__id 
                    where e.resultado = 1 or resultadost = 1
                """
        dfPortada = pl.DataFrame(pl.read_database_uri(uri=self.postgreConn, query=query, engine='connectorx'))
        return dfPortada

    def ExtSiembraExpectativa(self):
        query = """
                    --Nueva consulta para extraer expectativas de siembra      
                    with ctesemilla(interviewid, idsemilla, idgrano) as (
                        select interviewid, array_agg(idsemilla), idgrano from (
                            select er2.interview__id interviewid, unnest(er2.t_semillam) idsemilla, 1 idgrano 
                            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAM" er2
                            union all
                            select er3.interview__id, unnest(er3.t_semillaf), 2 idgrano
                            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAF" er3
                            union all
                            select er4.interview__id, unnest(er4.t_semillas), 3 idgrano
                            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAS" er4
                            union all
                            select er5.interview__id, unnest(er5.t_semillaa), 4 idgrano
                            from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_REPOCAA" er5 
                        ) as t
                        group by interviewid, idgrano
                    ), cteGrano(interviewid, idgrano) as(
                        select interview__id, unnest(expec) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2
                    ), cteareaProd(interviewid, areaSiembra, produccion, idgrano) as (
                        SELECT interview__id, array_agg(aream) areas, array_agg(produccionm) prod, 1 idgrano 
                        from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAM" er
                        group by interview__id
                        union all
                        select er6.interview__id, array_agg(er6.areaf), array_agg(er6.produccionf), 2 idgrano 
                        from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAF" er6
                        group by er6.interview__id 
                        union all
                        select er7.interview__id, array_agg(er7.areas), array_agg(er7.produccions), 3 idgrano
                        from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAS" er7
                        group by er7.interview__id
                        union all
                        select er8.interview__id, array_agg(er8.areaa), array_agg(er8.producciona), 4 idgrano  
                        from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAA" er8
                        group by er8.interview__id 
                    )	  	  
                    select row_num, llave, ctg.idgrano,e.num_expl_agricm,unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                    t.depto, t.munici,unnest(t.areaSiembra), unnest(t.produccion)  from(
                        SELECT 
                            row_number() OVER () AS row_num,
                            er.interview__id llave,
                            er.epoca_siembram epoca,
                            ct.idsemilla semilla,
                            er.depto_explosm depto,
                            er.munic_explosm  munici,
                            ctp.areasiembra areaSiembra,
                            ctp.produccion produccion
                        FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RMAIZ" er
                        inner join ctesemilla ct on ct.interviewid = er.interview__id and ct.idgrano = 1
                        inner join cteareaProd ctp on ctp.interviewid = er.interview__id and ctp.idgrano = 1
                    ) as t 
                    inner join cteGrano ctg on ctg.interviewid = t.llave
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e on e.interview__id = t.llave
                    where ctg.idgrano = 1 and (e.resultado = 1 or e.resultadost = 1)
                    union all --AQUI EMPIEZA FRIJOL
                    select t.row_num, t.llave, ctg.idgrano,e.num_expl_agricf ,unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                    t.depto, t.munici,unnest(t.areaSiembra), unnest(t.produccion)  from(
                        SELECT 
                            row_number() OVER () AS row_num,
                            er2.interview__id llave,
                            er2.epoca_siembraf  epoca,
                            ct.idsemilla semilla,
                            er2.depto_explosf depto,
                            er2.munic_explosf munici,
                            ctp.areasiembra areaSiembra,
                            ctp.produccion produccion
                        FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RFRIJOL" er2
                        inner join ctesemilla ct on ct.interviewid = er2.interview__id and ct.idgrano =2 
                        inner join cteareaProd ctp on ctp.interviewid = er2.interview__id and ctp.idgrano = 2
                    ) as t 
                    inner join cteGrano ctg on ctg.interviewid = t.llave
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e on e.interview__id = t.llave
                    where ctg.idgrano = 2 and (e.resultado = 1 or e.resultadost = 1)
                    union all--SORGO
                    select t.row_num, t.llave, ctg.idgrano,e.num_expl_agrics ,unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                    t.depto, t.munici,unnest(t.areaSiembra), unnest(t.produccion)  from(
                    SELECT 
                        row_number() OVER () AS row_num,
                        er9.interview__id llave,
                        er9.epoca_siembras epoca,
                        ct.idsemilla semilla,
                        er9.depto_exploss depto,
                        er9.munic_exploss munici,
                        ctp.areasiembra areaSiembra,
                        ctp.produccion produccion
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSORGO" er9 
                    inner join ctesemilla ct on ct.interviewid = er9.interview__id and ct.idgrano = 3 
                    inner join cteareaProd ctp on ctp.interviewid = er9.interview__id and ctp.idgrano = 3
                ) as t 
                inner join cteGrano ctg on ctg.interviewid = t.llave
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e on e.interview__id = t.llave
                where ctg.idgrano = 3 and (e.resultado = 1 or e.resultadost = 1)
                union all--Arroz 
                select t.row_num, t.llave, ctg.idgrano,e.num_expl_agrica ,unnest(t.epoca) epoca, unnest(t.semilla) semilla,
                t.depto, t.munici,unnest(t.areaSiembra), unnest(t.produccion)  from(
                    SELECT 
                        row_number() OVER () AS row_num,
                        er9.interview__id  llave,
                        er9.epoca_siembraa  epoca,
                        ct.idsemilla semilla,
                        er9.depto_explosa  depto,
                        er9.munic_explosa  munici,
                        ctp.areasiembra areaSiembra,
                        ctp.produccion produccion
                    FROM "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RARROZ" er9 
                    inner join ctesemilla ct on ct.interviewid = er9.interview__id and ct.idgrano = 4 
                    inner join cteareaProd ctp on ctp.interviewid = er9.interview__id and ctp.idgrano = 4
                ) as t 
                inner join cteGrano ctg on ctg.interviewid = t.llave
                inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e on e.interview__id = t.llave
                where ctg.idgrano = 4 and (e.resultado = 1 or e.resultadost = 1)
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
                    where ct.idgrano = 1 and e.resultado = 1
                    union all 
                    select e.interview__id, e.fecha_entr, ct.idgrano,e.areafrijol AreaCiclAnt, produccionfrijol produccionciclAnt, 
                    compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
                    where ct.idgrano = 2 and e.resultado = 1
                    union all 
                    select e.interview__id, e.fecha_entr, ct.idgrano,e.areasorgo AreaCiclAnt, produccionsorgo produccionciclAnt, 
                    compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
                    where ct.idgrano = 3 and e.resultado = 1
                    union all 
                    select e.interview__id, e.fecha_entr, ct.idgrano,e.areaarroz AreaCiclAnt, produccionarroz produccionciclAnt, 
                    compareapc  from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    inner join CteGranosBasicos ct on e.interview__id = ct.interviewid
                    where ct.idgrano = 4 and e.resultado = 1
                """
        dfcompara = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfcompara

    def ExtFondosAgricolas(self):
        query = """ 
                    select e.interview__id, e.credito, unnest(e.lcredito), e.inversionagricolaactual,e.remesa, e.porc_rem, e.pais  
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
                    where e.resultado = 1 
                """
        dfFondos = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfFondos

    def ExtCausasSiembra(self):
        query = """ 
                    select interview__id, unnest(resul) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
                    where e.resultado = 1
                    order by fecha_entr  
                """
        dfCausasSiembra = pl.DataFrame(pl.read_database_uri(query=query, uri=self.postgreConn, engine='connectorx'))
        return dfCausasSiembra
    
    def INSPRACTMAQ(self):
        query = """
                    select value as IdInsPracMaq, text as InsPracMaq, 'Practica' as Tipo from ws_dea.reusablecategoricaloptions r 
                    where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = '573c5634-9e89-4d6c-b9df-b8ee6f59c93a'
                    union all 
                    select value as IdInsPracMaq, text as InsPracMaq, 'Insumo' as Tipo from ws_dea.reusablecategoricaloptions r 
                    where r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' and r.categoriesid = 'a117f4fa-973c-417c-9730-a67794f7a732'
                    union all 
                    select value as IdInsPracMaq, text as InsPracMaq, 'Revisar' as Tipo 
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
            {'IdSemilla':[101,102,103,104,105,106,109],'Semilla':
             ['Semilla Nacional', 'Semilla Hibrida','Semilla Segregada','Frijol Rojo','Frijol Blanco','Frijol Negro','Semilla Mejorada']}

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
