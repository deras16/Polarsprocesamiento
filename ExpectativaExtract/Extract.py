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
                where e.resultado = 1 or e.resultadost = 1
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
                   with cteGrano(interviewid, idgrano) as(
	                    select e.interview__id, unnest(e.expec) from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e 
                    )
                    select er.interview__id, ct.idgrano, e2.num_expl_agricm, er2.depto_explosm iddepto, er2.munic_explosm idmuni, 
                    er.roster__vector[2] idepoca, er.roster__vector[3] idsemilla, er.aream areaprod, er.produccionm produccion
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAM" er
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RMAIZ" er2 on er2.interview__id = er.interview__id 
                    inner join cteGrano ct on ct.interviewid = er.interview__id
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e2 on e2.interview__id = er.interview__id 
                    where ct.idgrano = 1 and (e2.resultado = 1 or e2.resultadost = 1)
                    union all 
                    select er3.interview__id, ct.idgrano, e3.num_expl_agricf, er4.depto_explosf, er4.munic_explosf, er3.roster__vector[2] idepoca,
                    er3.roster__vector[3] idsemilla, er3.areaf  areaprod, er3.produccionf produccion
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAF" er3 
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RFRIJOL" er4 on er4.interview__id = er3.interview__id 
                    inner join cteGrano ct on ct.interviewid = er3.interview__id
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e3 on e3.interview__id = er3.interview__id 
                    where ct.idgrano = 2 and (e3.resultado = 1 or e3.resultadost = 1)
                    union all
                    select er5.interview__id, ct.idgrano, e4.num_expl_agrics, er6.depto_exploss, er6.munic_exploss, er5.roster__vector[2] idepoca,
                    er5.roster__vector[3] idsemilla, er5.areas areaprod, er5.produccions produccion
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAS" er5
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSORGO" er6 on er6.interview__id = er5.interview__id 
                    inner join cteGrano ct on ct.interviewid = er5.interview__id
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e4 on e4.interview__id = er5.interview__id 
                    where ct.idgrano = 3 and (e4.resultado = 1 or e4.resultadost = 1)
                    union all
                    select er7.interview__id,ct.idgrano, e5.num_expl_agrica, er8.depto_explosa, er8.munic_explosa, er7.roster__vector[2],
                    er7.roster__vector[3], er7.areaa, er7.producciona
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RSEMILLAA" er7 
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1_RARROZ" er8 on er8.interview__id = er7.interview__id
                    inner join ctegrano ct on ct.interviewid = er7.interview__id
                    inner join "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e5 on e5.interview__id = er7.interview__id 
                    where ct.idgrano = 4 and (e5.resultado = 1 or e5.resultadost = 1)
                    union all 
                    select e.interview__id, 1 idgrano, 1 numexplt,case
                        when e.lugar_sede = 1 then r3.value
                        when lugar_sede is null then r3.value
                        else r.value end as iddepto, 
                    case 
                        when e.lugar_sede = 1 then r4.value
                        when e.lugar_sede is null then r4.value
                        else r2.value 
                    end as idmunu,7 idepoca,120 idsemilla, e.semilla_maiz area, 0 produccion 
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
                    left join ws_dea.reusablecategoricaloptions r on r.value = e.depto_sede and r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
                    left join ws_dea.reusablecategoricaloptions r3 on r3.text = e.departamento and r3.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r3.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
                    left join ws_dea.reusablecategoricaloptions r2 on r2.value = e.mun_sede and r2.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r2.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
                    left join ws_dea.reusablecategoricaloptions r4 on r4.text = e.municipio and r4.parentvalue = r3.value and r4.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r4.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
                    where e.semilla_maiz > 0 and (e.resultado = 1 or e.resultadost = 1)
                    union all
                    select e.interview__id, 2 idgrano, 1 numexplt,case 
                        when e.lugar_sede = 1 then r5.value 
                        when lugar_sede is null then r5.value  
                        else r.value
                    end as iddepto, case 
                        when e.lugar_sede = 1 then r6.value 
                        when lugar_sede is null then r6.value  
                        else r2.value 
                    end as idmuni, 7 idepoca,121 idsemilla, e.semilla_frijol area, 0 produccion 
                    from "hq_dea_3a9df112-2351-459e-97a6-468d1cfaaf91"."EXPGB_2$1" e
                    left join ws_dea.reusablecategoricaloptions r on r.value = e.depto_sede and r.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
                    left join ws_dea.reusablecategoricaloptions r2 on r2.value = e.mun_sede and r2.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r2.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
                    left join ws_dea.reusablecategoricaloptions r5 on r5.text = e.departamento and r5.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r5.categoriesid = 'eba0ae33-2c7c-458e-9b99-d2ca85729cee'
                    left join ws_dea.reusablecategoricaloptions r6 on r6.text = e.municipio and r6.parentvalue = r5.value and r6.questionnaireid = 'f3be9695-9847-4dfc-9f7d-b64790b029cf' 
                    and r6.categoriesid = 'c0eac36c-1598-dd17-53ed-9fb351d194dd'
                    where e.semilla_frijol > 0 and (e.resultado = 1 or e.resultadost = 1)
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
