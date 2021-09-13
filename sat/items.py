# -*- coding: utf-8 -*-

import scrapy

class RecordConductorItem(scrapy.Item):
    dni = scrapy.Field()
    conductor = scrapy.Field()
    infracciones = scrapy.Field()
    fecha_extraccion = scrapy.Field()
    pass

class InfraccionItem(scrapy.Item):
    doc_infraccion = scrapy.Field()
    reglamento = scrapy.Field()
    falta = scrapy.Field()
    fecha_infraccion = scrapy.Field()
    placa = scrapy.Field()
    importe = scrapy.Field()
    tipo_falta = scrapy.Field()
    licencia = scrapy.Field()
    estado = scrapy.Field()
    pass

class TributoItem(scrapy.Item):
    tributo = scrapy.Field()
    anho = scrapy.Field()
    periodo = scrapy.Field()
    documento = scrapy.Field()
    total_a_pagar = scrapy.Field()
    total_a_pagar_ofic_sat = scrapy.Field()
    total_a_pagar_web_bancos = scrapy.Field()
    pass

class DeudaTributariaItem(scrapy.Item):
    dni = scrapy.Field()
    codigo_administrado = scrapy.Field()
    nombre_completo = scrapy.Field()
    tributos = scrapy.Field()
    fecha_extraccion = scrapy.Field()
    pass
