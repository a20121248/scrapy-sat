# -*- coding: utf-8 -*-

import scrapy

class RecordConductorItem(scrapy.Item):
    dni = scrapy.Field()
    conductor = scrapy.Field()
    fecha_extraccion = scrapy.Field()
    infracciones = scrapy.Field()
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