# -*- coding: utf-8 -*-
import scrapy
import io
import time
import pandas as pd
from datetime import datetime
from PIL import Image
from sat.utils import descifrarimagen
from sat.items import InfraccionItem, RecordConductorItem
from urllib.request import *

class RecordConductorSpider(scrapy.Spider):
    name = 'multas_de_transito'
    YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, *args, **kwargs):
        super(RecordConductorSpider, self).__init__(*args, **kwargs)
        self.in_path = './1_INPUT/'
        self.out_path = './2_OUTPUT/'
        #self.init_url = 'https://www.sat.gob.pe/Websitev9'
        self.sat_url = 'https://www.sat.gob.pe/WebSitev8/IncioOV2.aspx'
        self.sat_record_conductor_url = 'https://www.sat.gob.pe/VirtualSAT/modulos/RecordConductor.aspx'
        self.sat_imagen_url = 'https://www.sat.gob.pe/VirtualSAT'

        # INPUT
        dtype = {'DNI': str}
        self.input_df = pd.read_csv(self.in_path+self.filename, sep='\t', usecols=['DNI'], dtype=dtype, encoding='utf8')

        self.filepath_log = f"{self.out_path}multas_de_transito_{self.filename.split('.')[0]}_log_{self.YYYYMMDD_HHMMSS}.txt"
        with open(self.filepath_log, 'wb') as file:
            file.write('dni\tconductor\tfecha_extraccion\testado\n'.encode("utf8"))
    
    def start_requests(self):
        meta = {
            'me': 'OK'
        }
        yield scrapy.Request(url=self.sat_url, meta=meta, callback=self.generate_session, dont_filter=True)
        
    def generate_session(self, response):
        me = response.meta.get('me')
        mysession = str(response.url).strip().split("?")[1][10:]

        url_busqueda = f'{self.sat_record_conductor_url}?mysession={mysession}&tri=R'
        for row_idx, row in self.input_df.iterrows():
            meta = {
                'dni': row['DNI'],
                'me': me,
                'cookiejar': row_idx,
                'mysession': mysession,
                'reload': True
            }
            yield scrapy.Request(url=url_busqueda, meta=meta, callback=self.parse_with_session, dont_filter=True)
    
    # I know that the captcha won't load, so we better do another request in advance
    def parse_with_session(self, response):
        mysession = response.meta.get('mysession')
        VIEWSTATE = response.xpath("*//input[@id='__VIEWSTATE']/@value").extract_first().strip()
        EVENTVALIDATION = response.xpath("*//input[@id='__EVENTVALIDATION']/@value").extract_first().strip()
        meta = {
            'me': response.meta.get('me'),
            'dni': response.meta.get('dni'),
            'cookiejar': response.meta.get('cookiejar'),
            'mysession': mysession,
            'VIEWSTATE': VIEWSTATE,
            'EVENTVALIDATION': EVENTVALIDATION
        }

        if response.meta.get('reload'):
            formdata = {
                'ctl00$cplPrincipal$ucDatosCarrito1$valCantidad': '0',
                'ctl00$cplPrincipal$ucDatosCarrito1$valMonto': 'S/. 0.00',
                '__VIEWSTATE': VIEWSTATE,
                '__EVENTVALIDATION': EVENTVALIDATION,
                'ctl00$cplPrincipal$txtLicencia': '',
                'ctl00$cplPrincipal$txtDNI': '',
                'ctl00$cplPrincipal$txtCaptcha': '',
                'ctl00$cplPrincipal$CaptchaContinue': 'Buscar',
                'ctl00$cplPrincipal$hidTipConsulta': 'busqDNI'
            }
            url_captcha = f'{self.sat_record_conductor_url}?tri=R&tipoSancion=cond&mysession={mysession}'
            yield scrapy.FormRequest(url=url_captcha, formdata=formdata, meta=meta, callback=self.parse_captcha, dont_filter=True)
        else:
            img_path = response.xpath("//img[@class='captcha_class']/@src").extract_first()
            full_img = self.sat_imagen_url + img_path[2:]
            yield scrapy.Request(full_img, meta=meta, callback=self.read_captcha, dont_filter=True)

    def parse_captcha(self, response):
        img_path = response.xpath("//img[@class='captcha_class']/@src").extract_first()
        full_img = self.sat_imagen_url + img_path[2:]
        meta = {
            'me': response.meta.get('me'),
            'dni': response.meta.get('dni'),
            'cookiejar': response.meta.get('cookiejar'),
            'mysession': response.meta.get('mysession'),
            'VIEWSTATE': response.meta.get('VIEWSTATE'),
            'EVENTVALIDATION': response.meta.get('EVENTVALIDATION')
        }
        yield scrapy.Request(full_img, meta=meta, callback=self.read_captcha, dont_filter=True)
    
    def read_captcha(self, response):
        mysession = response.meta.get('mysession')
        meta = {
            'me': response.meta.get('me'),
            'dni': response.meta.get('dni'),
            'cookiejar': response.meta.get('cookiejar'),
            'mysession': mysession
        }

        img_bytes = response.body
        capturaImagen = Image.open(io.BytesIO(img_bytes))
        #capturaImagen.save(f"./captcha/{dni}_image_sat.png")
        ancho,alto = capturaImagen.size
        captcha = descifrarimagen(capturaImagen, int(ancho), int(alto))

        mysession = response.meta.get('mysession')
        meta = {
            'me': response.meta.get('me'),
            'dni': response.meta.get('dni'),
            'cookiejar': response.meta.get('cookiejar'),
            'mysession': mysession
        }

        formdata = {
            'ctl00$cplPrincipal$ucDatosCarrito1$valCantidad': '0',
            'ctl00$cplPrincipal$ucDatosCarrito1$valMonto': 'S/. 0.00',
            '__VIEWSTATE': response.meta.get('VIEWSTATE'),
            '__EVENTVALIDATION': response.meta.get('EVENTVALIDATION'),
            'ctl00$cplPrincipal$txtLicencia': '',
            'ctl00$cplPrincipal$txtDNI': response.meta.get('dni'),
            'ctl00$cplPrincipal$txtCaptcha': captcha,
            'ctl00$cplPrincipal$CaptchaContinue': 'Buscar',
            'ctl00$cplPrincipal$hidTipConsulta': 'busqDNI'
        }

        url_busqueda = f'{self.sat_record_conductor_url}?tri=R&tipoSancion=cond&mysession={mysession}'
        yield scrapy.FormRequest(url=url_busqueda, formdata=formdata, meta=meta, callback=self.parseFilter, dont_filter=True)
            
    def parseFilter(self,response):
        me = response.meta.get('me')
        dni = response.meta.get('dni')
        cookNum = response.meta.get('cookiejar')
        mysession = response.meta.get('mysession')
        url_busqueda = f'{self.sat_record_conductor_url}?tri=R&tipoSancion=cond&mysession={mysession}'
        
        msj_captcha = response.xpath('//span[@id="ctl00_cplPrincipal_lblMensajeCapcha"]/font/text()').extract_first()
        if msj_captcha=="Código de seguridad incorrecta.":
            print(f'DNI={dni}. Problema con captcha={msj_captcha}')
            meta = {
                'me': 'NO',
                'dni': dni,
                'cookiejar': cookNum,
                'mysession': mysession,
                'reload': False
            }
            yield scrapy.Request(url=url_busqueda, meta=meta, callback=self.parse_with_session, dont_filter=True)
        else:
            conductor = response.xpath('//span[@id="ctl00_cplPrincipal_lblConductor"]/b/text()')
            line = f'{dni}\t'
            if len(conductor)==0:
                # Creamos una lista de una infracción vacía
                infraccion_item = InfraccionItem()
                infraccion_item['doc_infraccion'] = '-'
                infraccion_item['reglamento'] = '-'
                infraccion_item['falta'] = '-'
                infraccion_item['fecha_infraccion'] = '-'
                infraccion_item['placa'] = '-'
                infraccion_item['importe'] = '-'
                infraccion_item['tipo_falta'] = '-'
                infraccion_item['licencia'] = '-'
                infraccion_item['estado'] = '-'
                infracciones_lst = [dict(infraccion_item)]

                item = RecordConductorItem()
                item['dni'] = dni
                item['conductor'] = '-'
                item['infracciones'] = infracciones_lst
                item['fecha_extraccion']=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                yield item
                line += f"-\t{item['fecha_extraccion']}\tERROR\n"
            else:
                # Creamos una lista de infracciones
                infracciones = response.xpath('//table[@id="ctl00_cplPrincipal_grdRecord"]//tr')
                infracciones_lst = []
                for infraccion in infracciones[1:]:
                    infraccion_item = InfraccionItem()
                    infraccion_item['doc_infraccion'] = infraccion.xpath('td[1]/span//text()').extract_first().strip()
                    infraccion_item['reglamento'] = infraccion.xpath('td[2]/span//text()').extract_first().strip()
                    infraccion_item['falta'] = infraccion.xpath('td[3]/span//text()').extract_first().strip()
                    infraccion_item['fecha_infraccion'] = infraccion.xpath('td[4]/span//text()').extract_first().strip()
                    infraccion_item['placa'] = infraccion.xpath('td[5]/span//text()').extract_first().strip()
                    infraccion_item['importe'] = infraccion.xpath('td[6]/span//text()').extract_first().strip()
                    infraccion_item['tipo_falta'] = infraccion.xpath('td[7]/span//text()').extract_first().strip()
                    infraccion_item['licencia'] = infraccion.xpath('td[8]/span//text()').extract_first().strip()
                    infraccion_item['estado'] = infraccion.xpath('td[9]/span//text()').extract_first().strip()
                    infracciones_lst.append(dict(infraccion_item))

                item = RecordConductorItem()
                item['dni'] = dni
                item['conductor'] = conductor.extract_first()[11:]
                item['infracciones'] = infracciones_lst
                item['fecha_extraccion']=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                yield item
                line += f"{item['conductor']}\t{item['fecha_extraccion']}\tSUCCESS\n"
            with open(self.filepath_log, 'ab') as file:
                file.write(line.encode("utf8"))