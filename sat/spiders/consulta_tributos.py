# -*- coding: utf-8 -*-
from re import A
import scrapy
import io
import time
import pandas as pd
from datetime import datetime
from PIL import Image
from sat.utils import descifrarimagen
from sat.items import TributoItem, DeudaTributariaItem
from urllib.request import *

class ConsultaTributosSpider(scrapy.Spider):
    name = 'consulta_tributos'
    YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, *args, **kwargs):
        super(ConsultaTributosSpider, self).__init__(*args, **kwargs)
        self.in_path = './1_INPUT/'
        self.out_path = './2_OUTPUT/'
        #self.init_url = 'https://www.sat.gob.pe/Websitev9'
        self.sat_url = 'https://www.sat.gob.pe/WebSitev8/IncioOV2.aspx'
        self.sat_busqueda_tributaria_url = 'https://www.sat.gob.pe/VirtualSAT/modulos/BusquedaTributario.aspx'
        self.sat_tributos_resumen_url = 'https://www.sat.gob.pe/VirtualSAT/modulos/TributosResumen.aspx'
        #https://www.sat.gob.pe/VirtualSAT/modulos/TributosResumen.aspx?mysession=WuSzDUZoB0ERjEpror7b1kEv1qenkBWwcEKr5%2bJevhQ%3d&tri=T&cod=Y6hahkGGwU8%3d&codveh=kC1iVSXSgTk%3d
        self.sat_imagen_url = 'https://www.sat.gob.pe/VirtualSAT'

        # INPUT
        dtype = {'DNI': str}
        self.input_df = pd.read_csv(self.in_path+self.filename, sep='\t', usecols=['DNI'], dtype=dtype, encoding='utf8')

        self.filepath_log = f"{self.out_path}consulta_tributos_{self.filename.split('.')[0]}_log_{self.YYYYMMDD_HHMMSS}.txt"
        with open(self.filepath_log, 'wb') as file:
            file.write('dni\tnombre_completo\tfecha_extraccion\testado\n'.encode("utf8"))
    
    def start_requests(self):
        meta = {
            'me': 'OK'
        }
        yield scrapy.Request(url=self.sat_url, meta=meta, callback=self.generate_session, dont_filter=True)
        
    def generate_session(self, response):
        me = response.meta.get('me')
        mysession = str(response.url).strip().split("?")[1][10:]

        url_busqueda = f'{self.sat_busqueda_tributaria_url}?mysession={mysession}&tri=V'
        for row_idx, row in self.input_df.iterrows():
            meta = {
                'dni': row['DNI'],
                'me': me,
                'cookiejar': row_idx,
                'mysession': mysession,
                'reload': True
            }
            yield scrapy.Request(url=url_busqueda, meta=meta, callback=self.parse_with_session, dont_filter=True)

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

        img_path = response.xpath("//img[@class='captcha_class']/@src").extract_first()
        full_img = self.sat_imagen_url + img_path[2:]
        yield scrapy.Request(full_img, meta=meta, callback=self.read_captcha, dont_filter=True)
    
    def read_captcha(self, response):
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
            'ctl00$cplPrincipal$ddlTipoDocu': '2',
            'ctl00$cplPrincipal$txtDocumento' : response.meta.get('dni'),
            'ctl00$cplPrincipal$txtCaptcha' : captcha,
            'ctl00$cplPrincipal$CaptchaContinue' : 'Buscar',
            'ctl00$cplPrincipal$hidTipConsulta' : 'busqTipoDocIdentidad'
        }

        url_busqueda = f'{self.sat_busqueda_tributaria_url}?tri=V&mysession={mysession}'
        yield scrapy.FormRequest(url=url_busqueda, formdata=formdata, meta=meta, callback=self.parseFilter, dont_filter=True)
            
    def parseFilter(self, response):
        me = response.meta.get('me')
        dni = response.meta.get('dni')
        cookNum = response.meta.get('cookiejar')
        mysession = response.meta.get('mysession')
        url_busqueda = f'{self.sat_busqueda_tributaria_url}?tri=V&mysession={mysession}'
        
        msj_captcha = response.xpath('//span[@id="ctl00_cplPrincipal_lblMensajeDocumento"]/text()').extract_first()
        if msj_captcha=="Código de seguridad incorrecta.":
            print(f'DNI={dni}. Problema con captcha={msj_captcha}')
            meta = {
                'me': 'NO',
                'dni': dni,
                'cookiejar': cookNum,
                'mysession': mysession
            }
            yield scrapy.Request(url=url_busqueda, meta=meta, callback=self.parse_with_session, dont_filter=True)
        else:
            lista_codigos = response.xpath("*//a[contains(@id,'lnkCodigo')]/@id").extract()
            codNum=response.css('a[id$="_lnkCodigo"]::text').extract()
            if len(lista_codigos)>0:
                for j,i in enumerate(lista_codigos):
                    data = {
                        '__EVENTTARGET': i.replace('_','$'),
                        '__EVENTARGUMENT':'',
                        '__VIEWSTATE': response.xpath("*//input[@id='__VIEWSTATE']/@value").extract_first().strip(),
                        '__EVENTVALIDATION': response.xpath("*//input[@id='__EVENTVALIDATION']/@value").extract_first().strip(),
                        'ctl00$cplPrincipal$hidTipConsulta': 'busqTipoDocIdentidad',
                        'ctl00$cplPrincipal$ucDatosCarrito1$valCantidad':'0',
                        'ctl00$cplPrincipal$ucDatosCarrito1$valMonto':'S/. 0.00'
                    }
                    meta = {
                        'me': me,
                        'codNum': codNum[j],
                        'cookiejar': cookNum,
                        'dni': dni,
                        'mysession': mysession
                    }
                    yield scrapy.FormRequest(url=url_busqueda, formdata=data, callback=self.parse_conductor, meta=meta, dont_filter=True)
            else:
                # DNI NO EXISTE O NO ENCONTRADO EN SAT
                fecha_extraccion = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                line = f"{dni}\t'-'\t{fecha_extraccion}\tNO REGISTRADO\n"
                with open(self.filepath_log, 'ab') as file:
                    file.write(line.encode("utf8"))

    def parse_conductor(self, response):
        mysession = response.meta.get('mysession')
        url_tributos_resumen = f'{self.sat_tributos_resumen_url}?tri=V&mysession={mysession}'
        meta = {
            'me': response.meta.get('me'),
            'dni': response.meta.get('dni'),
            'cookiejar': response.meta.get('cookiejar'),
            'mysession': mysession,
            'VIEWSTATE': response.meta.get('VIEWSTATE'),
            'EVENTVALIDATION': response.meta.get('EVENTVALIDATION'),
            'codigo_administrado': response.meta.get('codNum')
        }
        yield scrapy.Request(url=url_tributos_resumen, meta=meta, callback=self.parse_tributos_resumen, dont_filter=True)

    def parse_tributos_resumen(self, response):
        mysession = response.meta.get('mysession')
        dni = response.meta.get('dni')
        hidUrlPrint = response.xpath("*//input[contains(@id,'ctl00_cplPrincipal_hidUrlPrint')]/@value").extract_first()
        
        if hidUrlPrint is None:
            # DNI con error o registrado pero sin deuda
            fecha_extraccion = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            line = f"{dni}\t'-'\t{fecha_extraccion}\tSIN DEUDA\n"
            with open(self.filepath_log, 'ab') as file:
                file.write(line.encode("utf8"))
        else:
            formdata = {
                '__EVENTTARGET': 'ctl00$cplPrincipal$rbtMostrar$2',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': response.xpath("*//input[@id='__VIEWSTATE']/@value").extract_first().strip(),
                '__EVENTVALIDATION': response.xpath("*//input[@id='__EVENTVALIDATION']/@value").extract_first().strip(),
                'ctl00$cplPrincipal$rbtMostrar': '3',
                'ctl00$cplPrincipal$hidUrlPrint': hidUrlPrint
            }
            nombre_completo = response.xpath('//span[@id="ctl00_cplPrincipal_lblAdministrado"]/b/text()').extract_first().split('-')[1].strip()
            meta = {
                'dni': dni,
                'nombre_completo': nombre_completo,
                'codigo_administrado': response.meta.get('codigo_administrado'),
                'cookiejar': response.meta.get('cookiejar')
            }
            url_tributos_resumen = f'{self.sat_tributos_resumen_url}?tri=T&mysession={mysession}'
            yield scrapy.FormRequest(url=url_tributos_resumen, formdata=formdata, callback=self.parse_detalle_por_tributo, meta=meta, dont_filter=True)
        
    def parse_detalle_por_tributo(self, response):

        dni = response.meta.get('dni')
        codigo_administrado = response.meta.get('codigo_administrado')
        nombre_completo = response.meta.get('nombre_completo')

        # Creamos una lista de tributos
        tributos = response.xpath('//table[@id="ctl00_cplPrincipal_grdEstadoCuenta"]//tr')
        tributos_lst = []
        tributo_curr = ''
        line = f'{dni}\t'
        for tributo in tributos[1:]:
            sentinel_1 = tributo.xpath('td[1]/text()').extract_first().strip()
            sentinel_2 = tributo.xpath('td[2]/span/text()').extract_first()

            if sentinel_1 != '':
                tributo_curr = sentinel_1
            elif sentinel_2 is not None:
                sentinel_2 = sentinel_2.strip()
                if sentinel_2 != '':
                    tributo_item = TributoItem()
                    tributo_item['tributo'] = tributo_curr
                    tributo_item['anho'] = sentinel_2
                    tributo_item['periodo'] = tributo.xpath('td[3]/span//text()').extract_first().strip()
                    tributo_item['documento'] = tributo.xpath('td[4]/span//text()').extract_first().strip()
                    tributo_item['total_a_pagar'] = tributo.xpath('td[5]/span//text()').extract_first().strip()
                    tributo_item['total_a_pagar_ofic_sat'] = tributo.xpath('td[6]/span//text()').extract_first().strip()
                    tributo_item['total_a_pagar_web_bancos'] = tributo.xpath('td[7]/span//text()').extract_first().strip()
                    tributos_lst.append(dict(tributo_item))

        item = DeudaTributariaItem()
        item['dni'] = dni
        item['codigo_administrado'] = codigo_administrado
        item['nombre_completo'] = nombre_completo
        item['tributos'] = tributos_lst
        item['fecha_extraccion']=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        yield item
        line += f"{item['nombre_completo']}\t{item['fecha_extraccion']}\tSUCCESS\n"

        with open(self.filepath_log, 'ab') as file:
            file.write(line.encode("utf8"))
