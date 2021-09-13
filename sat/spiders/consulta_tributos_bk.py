# -*- coding: utf-8 -*-
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
    name = 'consulta_tributos_bk'
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
        print('JAVIER')
        file = open(f"salida-{response.meta.get('dni')}.html",'w')
        file.write(response.text)
        file.close()

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
            print(f'lista_codigos={lista_codigos}')
            print(f'codNum={codNum}')
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
                    #WuSzDUZoB0ERjEpror7b1kEv1qenkBWwcEKr5+JevhQ=
                    #url_tributos_resumen = f'{self.sat_tributos_resumen_url}?tri=V&mysession={mysession}'
                    meta = {
                        'me': me,
                        'codNum': codNum[j],
                        'cookiejar': cookNum,
                        'dni': dni,
                        'mysession': mysession
                    }
                    yield scrapy.FormRequest(url=url_busqueda, formdata=data, callback=self.parse_conductor, meta=meta, dont_filter=True)
            else:
                pass
                """
                line = f'{dni}\t'
    
                # Creamos una lista de una infracción vacía
                infraccion_item = TributoItem()
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

                item = DeudaTributariaItem()
                item['dni'] = dni
                item['conductor'] = '-'
                item['infracciones'] = infracciones_lst
                item['fecha_extraccion']=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                yield item
                line += f"-\t{item['fecha_extraccion']}\tERROR\n"
            with open(self.filepath_log, 'ab') as file:
                file.write(line.encode("utf8"))
                """

    def parse_conductor(self, response):
        mysession = response.meta.get('mysession')
        codNum = response.meta.get('codNum')
        formdata = {
            'ctl00$cplPrincipal$ucDatosCarrito1$valCantidad': '0',
            'ctl00$cplPrincipal$ucDatosCarrito1$valMonto': 'S/. 0.00',
            '__EVENTTARGET': 'ctl00$cplPrincipal$rbtMostrar$3',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': response.meta.get('VIEWSTATE'),
            '__EVENTVALIDATION': response.meta.get('EVENTVALIDATION'),
            'ctl00$cplPrincipal$rbtMostrar': '4',
            'ctl00$cplPrincipal$hidUrlPrint': f'TributosImprimir.aspx?ses={130272836}&con=0&ani=0&est=1&usu={codNum}'
        }

        #url = https://www.sat.gob.pe/VirtualSAT/modulos/TributosResumen.aspx?tri=T&mysession=WuSzDUZoB0ERjEpror7b1sxXi9gSojvvWCgiWiqoOyE%3d
        url_tributos_resumen = f'https://www.sat.gob.pe/VirtualSAT/modulos/TributosResumen.aspx?tri=T&mysession={mysession}'
        meta = {
            'me': response.meta.get('me'),
            'dni': response.meta.get('dni'),
            'cookiejar': response.meta.get('cookiejar'),
            'mysession': mysession
        }
        yield scrapy.Request(url=url_tributos_resumen, meta=meta, callback=self.parse_tributos_resumen, dont_filter=True)
    
    
        #url_tributos_resumen = f'{self.sat_tributos_resumen_url}?tri=V&mysession={mysession}'
        #yield scrapy.FormRequest(url=url_tributos_resumen, formdata=formdata, callback=self.parse_tributos_resumen, meta=meta, dont_filter=True)
        #yield scrapy.Request(full_img, meta=meta, callback=self.parse_tributos_resumen, dont_filter=True)

        #https://www.sat.gob.pe/VirtualSAT/modulos/TributosResumen.aspx?tri=T&mysession=WuSzDUZoB0ERjEpror7b1gR%2bzXO1BDaSmA29Gzq8KtQ%3d


    def parse_tributos_resumen(self, response):
        referer_url = response.request.headers.get('referer', None).decode('utf-8')
        referer_url_lst = referer_url.split('&')
        cod = referer_url_lst[-3].split('=')[1]
        codveh = referer_url_lst[-2].split('=')[1]
        pla = referer_url_lst[-1].split('=')[1]
        print(referer_url)
        print(f'codveh={codveh}')
        print(f'pla={pla}')
        file = open(f"salida-2-{response.meta.get('dni')}.html",'w')
        file.write(response.text)
        file.close()
        
