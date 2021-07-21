# -*- coding: utf-8 -*-

import pandas as pd
from sat.items import RecordConductorItem

class csvWriterPipeline(object):
    out_path = './2_OUTPUT/'
    items_written_infogeneral = 0

    def open_spider(self, spider):
        spider_name = type(spider).__name__
        if spider_name == 'RecordConductorSpider':
            self.ctd_save_infogeneral = 1
            self.columnsPrincipal = ['dni','conductor','doc_infraccion','reglamento','falta','fecha_infraccion','placa','importe','tipo_falta','licencia','estado','fecha_extraccion']
            self.prename_infogeneral = f"{self.out_path}record_conductor_{getattr(spider, 'filename').split('.')[0]}_{getattr(spider, 'YYYYMMDD_HHMMSS')}.txt"
        self.data_encontrada_infogeneral = []

    def process_item(self, item, spider):
        if isinstance(item, RecordConductorItem):
            item_df = pd.json_normalize(item['infracciones'])
            item_df['dni'] = item['dni']
            item_df['conductor'] = item['conductor']
            item_df['fecha_extraccion'] = item['fecha_extraccion']
        self.items_written_infogeneral += 1
        self.data_encontrada_infogeneral.append(item_df)
        if self.items_written_infogeneral % self.ctd_save_infogeneral == 0:
            self.data_encontrada_infogeneral = self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral)
        return item

    def guarda_data(self, lista_df, ctd_items=0):
        if len(lista_df)>0:
            pd.concat(lista_df).to_csv(self.prename_infogeneral, sep='\t', header=ctd_items<=self.ctd_save_infogeneral, index=False, encoding="utf-8", columns=self.columnsPrincipal, mode='a')
        return []

    def __del__(self):
        self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral)
        print(25*'=','THE END',25*'=')