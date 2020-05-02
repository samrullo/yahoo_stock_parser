import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from utils.myfunctions import setup_custom_logger
from urllib.request import urlopen
from bs4 import BeautifulSoup

_logger = setup_custom_logger(__file__, 'nikkei225_scraping.log', log_level=logging.INFO)

url = "https://indexes.nikkei.co.jp/nkave/index/component?idx=nk225"

html = urlopen(url)
soup = BeautifulSoup(html, 'lxml')

divs = soup.find_all('div')
for i, div in enumerate(divs):
    subdivs = div.find_all('div')
    for si, sub_div in enumerate(subdivs):
        # print("{}: class value is {}".format(si,sub_div.get('class')))
        if set(('col-sm-8', 'col-xs-12')).issubset(set(sub_div.get('class'))):
            _logger.info("The index of div is {} and sub_div is {}".format(i, si))
            nk225_table = sub_div

nk225_df = pd.DataFrame(columns=['code', 'name', 'full_name', 'industry'])
current_industry = ""
for row in nk225_table:
    record = BeautifulSoup(str(row), 'lxml').get_text()
    _logger.info("Raw record:{}".format(record))
    if record:
        record_as_list = record.split('\n')
        _logger.info("Initial record as list:{}".format(record_as_list))
        aset = set(record_as_list)
        if '' in aset:
            aset.remove('')
        _logger.info("After converting to set and removing empty strings:{}".format(aset))
        if len(aset) == 1:
            current_industry = aset.pop()
            _logger.info("The record contains industry_name:{}".format(current_industry))
        elif len(aset) == 3:
            record_list = list(aset)
            record_list.sort()
            _logger.info("This was regular record:{}".format(record_list + [current_industry]))
            if 'コード' not in record_list:
                nk225_df.loc[record_list[0], 'code'] = record_list[0].strip()
                nk225_df.loc[record_list[0], 'name'] = record_list[1].strip()
                nk225_df.loc[record_list[0], 'full_name'] = record_list[2].strip()
                nk225_df.loc[record_list[0], 'industry'] = current_industry
