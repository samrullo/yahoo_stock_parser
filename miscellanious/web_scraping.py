import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from urllib.request import urlopen
from bs4 import BeautifulSoup

url = "https://indexes.nikkei.co.jp/nkave/index/component?idx=nk225"

html = urlopen(url)
soup = BeautifulSoup(html, 'lxml')

print(soup.title)
# print(soup.get_text()[:2000])

# for link in soup.find_all('a'):
#     print(link.get('href'))

divs = soup.find_all('div')
for i, div in enumerate(divs):
    subdivs = div.find_all('div')
    for si, sub_div in enumerate(subdivs):
        # print("{}: class value is {}".format(si,sub_div.get('class')))
        if set(('col-sm-8', 'col-xs-12')) == set(sub_div.get('class')):
            print("The index of div is {} and sub_div is {}".format(i, si))

