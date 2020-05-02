import requests
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import numpy as np
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class YahooStockParser:
    def __init__(self, ticker: str, start: datetime.date, end: datetime.date):
        """
        Initialize Yahoo Stock Parser
        :param ticker:
        :param start:
        :param end:
        """
        self.ticker = ticker
        self.start = start
        self.end = end
        self.url = f'https://info.finance.yahoo.co.jp/history/?code={ticker}&sy={start.year}&sm={start.month}&sd={start.day}&ey={end.year}&em={end.month}&ed={end.day}&tm=d&p=1'
        self.stock_name = self.get_stock_name()
        logging.info(f"Initialized Yahoo Stock Parser for {self.ticker} : {self.stock_name} for the period {self.start} to {self.end}")

    def get_number_part_of_jp_date_token(self, jp_date: str, jp_date_token: str) -> int:
        """
        Given jp_date YYYY年MM月DD日, extract integer part for a specified jp_date_token and return integer part
        :param jp_date: YYYY年MM月DD日
        :param jp_date_token: 年,月 or 日
        :return: extracted integer
        """
        jp_date_paterns = re.findall("\d+" + jp_date_token, jp_date)
        return int(jp_date_paterns[0].replace(jp_date_token, ""))

    def get_date_from_japanese_date(self, jp_date: str) -> datetime.date:
        """
        Convert japanese date such as YYYY年MM月DD日 to datetime.date
        :param jp_date:
        :return:
        """
        return datetime.date(self.get_number_part_of_jp_date_token(jp_date, "年"), self.get_number_part_of_jp_date_token(jp_date, "月"), self.get_number_part_of_jp_date_token(jp_date, "日"))

    def convert_comma_seperated_number_string_to_integer(self, stock_px_str: str) -> int:
        """
        Convert comma seperated number string to integer
        :param stock_px_str: stock price string
        :return: stock price as integer
        """
        return int(stock_px_str.replace(",", ""))

    def convert_comma_seperated_number_string_to_float(self, stock_px_str: str) -> float:
        """
        Convert comma seperated number string to float
        :param stock_px_str: stock price string
        :return: stock price as float
        """
        return float(stock_px_str.replace(",", ""))

    def get_url_based_on_page_number(self, page: int) -> str:
        """
        Get url for a specified page number
        :param page: page number
        :return: url
        """
        return f'https://info.finance.yahoo.co.jp/history/?code={self.ticker}&sy={self.start.year}&sm={self.start.month}&sd={self.start.day}&ey={self.end.year}&em={self.end.month}&ed={self.end.day}&tm=d&p={page}'

    def get_html_from_url(self, url: str) -> str:
        """
        Get html text from specified url
        :param url: url string
        :return: html text
        """
        res = requests.get(url)
        return res.text

    def get_stock_name(self):
        """
        Get stock name by parsing title of the html of the first page for stock ticker
        :return:
        """
        html = self.get_html_from_url(self.url)
        soup = BeautifulSoup(html, 'lxml')
        stock_name_patterns = re.findall(".*株", soup.title.get_text())
        if len(stock_name_patterns) > 0:
            return stock_name_patterns[0]
        else:
            return "NOSTOCKNAMEFOUND"

    def generate_stock_dataframe_from_html(self, html):
        """
        Get stock dataframe from the specified html text
        :param html: html text
        :return: stock prices dataframe
        """
        stock_df = pd.DataFrame(columns=['adate', 'px_open', 'px_high', 'px_low', 'px_close', 'volume', 'px_close_after_adj'])
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table', attrs={'class': 'boardFin'})
        if len(tables) == 1:
            stock_table = tables[0]
            # if stock_table has no td elements this means we reached the end and no prices
            if len(stock_table.find_all('td')) > 0:
                stock_rows = stock_table.find_all('tr')
                # first row is a japanese header so we will skip it
                for i, stock_row in enumerate(stock_rows[1:]):
                    stock_columns = stock_row.find_all('td')
                    # there has to be 7 items in the row
                    if len(stock_columns) == 7:
                        stock_df.loc[i, 'adate'] = self.get_date_from_japanese_date(stock_columns[0].get_text())
                        stock_df.loc[i, 'px_open'] = self.convert_comma_seperated_number_string_to_float(stock_columns[1].get_text())
                        stock_df.loc[i, 'px_high'] = self.convert_comma_seperated_number_string_to_float(stock_columns[2].get_text())
                        stock_df.loc[i, 'px_low'] = self.convert_comma_seperated_number_string_to_float(stock_columns[3].get_text())
                        stock_df.loc[i, 'px_close'] = self.convert_comma_seperated_number_string_to_float(stock_columns[4].get_text())
                        stock_df.loc[i, 'volume'] = self.convert_comma_seperated_number_string_to_integer(stock_columns[5].get_text())
                        stock_df.loc[i, 'px_close_after_adj'] = self.convert_comma_seperated_number_string_to_float(stock_columns[6].get_text())
        return stock_df

    def get_all_stock_dataframe(self):
        """
        Get all existing stock prices as dataframe for the specified period
        :return: all stock prices dataframe
        """
        stock_df_list = []
        priceExists = True
        page = 1
        while priceExists:
            url = self.get_url_based_on_page_number(page)
            self.url = url
            html = self.get_html_from_url(url)
            stock_df = self.generate_stock_dataframe_from_html(html)
            stock_df_list.append(stock_df)
            if len(stock_df) == 0:
                priceExists = False
            page += 1
        if len(stock_df_list) > 0:
            stock_all_df = pd.concat(stock_df_list)
            stock_all_df.index = range(len(stock_all_df))
            logging.info(f"found total of {len(stock_all_df)} prices for {self.stock_name} for the period {self.start} to {self.end}")
            stock_all_df['ticker'] = self.ticker
            return stock_all_df
        else:
            return False
