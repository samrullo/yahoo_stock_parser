import requests
import bs4
from bs4 import BeautifulSoup
import re
import datetime
import pandas as pd
import numpy as np
import logging
import datetime
from typing import Tuple


def to_yyyymmdd(adate: datetime.date):
    return datetime.datetime.strftime(adate, "%Y%m%d")


def construct_url(ticker: str, start_date: datetime.date, end_date: datetime.date, page_num: int) -> str:
    """
    Construct url to locate stock prices based on ticker, start and end date and page number
    :param start_date:
    :param end_date:
    :param page_num:
    :return: url of a ticker from start date to end date
    """
    return f"https://finance.yahoo.co.jp/quote/{ticker}/history?styl=stock&from={to_yyyymmdd(start_date)}&to={to_yyyymmdd(end_date)}&timeFrame=d&page={page_num}"


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
        self.url = construct_url(ticker, start, end, 1)
        self.stock_px_table_class_name = "StocksEtfReitPriceHistory__historyTable__13C_ HistoryTable__1aNP"
        self.profile_url = f"https://stocks.finance.yahoo.co.jp/stocks/profile/?code={self.ticker}"
        self.profile_fields = {"特色": "features", "連結事業": "consolidated_business", "本社所在地": "headquarters",
                               "最寄り駅": "closest_station", "電話番号": "phone", "業種分類": "industry",
                               "英文社名": "company_name_en", "代表者名": "ceo", "設立年月日": "founded_date",
                               "市場名": "listed_market", "上場年月日": "ipo_date", "決算": "settlement_date",
                               "単元株数": "number_of_shares_per_transaction",
                               "従業員数（単独）": "number_of_employees_single", "従業員数（連結）": "number_of_employees_consolidated",
                               "平均年齢": "average_age", "平均年収": "average_annual_salary"}
        self.stock_name = self.get_stock_name()
        logging.info(
            f"Initialized Yahoo Stock Parser for {self.ticker} : {self.stock_name} for the period {self.start} to {self.end}")

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
        return datetime.date(self.get_number_part_of_jp_date_token(jp_date, "年"),
                             self.get_number_part_of_jp_date_token(jp_date, "月"),
                             self.get_number_part_of_jp_date_token(jp_date, "日"))

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
        return construct_url(self.ticker, self.start, self.end, page)

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
        try:
            html = self.get_html_from_url(self.url)
            soup = BeautifulSoup(html, 'lxml')
            stock_name_patterns = re.findall(".*株", soup.title.get_text())
            if len(stock_name_patterns) > 0:
                return stock_name_patterns[0]
            else:
                return "NOSTOCKNAMEFOUND"
        except Exception as e:
            logging.info(f"error : {e}")
            return "NOSTOCKNAMEFOUND"

    def extract_data_from_stock_px_row(self, stock_px_row: bs4.element.Tag) -> Tuple[
        datetime.date, float, float, float, float, int, float]:
        """
        Extract data like adate,open, high,low,close prices and adjusted price
        :param stock_px_row:
        :return: tuple of (adate,open,high,low,close,volume,adjusted price)
        """
        th_elements = stock_px_row.find_all("th")
        th_element = th_elements[0]
        adate = self.get_date_from_japanese_date(th_element.get_text())
        stock_columns = stock_px_row.find_all("td")
        if len(stock_columns) == 6:
            px_open = self.convert_comma_seperated_number_string_to_float(stock_columns[0].get_text())
            px_high = self.convert_comma_seperated_number_string_to_float(
                stock_columns[1].get_text())
            px_low = self.convert_comma_seperated_number_string_to_float(
                stock_columns[2].get_text())
            px_close = self.convert_comma_seperated_number_string_to_float(
                stock_columns[3].get_text())
            volume = self.convert_comma_seperated_number_string_to_integer(
                stock_columns[4].get_text())
            px_close_after_adj = self.convert_comma_seperated_number_string_to_float(
                stock_columns[5].get_text())
            return (adate, px_open, px_high, px_low, px_close, volume, px_close_after_adj)

    def generate_stock_dataframe_from_html(self, html):
        """
        Get stock dataframe from the specified html text
        :param html: html text
        :return: stock prices dataframe
        """
        stock_df = pd.DataFrame(
            columns=['adate', 'px_open', 'px_high', 'px_low', 'px_close', 'volume', 'px_close_after_adj'])
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table', attrs={'class': self.stock_px_table_class_name})
        if len(tables) == 1:
            stock_table = tables[0]
            # if stock_table has no td elements this means we reached the end and no prices
            if len(stock_table.find_all('td')) > 0:
                stock_rows = stock_table.find_all('tr')
                # first row is a japanese header so we will skip it
                for i, stock_row in enumerate(stock_rows[1:]):
                    try:
                        (adate, px_open, px_high, px_low, px_close, volume,
                         px_close_after_adj) = self.extract_data_from_stock_px_row(stock_row)
                        # there has to be 7 items in the row
                        stock_df.loc[i, 'adate'] = adate
                        stock_df.loc[i, 'px_open'] = px_open
                        stock_df.loc[i, 'px_high'] = px_high
                        stock_df.loc[i, 'px_low'] = px_low
                        stock_df.loc[i, 'px_close'] = px_close
                        stock_df.loc[i, 'volume'] = volume
                        stock_df.loc[i, 'px_close_after_adj'] = px_close_after_adj
                    except Exception as e:
                        logging.info(f"Something went wrong processing {i+1}th row of {self.ticker}\n{e}")
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
            logging.info(
                f"found total of {len(stock_all_df)} prices for {self.stock_name} for the period {self.start} to {self.end}")
            stock_all_df['ticker'] = self.ticker
            return stock_all_df
        else:
            return False

    def get_company_profile(self):
        html = self.get_html_from_url(self.profile_url)
        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all('table', attrs={'class': 'boardFinCom'})
        table = tables[0]
        rows = table.find_all('tr')
        profile = {'ticker': self.ticker}
        for row in rows:
            ths = row.find_all('th')
            tds = row.find_all('td')
            if len(ths) > 1:
                if "従業員数" in ths[0].get_text():
                    number_of_employees_single = tds[0].get_text().replace("人", "")
                    number_of_employees_consolidated = tds[1].get_text().replace("人", "")
                    if number_of_employees_single == '-':
                        profile['number_of_employees_single'] = 0
                        profile['number_of_employees_consolidated'] = 0
                    elif number_of_employees_consolidated == '-':
                        profile['number_of_employees_single'] = self.convert_comma_seperated_number_string_to_integer(
                            number_of_employees_single)
                        profile['number_of_employees_consolidated'] = profile['number_of_employees_single']
                    else:
                        profile['number_of_employees_single'] = self.convert_comma_seperated_number_string_to_integer(
                            number_of_employees_single)
                        profile[
                            'number_of_employees_consolidated'] = self.convert_comma_seperated_number_string_to_integer(
                            number_of_employees_consolidated)
                    logging.debug(
                        f"number_of_employees single : {profile['number_of_employees_single']}, number_of_employees_consolidated : {profile['number_of_employees_consolidated']}")
                if "平均" in ths[0].get_text():
                    average_age = tds[0].get_text().replace("歳", "")
                    average_annual_salary = tds[1].get_text().replace("千円", "")
                    if '‐' not in average_age and '-' not in average_age:
                        profile['average_age'] = self.convert_comma_seperated_number_string_to_float(average_age)
                    else:
                        profile['average_age'] = 0
                    if '‐' not in average_annual_salary and '-' not in average_annual_salary:
                        profile['average_annual_salary'] = self.convert_comma_seperated_number_string_to_integer(
                            average_annual_salary) * 1000
                    else:
                        profile['average_annual_salary'] = 0
                    logging.debug(
                        f"average age : {profile['average_age']}, average annual salary : {profile['average_annual_salary']}")
            else:
                if "設立年月日" in ths[0].get_text():
                    founded_date_str = tds[0].get_text().strip()
                    if "日" not in founded_date_str:
                        founded_date_str += "1日"
                    profile['founded_date'] = self.get_date_from_japanese_date(founded_date_str)
                    logging.debug(f"founded date : {profile['founded_date']}")
                elif "上場年月日" in ths[0].get_text():
                    ipo_date_str = tds[0].get_text().strip()
                    if "日" not in ipo_date_str:
                        ipo_date_str += "1日"
                    logging.debug(f"trying to convert ipo date {ipo_date_str} to datetime.date")
                    profile['ipo_date'] = self.get_date_from_japanese_date(ipo_date_str)
                    logging.debug(f"ipo date : {profile['ipo_date']}")
                elif "単元株数" in ths[0].get_text():
                    profile['number_of_shares_per_transaction'] = int(tds[0].get_text().strip().replace("株", ""))
                    logging.debug(f"number of shares per transaction : {profile['number_of_shares_per_transaction']}")
                else:
                    profile[self.profile_fields[ths[0].get_text().strip()]] = tds[0].get_text().strip()
        return profile
