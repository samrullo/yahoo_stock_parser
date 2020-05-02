from utils.myfunctions import *
from datetime import datetime
from datetime import date
from urllib.request import urlopen
from lxml.html import parse
import logging

logging.basicConfig(filename="stocks.log", level=logging.DEBUG)


class StockReader:

    def __init__(self):
        self.current_url = ""

    def _unpack(self, row, kind='td'):
        cells = row.findall('.//%s' % kind)
        vals = [val.text for val in cells]
        return vals

    def get_px_series(self, px_rows):
        px_df = pd.DataFrame(columns=['close', 'high', 'low', 'open'])
        for row in px_rows:
            if row.getchildren()[0].tag != 'th':
                arr = self._unpack(row, kind='td')
                if len(arr) != 0:
                    arr[0] = arr[0].replace('年', '-')
                    arr[0] = arr[0].replace('月', '-')
                    arr[0] = arr[0].replace('日', '')
                df = pd.DataFrame(index=[arr[0]], data=[arr[1:5]], columns=['open', 'high', 'low', 'close'])
                px_df = pd.concat([px_df, df])
        return px_df

    def getYahooSeries(self, ticker, start=date(2017, 12, 31), end=date.today(), inv_amt=10000):
        # loading data from 300 urls pages
        px_df = pd.DataFrame(columns=['close', 'high', 'low', 'open'])
        url_template = 'https://info.finance.yahoo.co.jp/history/?code={ticker}&sy={start_year}&sm={start_month}&sd={start_day}&ey={end_year}&em={end_month}&ed={end_day}&tm=d&p={page_numb}'
        # url_template='https://info.finance.yahoo.co.jp/history/?code=USDJPY=X&sy=2005&sm=1&sd=1&ey=2018&em=7&ed=15&tm=d&p=1'
        hasPX = True
        page = 1
        while (hasPX):
            url = url_template.format(ticker=ticker, start_year=start.year, start_month=start.month,
                                      start_day=start.day, end_year=end.year, end_month=end.month, end_day=end.day,
                                      page_numb=str(page))
            logging.info(url)
            self.current_url = url
            page += 1
            parsed = parse(urlopen(url))
            doc = parsed.getroot()
            tables = doc.findall('.//table')
            if len(tables) > 2:
                px_tbl = tables[1]
                rows = px_tbl.findall('.//tr')
                if len(rows) != 0:
                    df = self.get_px_series(rows)
                    if len(df) != 0:
                        px_df = pd.concat([px_df, df])
                    else:
                        hasPX = False
                else:
                    hasPX = False
            else:
                hasPX = False
        if len(px_df) != 0:
            px_df.index = pd.to_datetime(px_df.index)
            for col in px_df.columns.values:
                px_df[col] = px_df[col].str.replace(',', '')
                px_df[col] = pd.to_numeric(px_df[col])
            px_df.sort_index(inplace=True)
            px_df['dly_ret'] = px_df['close'].pct_change()
            px_df['dly_ret_bps'] = px_df['dly_ret'] * 10000
            px_df['comp_ret'] = inv_amt * np.cumprod(1 + px_df.dly_ret)
        return px_df


main_folder = 'C:\\Users\\amrul\\Documents\\investment_analysis\\'
start = date(2020, 3, 31)
end = date(2020, 4, 30)
stock_obj = StockReader()
softbank_df = stock_obj.getYahooSeries('9984.T', start=start, end=end)
