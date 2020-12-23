import os
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

start = datetime.date(2020, 1, 1)
end = datetime.date(2020, 6, 12)

# ticker = "KLBF"
ticker = "PTKFY"
yahoo_parser = YahooStockParser(ticker, start, end)
px_df = yahoo_parser.get_all_stock_dataframe()
