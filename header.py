import os
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import create_engine, Table, MetaData, Date, Integer,Float, String
import seaborn as sns
import matplotlib.pyplot as plt

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

start = datetime.date(2018, 12, 31)
end = datetime.date(2020, 4, 30)

engine = create_engine(Config.db_uri)
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload_with=engine)
con = engine.connect()
