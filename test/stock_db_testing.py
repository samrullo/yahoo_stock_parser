import os
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    Date,
    Integer,
    Float,
    String,
    select,
)

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

engine = create_engine(Config.db_uri)
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload=True, autoload_with=engine)
eq_ret_tbl = Table("eq_returns", meta, autoload=True, autoload_with=engine)
con = engine.connect()
