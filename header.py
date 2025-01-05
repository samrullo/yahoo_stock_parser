import os
import pathlib
from sampytools.configdict import ConfigDict
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

start = datetime.date(2018, 12, 31)
end = datetime.date(2020, 4, 30)

engine = create_engine(Config.db_uri)

# meta = MetaData() creates a container for your database schema.
# eq_px_tbl = Table("eq_prices", meta, autoload_with=engine) retrieves the schema for the eq_prices table
# from the database and lets you programmatically interact with its structure using SQLAlchemy.
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload_with=engine)

meta.reflect(bind=engine)

tables = ConfigDict({table.name: table for table in list(meta.tables.values())})
