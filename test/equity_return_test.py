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
from stocks.equity_return_calculator import EquityReturnCalculator

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

start = datetime.date(2018, 12, 31)
end = datetime.date(2020, 4, 30)

engine = create_engine(Config.db_uri)
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload=True, autoload_with=engine)
eq_ret_tbl = Table("eq_returns", meta, autoload=True, autoload_with=engine)
con = engine.connect()

topix_df = pd.read_excel(
    os.path.join(
        r"C:\Users\amrul\PycharmProjects\stocks", "datasets", "TOPIX_weight_en.xlsx"
    )
)
topix_df["ticker"] = (
    topix_df["Code"].apply(str).map(lambda topix_code: topix_code + ".T")
)
logging.info(f"Total of {len(topix_df)} equities in TOPIX")
logging.info(
    f"By New Index Series Code count\n{topix_df.groupby('New Index Series Code').count()['Issue']}"
)

topix_small_one_df = topix_df.loc[
    topix_df["New Index Series Code"] == "TOPIX Small 1"
].copy()
topix_core30_df = topix_df.loc[
    topix_df["New Index Series Code"] == "TOPIX Core30"
].copy()

for i, ticker in enumerate(topix_core30_df["ticker"].unique().tolist()):
    logging.info(f"{i} : {ticker} started processing ...")
    eq_ret_obj = EquityReturnCalculator(engine, ticker, start, end)
    eq_px_df = eq_ret_obj.get_eq_prices()
    eq_ret_df = eq_ret_obj.generate_equity_returns(eq_px_df)
    eq_ret_obj.save_to_db(eq_ret_df)

eq_returns_df = pd.read_sql(select([eq_ret_tbl]), engine)
logging.info(f"{eq_returns_df.to_string()}")
