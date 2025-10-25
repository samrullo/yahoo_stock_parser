from tqdm import tqdm
import os
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String
from jeq_stocks_workspace.parse_yahoo_fundamentals import run_from_live

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dump_eq_prices.log"), logging.StreamHandler()],
)

start = datetime.date(2025, 10, 21)
end = datetime.date(2025, 10, 24)

engine = create_engine(Config.db_uri)
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload_with=engine)
con = engine.connect()

topix_df = pd.read_csv(
    Config.base_folder / "datasets" / "topixweight_e_20241031.csv", encoding="latin1"
)
topix_df["ticker"] = (
    topix_df["Code"].apply(str).map(lambda topix_code: topix_code + ".T")
)
logging.info(f"Total of {len(topix_df)} equities in TOPIX")
logging.info(
    f"By New Index Series Code count\n{topix_df.groupby('New Index Series Code').count()['Issue']}"
)

topix_eq_px_df_list = []

tickers = topix_df["ticker"].unique().tolist()

with tqdm(total=len(tickers)) as pbar:
    for i, ticker in enumerate(tickers):
        pbar.set_description(f"{i} : {ticker} start processing ....")
        run_from_live(ticker,Config.db_uri)
        pbar.update(1)

# query all fundamentals
fundamentals_table_name="yahoo_fundamentals"
fundamdf=pd.read_sql(f"SELECT * FROM `{fundamentals_table_name}`",engine)
logging.info(f"There are {len(fundamdf)} records in {fundamentals_table_name} table")