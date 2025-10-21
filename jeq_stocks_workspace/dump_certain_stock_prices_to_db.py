from tqdm import tqdm
import os
import pathlib
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dump_eq_prices.log"), logging.StreamHandler()],
)

start = datetime.date(2019, 1, 1)
end = datetime.date(2024, 12, 31)

engine = create_engine(Config.db_uri)
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload_with=engine)
con = engine.connect()

topix_eq_px_df_list = []

tickers_file = pathlib.Path(
    r"C:\Users\amrul\programming\various_projects\yahoo_stock_parser\datasets\added_tickers.txt"
)
tickers = tickers_file.read_text().split("\n")

with tqdm(total=len(tickers)) as pbar:
    for i, ticker in enumerate(tickers):
        pbar.set_description(f"{i} : {ticker} start processing ....")
        yahoo_parser = YahooStockParser(ticker, start, end)
        eq_px_df = yahoo_parser.get_all_stock_dataframe()
        eq_px_df.to_sql(
            "eq_prices",
            engine,
            index=False,
            if_exists="append",
            dtype={
                "adate": Date,
                "px_open": Float,
                "px_high": Float,
                "px_low": Float,
                "px_close": Float,
                "volume": Integer,
                "px_close_after_adj": Float,
                "ticker": String,
            },
        )
        topix_eq_px_df_list.append(eq_px_df)
        pbar.update(1)

topix_eq_px_df = pd.concat(topix_eq_px_df_list)
logging.info(f"Topix eq prices from {start} to {end} {len(topix_eq_px_df)}")
