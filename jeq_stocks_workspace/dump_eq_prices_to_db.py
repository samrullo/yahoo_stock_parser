import os
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

start = datetime.date(2020, 5, 1)
end = datetime.date(2020, 8, 6)

engine = create_engine(Config.db_uri)
meta = MetaData()
eq_px_tbl = Table("eq_prices", meta, autoload=True, autoload_with=engine)
con = engine.connect()

topix_df = pd.read_excel(os.path.join(r"C:\Users\amrul\PycharmProjects\stocks", "datasets", "TOPIX_weight_en.xlsx"))
topix_df['ticker'] = topix_df['Code'].apply(str).map(lambda topix_code: topix_code + ".T")
logging.info(f"Total of {len(topix_df)} equities in TOPIX")
logging.info(f"By New Index Series Code count\n{topix_df.groupby('New Index Series Code').count()['Issue']}")

topix_small_one_df = topix_df.loc[topix_df['New Index Series Code'] == "TOPIX Small 1"].copy()
topix_core30_df = topix_df.loc[topix_df['New Index Series Code'] == 'TOPIX Core30'].copy()

topix_eq_px_df_list = []

tickers = topix_df['ticker'].unique().tolist()

for i, ticker in enumerate(tickers):
    logging.info(f"{i} : {ticker} start processing ....")
    yahoo_parser = YahooStockParser(ticker, start, end)
    eq_px_df = yahoo_parser.get_all_stock_dataframe()
    topix_eq_px_df_list.append(eq_px_df)

topix_eq_px_df = pd.concat(topix_eq_px_df_list)
logging.info(f"Topix eq prices from {start} to {end} {len(topix_eq_px_df)}")

topix_eq_px_df.to_sql("eq_prices", engine, index=False, if_exists="append", dtype={"adate": Date, "px_open": Float, "px_high": Float, "px_low": Float, "px_close": Float, "volume": Integer, "px_close_after_adj": Float, "ticker": String})
