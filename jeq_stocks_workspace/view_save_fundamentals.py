from tqdm import tqdm
import pathlib
import os
import pandas as pd
import numpy as np
from stocks.yahoo_stock_parser import YahooStockParser
import datetime
import logging
from config import Config
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String
from jeq_stocks_workspace.parse_yahoo_fundamentals import run_from_live
from sampytools.pandas_utils import remove_nonnumeric_chars_from_numeric_cols
from sampytools.pandas_utils import convert_columns_to_numeric
from sampytools.list_utils import get_list_diff

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dump_eq_prices.log"), logging.StreamHandler()],
)

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

# query all fundamentals
fundamentals_table_name = "yahoo_fundamentals"
fundamdf = pd.read_sql(f"SELECT * FROM `{fundamentals_table_name}`", engine)
logging.info(f"There are {len(fundamdf)} records in {fundamentals_table_name} table")

# save fundamentals data
save_results = False
if save_results:
    thefolder = pathlib.Path(r"/Users/samrullo/programming/pyprojects/yahoo_stock_parser/datasets")
    fmrgdf = pd.merge(left=fundamdf, right=topix_df, on="ticker")
    front_cols = get_list_diff(topix_df.columns, ["ticker", "Date"])
    back_cols = get_list_diff(fundamdf.columns, ["ticker"])
    fmrg_cols = ["ticker"] + front_cols + back_cols
    tfdf = fmrgdf[fmrg_cols].copy()
    tfdf = tfdf.rename(columns={"Component Weight (TOPIX)": "weight_pct"})
    tfdf = remove_nonnumeric_chars_from_numeric_cols(tfdf, ["%"], ["weight_pct"])
    tfdf = convert_columns_to_numeric(tfdf, ["weight_pct"])
    tfdf = tfdf.sort_values("weight_pct", ascending=False)
    tfdf["weight_pct_cum"] = tfdf["weight_pct"].cumsum()
    tfdf.to_csv(thefolder / "topix_fundamentals.csv", index=False)
