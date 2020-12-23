import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Table, MetaData, Date, Integer, Float, String
from config import Config
import datetime

topix_en_df = pd.read_excel(os.path.join(r"C:\Users\amrul\PycharmProjects\stocks", "datasets", "TOPIX_weight_en.xlsx"), dtype={"Code": str})
topix_jp_df = pd.read_excel(os.path.join(r"C:\Users\amrul\PycharmProjects\stocks", "datasets", "TOPIX_weight_jp.xlsx"), dtype={"コード": str})

topix_en_df['ticker'] = topix_en_df['Code'] + ".T"
topix_jp_df['ticker'] = topix_jp_df['コード'] + ".T"

en_cols = ['Date', 'Issue', 'Code', 'Sector', 'Component Weight (TOPIX)', 'New Index Series Code', 'Issue to which the liquidity factor is applied']
jp_cols = ['日付', '銘柄名', 'コード', '業種', 'TOPIXに占める個別銘柄のウェイト', 'ニューインデックス区分', '調整係数対象銘柄']

topix_mrg_df = pd.merge(left=topix_en_df, right=topix_jp_df, on='ticker')
mrg_cols = ['Date', 'Issue', 'Code', 'Sector', 'Component Weight (TOPIX)', 'New Index Series Code', 'Issue to which the liquidity factor is applied', 'ticker', '日付', '銘柄名', 'コード', '業種', 'TOPIXに占める個別銘柄のウェイト', 'ニューインデックス区分', '調整係数対象銘柄']
rename_cols = {"Date": "weight_date", "Issue": "desc_en", '銘柄名': "desc_jp", "Sector": "industry_en", '業種': "industry_jp",
               'Component Weight (TOPIX)': "component_weight", 'New Index Series Code': "new_index_series_code_en",
               'ニューインデックス区分': "new_index_series_code_jp"}
topix_mrg2_df = topix_mrg_df.rename(columns=rename_cols)
topix_mrg2_df["weight_date"] = datetime.date(2020, 3, 31)

db_cols = ["ticker", "weight_date", "desc_en", "desc_jp", "industry_en", "industry_jp", "new_index_series_code_en", "new_index_series_code_jp", "component_weight"]

dtype_dict = {col: String for col in db_cols}
dtype_dict["weight_date"] = Date
dtype_dict["component_weight"] = Float

engine = create_engine(Config.db_uri)
topix_mrg2_df[db_cols].to_sql("eq_sec_master", engine, index=False, if_exists="append", dtype=dtype_dict)
